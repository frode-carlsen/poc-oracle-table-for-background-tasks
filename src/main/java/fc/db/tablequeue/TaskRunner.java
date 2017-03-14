package fc.db.tablequeue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Main class handling polling tasks and dispatching these. */
public class TaskRunner {

	private static final Logger log=LoggerFactory.getLogger(TaskRunner.class);
	
	/** Defines sql used for tables and queries. */
	private final TaskTableDefinition tableDefinition = new TaskTableDefinition();
	
	/** Prefix every thread in pool with given name. */
	private final String threadPoolNamePrefix = getClass().getSimpleName();
	
	/** Max number of tasks that will be attempted to poll on every try. */
    private final int maxNumberOfTasksToPoll;
    
    /** Delay between each interval of polling. */
    private final long delayBetweenPollingMillis = 500L;

    /** Internal buffer for tasks (which tasks are run from) in an executor. */
    private final BlockingQueue<Runnable> internalRunTaskWaitQueue;
    
    /** Executor to handle tasks. */
    private final ExecutorService runTaskService;
    
    /** Single scheduled thread handling polling. */
    private final ScheduledExecutorService pollingService;

    /** Future to allow cancelling polling. */
    private ScheduledFuture<?> pollingServiceScheduledFuture;

    private final DataSource dataSource;

    /** Abstract factory to create runnables handling tasks. */
	private RunSingleTaskFactory runTaskFactory;
    
    public TaskRunner(int numberOfTaskRunnerThreads, int maxNumberOfTasksToPoll, DataSource dataSource, TaskDispatcher taskDispatcher) {
        if (numberOfTaskRunnerThreads <= 0) {
            throw new IllegalArgumentException("numberOfTaskRunnerThreads<=0: invalid"); //$NON-NLS-1$
        } else if (maxNumberOfTasksToPoll <= 0) {
            throw new IllegalArgumentException("maxNumberOfTasksToPoll<=0: invalid"); //$NON-NLS-1$
        }
        this.maxNumberOfTasksToPoll = maxNumberOfTasksToPoll;

        Objects.requireNonNull(dataSource, "dataSource"); //$NON-NLS-1$
        Objects.requireNonNull(taskDispatcher, "taskDispatcher"); //$NON-NLS-1$
        this.dataSource = dataSource;
        this.runTaskFactory = new RunSingleTaskFactory(taskDispatcher, tableDefinition, dataSource);
        
        this.internalRunTaskWaitQueue = new ArrayBlockingQueue<>(2 * maxNumberOfTasksToPoll);
        this.pollingService = Executors.newSingleThreadScheduledExecutor(new Utils.NamedThreadFactory(threadPoolNamePrefix + "-poller", false)); //$NON-NLS-1$
        this.runTaskService = new ThreadPoolExecutor(numberOfTaskRunnerThreads, numberOfTaskRunnerThreads,
                0L, TimeUnit.MILLISECONDS,
                internalRunTaskWaitQueue, new Utils.NamedThreadFactory(threadPoolNamePrefix + "-runtask", true)); //$NON-NLS-1$

    }

    public synchronized void stop() {
        if (pollingServiceScheduledFuture != null) {
            pollingServiceScheduledFuture.cancel(true);
            pollingServiceScheduledFuture = null;
        }
    }

    public synchronized void start() {
        pollingServiceScheduledFuture = pollingService
                .scheduleWithFixedDelay(new PollAvailableTasks(), delayBetweenPollingMillis / 2, delayBetweenPollingMillis, TimeUnit.MILLISECONDS);

    }

    protected Connection newConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * Poll database table for tasks to run. Handled in a single thread.
     */
    protected class PollAvailableTasks implements Runnable {

		/** simple backoff interval in seconds per round to account for transient database errors. */
        private final int[] backoffInterval = new int[] { 1, 5, 5, 10, 10, 10, 10, 30 };
        private int backoffRound = 0;

        @Override
        public void run() {
            try {
                if (backoffRound > 0) {
                    Thread.sleep(backoffInterval[Math.min(backoffRound, backoffInterval.length) - 1] * 1000L);
                }

                pollForAvailableTasks();
                
                backoffRound = 0;
            } catch (InterruptedException e) {
            	backoffRound++;
                Thread.currentThread().interrupt();
            } catch (SQLRecoverableException | SQLTransientException e) {
                backoffRound++;
                log.warn("Transient failure connecting to database, sleep on next try (round=" + backoffRound + "): " + e.getClass() + ":" + e); //$NON-NLS-1$
            } catch (Exception e) {
                backoffRound++;
                log.warn("Failed to poll database, sleep on next try (round=" + backoffRound + ")", e); //$NON-NLS-1$
            }
        }

        protected void pollForAvailableTasks() throws SQLException {

            int capacity = internalRunTaskWaitQueue.remainingCapacity();
            if (capacity < 1) {
                // internal work queue already full, no point trying to push more
                return;
            }

            int numberOfTasksToPoll = Math.min(capacity, maxNumberOfTasksToPoll);

            List<Runnable> runTasks = new ArrayList<>(numberOfTasksToPoll);

            try (Connection conn = newConnection();
                    PreparedStatement pstmt = tableDefinition.getStatementForPolling(conn);) {

            	pstmt.setFetchSize(1);
                ResultSet rs = pstmt.executeQuery();
                Timestamp thisTimestamp = Timestamp.valueOf(LocalDateTime.now());
                while (rs.next() && numberOfTasksToPoll-- >= 0) {
                    runTasks.add(readAndDispatch(rs, thisTimestamp));
                    rs.updateTimestamp(tableDefinition.getLastAttemptTimestampColumnName(), thisTimestamp);
                    rs.updateString(tableDefinition.getServerNodeColumnName(), Utils.getJvmUniqueProcessName());
                    rs.updateRow();
                }

                conn.commit();
            }

            for (Runnable task : runTasks) {
                runTaskService.submit(task);
            }

        }

        protected Runnable readAndDispatch(ResultSet rs, Timestamp thisTryTimestamp) throws SQLException {
            Long id = rs.getLong(tableDefinition.getIdColumnName()); //$NON-NLS-1$
            String taskName = rs.getString(tableDefinition.getTaskNameColumnName()); //$NON-NLS-1$
            return runTaskFactory.newTask(id, taskName, thisTryTimestamp);
        }

    }

}
