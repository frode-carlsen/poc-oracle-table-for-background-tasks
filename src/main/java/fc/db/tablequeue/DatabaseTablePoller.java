package fc.db.tablequeue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.sql.Savepoint;
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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseTablePoller {

    private static final Logger log = LoggerFactory.getLogger(DatabaseTablePoller.class);

    private final String threadPoolNamePrefix = DatabaseTablePoller.class.getSimpleName();
    private final int maxNumberOfTasksToPoll;
    private int maxInternalRunTaskQueueSize;
    private final long delayBetweenPollingMillis = 500L;
    private final TaskDispatcher taskDispatcher;

    private final BlockingQueue<Runnable> internalRunTaskWaitQueue;
    private final ExecutorService runTaskService;
    private final ScheduledExecutorService pollingService;
    private ScheduledFuture<?> pollingServiceScheduledFuture;

    public DatabaseTablePoller(int numberOfTaskRunnerThreads, int maxNumberOfTasksToPoll, TaskDispatcher taskDispatcher) {
        this(numberOfTaskRunnerThreads, maxNumberOfTasksToPoll, 100, taskDispatcher);
    }

    public DatabaseTablePoller(int numberOfTaskRunnerThreads, int maxNumberOfTasksToPoll, int maxInternalTaskQueueSize, TaskDispatcher taskDispatcher) {
        if (numberOfTaskRunnerThreads <= 0) {
            throw new IllegalArgumentException("numberOfTaskRunnerThreads<=0: invalid"); //$NON-NLS-1$
        } else if (maxNumberOfTasksToPoll <= 0) {
            throw new IllegalArgumentException("maxNumberOfTasksToPoll<=0: invalid"); //$NON-NLS-1$
        } else if (maxInternalTaskQueueSize <= 0) {
            throw new IllegalArgumentException("maxInternalTaskQueueSize<=0: invalid"); //$NON-NLS-1$
        }
        Objects.requireNonNull(taskDispatcher, "taskDispatcher"); //$NON-NLS-1$

        this.maxInternalRunTaskQueueSize = maxInternalTaskQueueSize;
        this.taskDispatcher = taskDispatcher;
        this.maxNumberOfTasksToPoll = maxNumberOfTasksToPoll;

        this.internalRunTaskWaitQueue = new ArrayBlockingQueue<>(maxInternalRunTaskQueueSize);

        this.pollingService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(threadPoolNamePrefix + "-poller", false)); //$NON-NLS-1$
        this.runTaskService = new ThreadPoolExecutor(numberOfTaskRunnerThreads, numberOfTaskRunnerThreads,
                0L, TimeUnit.MILLISECONDS,
                internalRunTaskWaitQueue, new NamedThreadFactory(threadPoolNamePrefix + "-runtask", true)); //$NON-NLS-1$

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

    protected String doneFlag() {
        return "DONE"; //$NON-NLS-1$
    }

    protected String failedFlag() {
        return "FAILED"; //$NON-NLS-1$
    }

    protected void doWork(Long id, String taskName, int failedAttempts) {
        taskDispatcher.dispatch(id, taskName, failedAttempts);
    }

    protected Connection newConnection() throws SQLException {
        return Db.createConnection();
    }

    public static String getLastError(Long id, String taskName, Exception e) {
        StringWriter sw = new StringWriter(4096);
        sw.write("Failed to Process task id=" + id + ", taskName=" + taskName + "\n"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        pw.flush();

        return sw.toString();
    }

    /** Thread factory which lets us configure name of pooled thread, and daemon flag. */
    public static class NamedThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger(1);
        private final String prefix;
        private final boolean daemon;

        public NamedThreadFactory(String prefix, boolean daemon) {
            this.prefix = prefix;
            this.daemon = daemon;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "_" + String.format("%03d", counter.getAndIncrement())); //$NON-NLS-1$ //$NON-NLS-2$
            t.setDaemon(daemon);
            return t;
        }
    }

    /**
     * Poll database table for tasks to run. Handled in a single thread.
     */
    protected class PollAvailableTasks implements Runnable {

        private final AtomicBoolean sleep = new AtomicBoolean();

        @Override
        public void run() {
            try {
                if (sleep.getAndSet(false)) {
                    Thread.sleep(60 * 1000L);
                }
                pollForAvailableTasks();
            } catch (Exception e) {
                // TODO: should only log same exception once every X minutes to avoid spamming logs in case of
                // persistent serious failures.
                // For now, just sleep on next try
                sleep.set(true);
                log.warn("Failed to poll database, sleep on next try", e); //$NON-NLS-1$
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
            String sql = getSqlForPolling();

            try (Connection conn = newConnection();
                    PreparedStatement pstmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
                pstmt.setFetchSize(1);
                pstmt.setString(1, doneFlag());
                pstmt.setString(2, failedFlag());

                ResultSet rs = pstmt.executeQuery();
                Timestamp thisTimestamp = Timestamp.valueOf(LocalDateTime.now());
                while (rs.next() && numberOfTasksToPoll-- >= 0) {
                    runTasks.add(readAndDispatch(rs, thisTimestamp));
                    rs.updateTimestamp("last_attempt_ts", thisTimestamp); //$NON-NLS-1$
                    rs.updateRow();
                }

                conn.commit();
            }

            for (Runnable task : runTasks) {
                runTaskService.submit(task);
            }

        }

        protected Runnable readAndDispatch(ResultSet rs, Timestamp thisTryTimestamp) throws SQLException {
            Long id = rs.getLong("id"); //$NON-NLS-1$
            String taskName = rs.getString("task_name"); //$NON-NLS-1$
            return new RunTask(id, taskName, thisTryTimestamp);
        }

        protected String getSqlForPolling() {
            return "SELECT id, priority, status, task_name, last_attempt_ts" //$NON-NLS-1$
                    + " FROM dbqueue" //$NON-NLS-1$
                    + " WHERE (next_attempt_after < systimestamp)" //$NON-NLS-1$
                    + "       AND failed_attempts < max_attempts" //$NON-NLS-1$
                    + "       AND status NOT IN (?, ?) " //$NON-NLS-1$
                    + " ORDER BY " //$NON-NLS-1$
                    + "       priority, last_attempt_ts NULLS FIRST, id" //$NON-NLS-1$
                    + " FOR UPDATE SKIP LOCKED"; //$NON-NLS-1$
        }

    }

    /**
     * Run a single task. Multiple tasks may be run concurrently on different threads. Only one task is run by one
     * thread at a time, with its own transaction.
     * In case of failure, will be retried up to "max_attempts" and with specified "secs_between_attempts" between.
     */
    protected class RunTask implements Runnable {

        private final Long id;
        private Timestamp timestampLowWatermark;
        private String taskName;

        protected RunTask(Long id, String taskName, Timestamp timestampLowWatermark) {
            Objects.requireNonNull(id, "id"); //$NON-NLS-1$
            Objects.requireNonNull(taskName, "taskName"); //$NON-NLS-1$
            Objects.requireNonNull(timestampLowWatermark, "timestampLowWatermark"); //$NON-NLS-1$

            this.taskName = taskName;
            this.timestampLowWatermark = timestampLowWatermark;
            this.id = id;
        }

        @Override
        public void run() {

            // set up thread name so it's reflected directly in logs
            String oldThreadName = Thread.currentThread().getName();
            Thread.currentThread().setName(oldThreadName + " [id=" + id + ", taskName=" + taskName + "]"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

            try (Connection conn = newConnection();
                    PreparedStatement stmt = conn.prepareStatement(
                            getSqlForPickingSingleTaskById(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {

                stmt.setLong(1, this.id);
                stmt.setString(2, this.taskName);
                stmt.setTimestamp(3, this.timestampLowWatermark);

                stmt.setFetchSize(1);

                try (ResultSet rs = stmt.executeQuery();) {

                    if (rs.next() && isInProcessableState(rs)) {
                        if (handleTask(conn, rs)) {
                            conn.commit();
                        } else {
                            conn.rollback();
                        }
                    }
                }
                // } catch (SQLTransientException | SQLNonTransientConnectionException | SQLRecoverableException e) {
                // TODO: ignore these?
            } catch (SQLException e) {
                log.warn("Failed to run task, id=" + id + ", will be automatically repeated", e); //$NON-NLS-1$ //$NON-NLS-2$
            } finally {
                Thread.currentThread().setName(oldThreadName);
            }
        }

        protected boolean handleTask(Connection conn, ResultSet rs) throws SQLException {
            String taskName = rs.getString("task_name"); //$NON-NLS-1$
            int failedAttempts = rs.getInt("failed_attempts"); //$NON-NLS-1$

            markTaskBeingProcessed(rs);

            // set up a savepoint to rollback to in case of failure
            Savepoint savepoint = conn.setSavepoint();

            try {
                doWork(id, taskName, failedAttempts);
                handleTaskSuccess(rs);
            } catch (SQLTransientException | SQLNonTransientConnectionException | SQLRecoverableException e) {
                handleTransientAndRecoverableException(taskName, e);
                return false;
            } catch (Exception e) {
                conn.rollback(savepoint);
                // assume error can be written to the queue
                handleTaskException(rs, taskName, failedAttempts, e);
            }

            rs.updateRow();
            return true;

        }

        protected void handleTransientAndRecoverableException(String taskName, SQLException e) {
            // assume won't help to try and write to database just now, so log only
            // instead
            log.warn("Failed to process task: id=" + id + ", taskName=" + taskName, e); //$NON-NLS-1$ //$NON-NLS-2$
        }

        protected void markTaskBeingProcessed(ResultSet rs) throws SQLException {
            // mark row being processed
            rs.updateTimestamp("last_attempt_ts", Timestamp.valueOf(LocalDateTime.now())); //$NON-NLS-1$
            rs.updateRow();
        }

        protected void handleTaskSuccess(ResultSet rs) throws SQLException {
            rs.updateString("last_error", null); //$NON-NLS-1$
            rs.updateTimestamp("next_attempt_after", null); //$NON-NLS-1$
            rs.updateString("status", doneFlag()); //$NON-NLS-1$
        }

        protected void handleTaskException(ResultSet rs, String taskName, int failedAttempts, Exception e) throws SQLException {
            failedAttempts++;
            int maxAttempts = rs.getInt("max_attempts"); //$NON-NLS-1$
            int secsBetweenAttempts = rs.getInt("secs_between_attempts"); //$NON-NLS-1$
            rs.updateInt("failed_attempts", failedAttempts); //$NON-NLS-1$
            if (failedAttempts >= maxAttempts) {
                rs.updateTimestamp("next_attempt_after", null); //$NON-NLS-1$
                rs.updateString("status", failedFlag()); //$NON-NLS-1$
            } else {
                rs.updateTimestamp("next_attempt_after", Timestamp.valueOf(LocalDateTime.now().plusSeconds(secsBetweenAttempts))); //$NON-NLS-1$
            }
            String exceptionString = getLastError(id, taskName, e);
            rs.updateString("last_error", exceptionString); //$NON-NLS-1$
        }

        protected boolean isInProcessableState(ResultSet rs) throws SQLException {
            String status = rs.getString("status"); //$NON-NLS-1$
            return !Objects.equals(doneFlag(), status);
        }

        protected String getSqlForPickingSingleTaskById() {
            return "SELECT id, priority, task_name, status, last_attempt_ts, next_attempt_after, secs_between_attempts, failed_attempts, max_attempts, last_error" //$NON-NLS-1$
                    + " FROM dbqueue" //$NON-NLS-1$
                    + " WHERE id=? AND task_name=? AND last_attempt_ts>=?" //$NON-NLS-1$
                    + " FOR UPDATE SKIP LOCKED"; //$NON-NLS-1$
        }

    }

    /** Test method. */
    public static void main(String[] args) {

        TaskDispatcher taskDispatcher = (id, taskName, failedAttempts) -> {
            if (id == 3) {
                // TODO: remove. throws a failure for just for fun
                throw new IllegalArgumentException("Unknown task id=" + 3, new UnsupportedOperationException()); //$NON-NLS-1$
            }
            log.info("*********** Processed: id=" + id + ", task=" + taskName); //$NON-NLS-1$ //$NON-NLS-2$
            return;
        };

        new DatabaseTablePoller(5, 50, taskDispatcher).start();
    }
}
