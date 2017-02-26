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

    private static final AtomicInteger COUNTER = new AtomicInteger(1);
    private static final Logger log = LoggerFactory.getLogger(DatabaseTablePoller.class);

    private final String threadPoolNamePrefix = DatabaseTablePoller.class.getSimpleName();
    private final int maxNumberOfTasksToPoll;
    private final int maxInternalRunTaskQueueSize = 100;
    private final long delayBetweenPollingMillis = 500L;

    private final ExecutorService runTaskService;
    private final ScheduledExecutorService pollingService;
    private ScheduledFuture<?> scheduledPoller;

    private final BlockingQueue<Runnable> internalRunTaskWaitQueue;

    public DatabaseTablePoller(int numberOfTaskRunnerThreads, int maxNumberOfTasksToPoll) {
        if (numberOfTaskRunnerThreads <= 0) {
            throw new IllegalArgumentException("numberOfTaskRunnerThreads<=0");
        } else if (maxNumberOfTasksToPoll <= 0) {
            throw new IllegalArgumentException("maxNumberOfTasksToPoll<=0");
        }

        this.maxNumberOfTasksToPoll = maxNumberOfTasksToPoll;

        this.internalRunTaskWaitQueue = new ArrayBlockingQueue<>(maxInternalRunTaskQueueSize);

        this.pollingService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(threadPoolNamePrefix + "-poller", false));
        this.runTaskService = new ThreadPoolExecutor(numberOfTaskRunnerThreads, numberOfTaskRunnerThreads,
                0L, TimeUnit.MILLISECONDS,
                internalRunTaskWaitQueue, new NamedThreadFactory(threadPoolNamePrefix + "-runtask", true));

    }

    public synchronized void stop() {
        if (scheduledPoller != null) {
            scheduledPoller.cancel(true);
            scheduledPoller = null;
        }
    }

    public synchronized void start() {
        scheduledPoller = pollingService
                .scheduleWithFixedDelay(new PollAvailableTasks(), delayBetweenPollingMillis / 2, delayBetweenPollingMillis, TimeUnit.MILLISECONDS);

    }

    protected String getSqlForPolling() {
        return "SELECT id, priority, status, last_attempt_ts FROM dbqueue"
                + " WHERE (next_attempt_after < systimestamp)"
                + "       and failed_attempts < max_attempts"
                + "       and status not in (?, ?) "
                + " ORDER BY "
                + "       priority, last_attempt_ts nulls first, id"
                + " FOR UPDATE SKIP LOCKED";
    }

    protected String getSqlForPickingSingleTaskById() {
        return "select id, priority, task_name, status, last_attempt_ts, next_attempt_after, secs_between_attempts, failed_attempts, max_attempts, last_error from dbqueue where id=? for update skip locked";
    }

    protected String doneFlag() {
        return "DONE";
    }

    protected String failedFlag() {
        return "FAILED";
    }

    protected void doWork(Long id, String taskName, @SuppressWarnings("unused") int failedAttempts) {
        if (id == 3) {
            // TODO: remove.  throws a failure for just for fun
            throw new IllegalArgumentException("Unknown task id=" + 3, new UnsupportedOperationException());
        }
        log.info("*********** Processed: id=" + id + ", task=" + taskName);
    }

    protected Connection newConnection() throws SQLException {
        return Db.createConnection();
    }

    static String getLastError(Long id, String taskName, Exception e) {
        StringWriter sw = new StringWriter(4096);
        sw.write("Failed to Process task id=" + id + ", taskName=" + taskName + "\n");

        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        pw.flush();

        return sw.toString();
    }

    static class NamedThreadFactory implements ThreadFactory {
        private String prefix;
        private boolean daemon;

        NamedThreadFactory(String prefix, boolean daemon) {
            this.prefix = prefix;
            this.daemon = daemon;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "_" + String.format("%03d", COUNTER.getAndIncrement()));
            t.setDaemon(daemon);
            return t;
        }
    }

    /**
     * Poll database table for tasks to run. Handled in a single thread.
     */
    class PollAvailableTasks implements Runnable {

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
                // persistent serious failures. for now, just sleep on next try
                sleep.set(true);
                log.warn("Failed to poll database, sleep on next try", e);
            }
        }

        private void pollForAvailableTasks() throws SQLException {

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
                Timestamp lastTry = Timestamp.valueOf(LocalDateTime.now());
                while (rs.next() && numberOfTasksToPoll-- >= 0) {
                    runTasks.add(readAndDispatch(rs));
                    rs.updateTimestamp("last_attempt_ts", lastTry);
                    rs.updateRow();
                }

                conn.commit();
            }

            for (Runnable task : runTasks) {
                runTaskService.submit(task);
            }

        }

        private Runnable readAndDispatch(ResultSet rs) throws SQLException {
            Long id = rs.getLong("id");
            return new RunTask(id);
        }

    }

    /**
     * Run a single task. Multiple tasks may be run concurrently on different threads. Only one task is run by one
     * thread at a time, with its own transaction.
     * In case of failure, will be retried up to "max_attempts" and with specified "secs_between_attempts" between.
     */
    class RunTask implements Runnable {

        private final Long id;

        RunTask(Long id) {
            Objects.requireNonNull(id, "id");
            this.id = id;
        }

        @Override
        public void run() {
            String sql = getSqlForPickingSingleTaskById();

            try (Connection conn = newConnection();
                    PreparedStatement stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {

                stmt.setLong(1, this.id);
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
                log.warn("Failed to run task, id=" + id + ", will be automatically repeated", e);
            }
        }

        protected boolean handleTask(Connection conn, ResultSet rs) throws SQLException {
            String taskName = rs.getString("task_name");
            int failedAttempts = rs.getInt("failed_attempts");

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

        private void handleTransientAndRecoverableException(String taskName, SQLException e) {
            // assume won't help to try and write to database just now, so log only
            // instead
            log.warn("Failed to process task: id=" + id + ", taskName=" + taskName, e);
        }

        private void markTaskBeingProcessed(ResultSet rs) throws SQLException {
            // mark row being processed
            rs.updateTimestamp("last_attempt_ts", Timestamp.valueOf(LocalDateTime.now()));
            rs.updateRow();
        }

        private void handleTaskSuccess(ResultSet rs) throws SQLException {
            rs.updateString("last_error", null);
            rs.updateTimestamp("next_attempt_after", null);
            rs.updateString("status", doneFlag());
        }

        private void handleTaskException(ResultSet rs, String taskName, int failedAttempts, Exception e) throws SQLException {
            failedAttempts++;
            int maxAttempts = rs.getInt("max_attempts");
            int secsBetweenAttempts = rs.getInt("secs_between_attempts");
            rs.updateInt("failed_attempts", failedAttempts);
            if (failedAttempts >= maxAttempts) {
                rs.updateTimestamp("next_attempt_after", null);
                rs.updateString("status", failedFlag());
            } else {
                rs.updateTimestamp("next_attempt_after", Timestamp.valueOf(LocalDateTime.now().plusSeconds(secsBetweenAttempts)));
            }
            String exceptionString = getLastError(id, taskName, e);
            rs.updateString("last_error", exceptionString);
        }

        protected boolean isInProcessableState(ResultSet rs) throws SQLException {
            String status = rs.getString("status");
            return !doneFlag().equals(status);
        }

    }

    /** Test method. */
    public static void main(String[] args) {
        DatabaseTablePoller poller = new DatabaseTablePoller(2, 2);
        poller.start();
    }
}
