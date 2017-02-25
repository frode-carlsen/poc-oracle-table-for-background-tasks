package fc.db.tablequeue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabaseTablePoller {

    private final ExecutorService service;

    private static final String TH_NAME_PREFIX = DatabaseTablePoller.class.getSimpleName();
    private static final AtomicInteger COUNTER = new AtomicInteger(1);

    private final long scheduleDelayMillis = 500L;

    private ScheduledFuture<?> scheduledPoller;

    private final int pollSize;

    public DatabaseTablePoller(int dispatchThreads, int pollSize) {

        this.pollSize = pollSize;
        service = Executors.newFixedThreadPool(dispatchThreads, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, TH_NAME_PREFIX + "_" + String.format("%03d", COUNTER.getAndIncrement()));
                t.setDaemon(true);
                return t;
            }
        });

    }

    public synchronized void stop() {
        if (scheduledPoller != null) {
            scheduledPoller.cancel(true);
            scheduledPoller = null;
        }
    }

    public synchronized void start() {

        class PollDatabaseTask implements Runnable {

            @Override
            public void run() {
                try {
                    pollDatabaseTable();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            private void pollDatabaseTable() throws SQLException {

                int rowCount = pollSize;

                List<Runnable> runTasks = new ArrayList<>(pollSize);
                String sql = "SELECT id, priority, status, last_try FROM dbqueue"
                        + " WHERE (last_try IS NULL or (last_try + numtodsinterval(?, 'second') < systimestamp))"
                        + "       and status not in (?) "
                        + " ORDER BY "
                        + "       priority, last_try nulls first, id"
                        + " FOR UPDATE SKIP LOCKED";

                try (Connection conn = newConnection();
                        PreparedStatement pstmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
                    pstmt.setString(1, "30");
                    pstmt.setString(2, "DONE");
                    pstmt.setFetchSize(Math.max(1, pollSize / 2));

                    ResultSet rs = pstmt.executeQuery();
                    Timestamp lastTry = Timestamp.valueOf(LocalDateTime.now());
                    while (rs.next() && rowCount-- > 0) {
                        runTasks.add(readAndDispatch(rs));
                        rs.updateTimestamp("last_try", lastTry);
                        rs.updateRow();
                    }

                    conn.commit();
                }

                for (Runnable task : runTasks) {
                    service.submit(task);
                }

            }

            private Runnable readAndDispatch(ResultSet rs) throws SQLException {

                class RunTask implements Runnable {

                    private final Long id;

                    RunTask(Long id) {
                        Objects.requireNonNull(id, "id");
                        this.id = id;
                    }

                    @Override
                    public void run() {
                        String sql = "select id, priority, status, last_try from dbqueue where id=? for update skip locked";

                        try (Connection conn = newConnection();
                                PreparedStatement stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {

                            stmt.setLong(1, this.id);
                            stmt.setFetchSize(1);

                            ResultSet rs = stmt.executeQuery();

                            boolean commit = false;
                            while (rs.next()) {
                                String status = rs.getString("status");
                                if (!"DONE".equals(status)) {
                                    System.out.println("Processed: " + id);
                                    rs.updateString("status", "DONE");
                                    rs.updateTimestamp("last_try", Timestamp.valueOf(LocalDateTime.now()));
                                    rs.updateRow();
                                    commit = true;
                                }

                            }

                            if (commit) {
                                conn.commit();
                            }

                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                }

                Long id = rs.getLong("id");

                return new RunTask(id);
            }

        }

        scheduledPoller = Executors.newSingleThreadScheduledExecutor()
                .scheduleWithFixedDelay(new PollDatabaseTask(), scheduleDelayMillis / 2, scheduleDelayMillis, TimeUnit.MILLISECONDS);

    }

    Connection newConnection() throws SQLException {
        return Db.createConnection();
    }

    public static void main(String[] args) {
        DatabaseTablePoller poller = new DatabaseTablePoller(1, 2);
        poller.start();
    }
}
