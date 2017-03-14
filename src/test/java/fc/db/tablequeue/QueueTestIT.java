package fc.db.tablequeue;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueueTestIT {

    static {
        TestDb.initTestDb(true);
    }

    @Before
    public void setup_testdata() throws SQLException {

        Object[][] testdata = new Object[][] {
                { 5, "mytask", 5, "TODO" },
                { 4, "mytask", 2, "TODO" },
                { 3, "mytask", 2, "TODO" },
                { 2, "mytask", 1, "DONE" },
                { 1, "mytask", 1, "TODO" }

        };
        String insertSql = "insert into process_task (id, task_name, priority, status) values (?, ?, ?, ?)";

        try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(insertSql);) {
            setupTestData(testdata, stmt);
            stmt.executeBatch();

            conn.commit();
        }

    }

    private Connection getConnection() throws SQLException {
        return TestDb.getDataSource().getConnection();
    }

    private void setupTestData(Object[][] testdata, PreparedStatement stmt) throws SQLException {
        for (int i = 1; i <= testdata.length; i++) {

            Object[] args = testdata[i - 1];
            for (int j = 1; j <= args.length; j++) {
                stmt.setObject(j, args[j - 1]);
            }
            stmt.addBatch();

        }
    }

    @After
    public void clear_testdata() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement("truncate table process_task");) {
            stmt.execute();
        }
    }

    @Test
    public void shall_read_queue_from_multiple_connections() throws Exception {

        try (Connection conn1 = getConnection();
                Connection conn2 = getConnection();
                Connection conn3 = getConnection();) {

            long task1 = getTask(conn1);
            long task2 = getTask(conn2);
            long task3 = getTask(conn3);

            assertThat(task1).isEqualTo(1);
            assertThat(task2).isEqualTo(2);
            assertThat(task3).isEqualTo(3);

            long task1_1 = getTask(conn1);
            assertThat(task1).isEqualTo(task1_1);
        }
    }

    @Test
    public void shall_read_queue() throws Exception {

        try (Connection conn1 = getConnection();
                Connection conn2 = getConnection();
                Connection conn3 = getConnection();) {

            long task1 = getTaskExcept(conn1, "DONE");
            long task2 = getTaskExcept(conn2, "DONE");
            long task3 = getTaskExcept(conn3, "DONE");

            assertThat(task1).isEqualTo(1);
            assertThat(task2).isEqualTo(3);
            assertThat(task3).isEqualTo(4);

            long task3_1 = getTaskExcept(conn3, "DONE");
            assertThat(task3).isEqualTo(task3_1);
        }
    }

    private long getTask(Connection conn) throws SQLException {
        return getTaskExcept(conn, "N/A");
    }

    private long getTaskExcept(Connection conn, String exceptStatus) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "select id from process_task where status not in (?) order by priority, id  for update skip locked");) {

            ps.setFetchSize(1);
            ps.setString(1, exceptStatus);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong("id");
            } else {
                return -1L;
            }
        }
    }
}
