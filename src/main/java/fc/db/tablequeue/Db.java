package fc.db.tablequeue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/** simple setup for testing. */
public class Db {

    private static DataSource dataSource;

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:oracle:thin:@localhost:1521/XE");
        config.setMaximumPoolSize(5);
        config.setUsername("testdb");
        config.setPassword("testdb");
        config.setAutoCommit(false);

        HikariDataSource ds = new HikariDataSource(config);
        dataSource = ds;

    }

    private static final AtomicBoolean INITIALISED = new AtomicBoolean();

    public static void initTestDb() {

        if (INITIALISED.compareAndSet(false, true)) {
            Flyway flyway = new Flyway();

            flyway.setDataSource(dataSource);
            flyway.setLocations("filesystem:./src/main/resources/db/migrations");
            flyway.setCleanOnValidationError(true);
            flyway.clean();
            flyway.migrate();
        }
    }

    public static Connection createConnection() throws SQLException {
        initTestDb();
        return dataSource.getConnection();
    }
}
