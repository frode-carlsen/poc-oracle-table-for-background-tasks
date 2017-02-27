package fc.db.tablequeue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/** simple setup for testing. */
class TestDb {

    private static final DataSource dataSource = getDataSource();

    static DataSource getDataSource() {
        if (dataSource != null) {
            return dataSource;
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:oracle:thin:@localhost:1521/XE");
        config.setMaximumPoolSize(5);
        config.setUsername("testdb");
        config.setPassword("testdb");
        config.setAutoCommit(false);

        return new HikariDataSource(config);
    }

    private static final AtomicBoolean INITIALISED = new AtomicBoolean();

    static void initTestDb(boolean forceClean, String... additionalMigrationLocations) {
        String[] locations = new String[1 + additionalMigrationLocations.length];
        locations[0] = "filesystem:./src/main/resources/db/migrations";
        System.arraycopy(additionalMigrationLocations, 0, locations, 1, additionalMigrationLocations.length);
        
        if (INITIALISED.compareAndSet(false, true)) {
            Flyway flyway = new Flyway();

            flyway.setDataSource(dataSource);
            flyway.setLocations(locations);
            flyway.setCleanOnValidationError(true);
            if (forceClean) {
                flyway.clean();
            }
            flyway.migrate();
        }
    }

    static Connection createConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
