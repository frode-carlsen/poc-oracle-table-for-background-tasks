package fc.db.tablequeue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestDatabaseTablePollerApplication {
    private static final Logger log = LoggerFactory.getLogger(TestDatabaseTablePollerApplication.class);

    /** Test method. */
    public static void main(String[] args) {

        TestDb.initTestDb(false, "filesystem:./src/main/resources/db/testdata");
        
        TaskDispatcher taskDispatcher = (id, taskName) -> {
            if (id == 3) {
                // TODO: throws a failure for just for impact
                throw new IllegalArgumentException("Unknown task id=" + 3, new UnsupportedOperationException()); //$NON-NLS-1$
            }
            log.info("*********** Processed: id=" + id + ", task=" + taskName); //$NON-NLS-1$ //$NON-NLS-2$
            return;
        };

        new DatabaseTablePoller(5, 2, TestDb.getDataSource(), taskDispatcher).start();
    }
}
