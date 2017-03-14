package fc.db.tablequeue;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestDatabaseTablePollerApplication {
	private static final Logger log = LoggerFactory.getLogger(TestDatabaseTablePollerApplication.class);

	/** Test method. */
	public static void main(String[] args) {

		TestDb.initTestDb(false, "filesystem:./src/main/resources/db/testdata");

		TaskDispatcher taskDispatcher = (task) -> {
			if (Objects.equals(task.getId(), 3L)) {
				// TODO: throws a failure for just for impact
				throw new IllegalArgumentException("Unknown task id=" + task.getId(), //$NON-NLS-1$
						new UnsupportedOperationException());
			}
			log.info("*********** Processed: id=" + task.getId() + ", task=" + task.getTaskName()); //$NON-NLS-1$ //$NON-NLS-2$
			return;
		};

		new TaskRunner(5, 2, TestDb.getDataSource(), taskDispatcher).start();
	}
}
