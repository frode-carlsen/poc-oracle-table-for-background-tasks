package fc.db.tablequeue;

import java.io.IOException;
import java.io.StringReader;
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
import java.util.Objects;
import java.util.Properties;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Create a runnable to run a single task in a new transaction. */
public class RunSingleTaskFactory {

	private static final Logger log = LoggerFactory.getLogger(TaskRunner.class);

	private final TaskDispatcher taskDispatcher;
	private final TaskTableDefinition tableDef;
	private final DataSource dataSource;

	public RunSingleTaskFactory(TaskDispatcher taskDispatcher, TaskTableDefinition tableDefinition,
			DataSource dataSource) {
		Objects.requireNonNull(taskDispatcher, "taskDispatcher"); //$NON-NLS-1$
		Objects.requireNonNull(tableDefinition, "tableDefinition"); //$NON-NLS-1$
		Objects.requireNonNull(dataSource, "dataSource"); //$NON-NLS-1$

		this.taskDispatcher = taskDispatcher;
		this.tableDef = tableDefinition;
		this.dataSource = dataSource;
	}

	public Runnable newTask(Long id, String taskName, Timestamp thisTryTimestamp) {
		return new RunSingleTask(id, taskName, thisTryTimestamp);
	}

	/**
	 * Run a single task. Multiple tasks may be run concurrently on different
	 * threads and in different JVMs.
	 * <p>
	 * Only one task is run by one thread at a time, with its own transaction.
	 */
	protected class RunSingleTask implements Runnable {

		private final Long id;
		private Timestamp timestampLowWatermark;
		private String taskName;

		protected RunSingleTask(Long id, String taskName, Timestamp timestampLowWatermark) {
			Objects.requireNonNull(id, "id"); //$NON-NLS-1$
			Objects.requireNonNull(taskName, "taskName"); //$NON-NLS-1$
			Objects.requireNonNull(timestampLowWatermark, "timestampLowWatermark"); //$NON-NLS-1$

			this.id = id;
			this.taskName = taskName;
			this.timestampLowWatermark = timestampLowWatermark;
		}

		@Override
		public void run() {
			// set up thread name so it's reflected directly in logs
			String oldThreadName = Thread.currentThread().getName();
			Thread.currentThread().setName(oldThreadName + " [id=" + id + ", taskName=" + taskName + "]"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			try {
				doInTransaction();
			} finally {
				Thread.currentThread().setName(oldThreadName);
			}
		}

		protected void doInTransaction() {
			try (Connection conn = newConnection();
					PreparedStatement stmt = tableDef.getStatementForPickingSingleTaskById(conn, id, taskName, timestampLowWatermark);) {

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
			} catch (SQLTransientException | SQLNonTransientConnectionException | SQLRecoverableException e) {
				handleTransientAndRecoverableException(this.taskName, e);
			} catch (Exception e) {
				log.warn("Failed to run task, id=" + id + ", will be automatically repeated", e); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}

		protected Connection newConnection() throws SQLException {
			return dataSource.getConnection();
		}

		protected boolean handleTask(Connection conn, ResultSet rs) throws SQLException {
			String taskName = rs.getString(tableDef.getTaskNameColumnName());
			int previousFailedAttempts = rs.getInt(tableDef.getFailedAttemptsColumnName());

			markTaskBeingProcessed(rs);

			// set up a savepoint to rollback to in case of failure
			Savepoint savepoint = conn.setSavepoint();

			try {
				TaskInfo task = readTask(rs, taskName);
				doWork(task);
				handleTaskSuccess(rs);
			} catch (SQLTransientException | SQLNonTransientConnectionException | SQLRecoverableException e) {
				throw e;
			} catch (Exception e) {
				conn.rollback(savepoint);
				// assume error can be written to the queue
				handleTaskException(rs, taskName, previousFailedAttempts, e);
			}

			rs.updateRow();
			return true;

		}

		protected TaskInfo readTask(ResultSet rs, String taskName) throws SQLException, IOException {
			TaskInfo.Builder taskBuilder = TaskInfo.build(taskName)
					.withPriority(rs.getInt(tableDef.getPriorityColumnName()))
					.withId(id);

			String paramString = rs.getString(tableDef.getParametersColumnName());
			if (paramString != null) {
				Properties props = new Properties();
				props.load(new StringReader(paramString));
				taskBuilder.withProperties(props);
			}

			TaskInfo task = taskBuilder.build();
			return task;
		}

		/** do real work. */
		protected void doWork(TaskInfo task) {
			taskDispatcher.dispatch(task);
		}

		/** handle recoverable / transient exception. */
		protected void handleTransientAndRecoverableException(String taskName, SQLException e) {
			/*
			 * assume won't help to try and write to database just now, log only
			 * instead
			 */
			log.warn("Failed to process task: id=" + id + ", taskName=" + taskName + ". Will automatically retry", e); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		}

		/** tracks last time a task was attempted and by who. */
		protected void markTaskBeingProcessed(ResultSet rs) throws SQLException {
			// mark row being processed with timestamp and server process id
			rs.updateTimestamp(tableDef.getLastAttemptTimestampColumnName(), Timestamp.valueOf(LocalDateTime.now()));
			rs.updateString(tableDef.getServerNodeColumnName(), Utils.getJvmUniqueProcessName());
			rs.updateRow();
		}

		/** update status on task success. */
		protected void handleTaskSuccess(ResultSet rs) throws SQLException {
			rs.updateString(tableDef.getLastErrorColumnName(), null);
			rs.updateTimestamp(tableDef.getNextAttemptAfterTimestampColumnName(), null);
			rs.updateString(tableDef.getStatusColumnName(), tableDef.doneFlag());
		}

		/** handle exception on task. Update failure count if less than max. */
		protected void handleTaskException(ResultSet rs, String taskName, int failedAttempts, Exception e)
				throws SQLException {
			failedAttempts++;
			int maxAttempts = rs.getInt(tableDef.getMaxAttemptsColumnName());
			int secsBetweenAttempts = rs.getInt(tableDef.getSecondsBeforeNextAttemptColumnName());
			rs.updateInt(tableDef.getFailedAttemptsColumnName(), failedAttempts);

			if (failedAttempts >= maxAttempts) {
				rs.updateTimestamp(tableDef.getNextAttemptAfterTimestampColumnName(), null);
				rs.updateString(tableDef.getStatusColumnName(), tableDef.failedFlag());
			} else {
				Timestamp newTimestamp = Timestamp.valueOf(LocalDateTime.now().plusSeconds(secsBetweenAttempts));
				rs.updateTimestamp(tableDef.getNextAttemptAfterTimestampColumnName(), newTimestamp);
			}

			String exceptionString = Utils.getLastError(id, taskName, e);
			rs.updateString(tableDef.getLastErrorColumnName(), exceptionString);
		}

		/** check if task is ready to be processed. */
		protected boolean isInProcessableState(ResultSet rs) throws SQLException {
			String status = rs.getString(tableDef.getStatusColumnName());
			return !Objects.equals(tableDef.doneFlag(), status);
		}

	}

	TaskInfo readTask(ResultSet rs) {
		// TODO Auto-generated method stub
		return null;
	}

}
