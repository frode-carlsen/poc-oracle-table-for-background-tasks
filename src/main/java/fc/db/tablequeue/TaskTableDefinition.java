package fc.db.tablequeue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * SQL Definitions of task table.
 */
public class TaskTableDefinition {

	public String doneFlag() {
		return "DONE"; //$NON-NLS-1$
	}

	public String failedFlag() {
		return "FAILED"; //$NON-NLS-1$
	}

	public String readyFlag() {
		return "TODO"; //$NON-NLS-1$
	}

	public String getFailedAttemptsColumnName() {
		return "failed_attempts"; //$NON-NLS-1$
	}

	public String getIdColumnName() {
		return "id"; //$NON-NLS-1$
	}

	public String getLastAttemptTimestampColumnName() {
		return "last_run"; //$NON-NLS-1$
	}

	public String getLastErrorColumnName() {
		return "last_error"; //$NON-NLS-1$
	}

	public String getMaxAttemptsColumnName() {
		return "max_attempts"; //$NON-NLS-1$
	}

	public String getNextAttemptAfterTimestampColumnName() {
		return "next_run_after"; //$NON-NLS-1$
	}

	public String getSecondsBeforeNextAttemptColumnName() {
		return "secs_to_next_try"; //$NON-NLS-1$
	}

	protected PreparedStatement getStatementForPickingSingleTaskById(Connection conn, Long id, String taskName,
			Timestamp timestampLowWatermark) throws SQLException {
		String sql = "SELECT id, priority, task_name, status, task_parameters, last_run, next_run_after, secs_to_next_try, failed_attempts, max_attempts, last_error" //$NON-NLS-1$
				+ " FROM process_task" //$NON-NLS-1$
				+ " WHERE id=?"//$NON-NLS-1$
				+ "   AND task_name=?"//$NON-NLS-1$
				+ "   AND last_run>=?" //$NON-NLS-1$
				+ " FOR UPDATE SKIP LOCKED"; //$NON-NLS-1$

		PreparedStatement pstmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		pstmt.setLong(1, id);
		pstmt.setString(2, taskName);
		pstmt.setTimestamp(3, timestampLowWatermark);
		return pstmt;
	}

	protected PreparedStatement getStatementForPolling(Connection conn) throws SQLException {
		String sql = "SELECT id, priority, status, task_name, last_run" //$NON-NLS-1$
				+ " FROM process_task" //$NON-NLS-1$
				+ " WHERE (next_run_after < systimestamp)" //$NON-NLS-1$
				+ "       AND failed_attempts < max_attempts" //$NON-NLS-1$
				+ "       AND status IN (?) " //$NON-NLS-1$
				+ " ORDER BY " //$NON-NLS-1$
				+ "       priority, last_run NULLS FIRST, id" //$NON-NLS-1$
				+ " FOR UPDATE SKIP LOCKED"; //$NON-NLS-1$

		PreparedStatement pstmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		pstmt.setString(1, readyFlag());
		return pstmt;

	}

	public String getStatusColumnName() {
		return "status"; //$NON-NLS-1$
	}

	public String getTaskNameColumnName() {
		return "task_name"; //$NON-NLS-1$
	}

	public String getParametersColumnName() {
		return "task_parameters";
	}

	public String getPriorityColumnName() {
		return "priority";
	}

	void writeTask(TaskInfo task, Connection conn) throws SQLException, IOException {
		String insertSql = "insert into process_task(id, task_name, priority, next_run_after, task_parameters )"
				+ " values (seq_process_task.nextval, ?, ?, ?, ?)";
		try (PreparedStatement stmt = conn.prepareStatement(insertSql);) {
			stmt.setString(1, task.getTaskName());
			stmt.setInt(2, task.getPriority());

			Timestamp after = task.getRunAfterDateTime() == null ? null : Timestamp.valueOf(task.getRunAfterDateTime());
			stmt.setTimestamp(3, after);

			String paramString = task.getPropertiesAsString();
			stmt.setString(4, paramString);

			stmt.executeUpdate();
		}
	}

	public String getServerNodeColumnName() {
		return "last_node";
	}

}
