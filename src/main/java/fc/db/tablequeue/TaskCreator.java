package fc.db.tablequeue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class TaskCreator {
	private static TaskTableDefinition tableDef = new TaskTableDefinition();

	public void putTask(TaskInfo task, Connection conn) throws SQLException, IOException {
		tableDef.writeTask(task, conn);
	}
	
}
