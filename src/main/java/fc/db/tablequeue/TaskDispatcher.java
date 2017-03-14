package fc.db.tablequeue;

public interface TaskDispatcher {

    void dispatch(TaskInfo task);
}
