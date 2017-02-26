package fc.db.tablequeue;

public interface TaskDispatcher {

    void dispatch(Long id, String taskName, int failedAttempts);
}
