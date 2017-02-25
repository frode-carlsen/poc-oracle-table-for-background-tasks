h1. Proof-of-concept code:

Using oracle database table + skip locked to handle distributed/clustered background processing with low contention and easy recovery.

Useful if on an oracle database (should also work on Postgresql 9.5+ which supports skip locked).  Could also possibly be made to work with DB2 (using skip locked data) and SQLServer (using READPAST)

Similar to how Oracle SOA Suite - DBAdapter handles distributed polling.
* http://www.ateam-oracle.com/db-adapter-distributed-polling-skip-locked-demystified/
* https://docs.oracle.com/cd/E23943_01/integration.1111/e10231/adptr_db.htm#BGBIJHAC

The code uses a single polling thread (per jvm), which polls for things to do every 500ms (configurable) and pulls up to 2 tasks (also configurable) per polling round.  Once any tasks are found, these are dispatched on separate threads (task threads). Each task runs in its own thread, with its own transaction.  If more than 1 jvm is polling for tasks, the skip locked handling ensures they each see and process different tasks - which makes for relatively simple and easy distribution/scaling.

The task threads re-acquires the lock if possible and 'could do some work' and updates status to 'DONE'. If the lock isn't available, then someone else is on to it. If the task threads don't pick up a task within 30 seconds (configurable), the polling thread will try and reschedule assuming it's failed.  Once picked up by a task thread, it checks if it's already done before it proceeds to do work, so no duplicate processing will ever happen.

Multiple servers can happily poll at the same time, only one server will get the same task (assuming a horizontal scalability setup or cookie cutter design). If a server is taken offline then some other server will pick up any undone tasks. 

This is not a complete example or beautified production code. No actual work is performed other than a symbolic status change from 'TODO' to 'DONE'.

For a real implementation, connecting this to some actual 'work' should be made pluggable.  For example using a ServiceLoader or some other dispatching mechanism inside the RunTask class.

For testing:

1. Start the DatabasePoller class
2. Insert some rows into the dbqueue table with any status but 'DONE' and commit.
3. Query the table read-only until all are 'DONE'.
4. For fun, do a select * from dbqueue where id=<..> for update skip locked on a row where you've set status='TODO' in a separte window ,and then start the application.  Watch it never become 'DONE' until you commit/rollback 

A couple of notes if there is a large number of tasks generated/sec:

1. once done with work, the row processed should change status (to 'DONE') or alternatively be marked as logically deleted (sql needs updating to support).  Using logically delete rather than physical delete works better if table is highly volatile (size volatility) which improves use of statistics.  Physical delete can be done as a background task periodically.  A separate partition could be used to handle logically deleted entries
2. it might be useful to remove all stats on the table and lock down stats (to avoid automatically gathering statistics continually). The optimizer then uses dynamic sampling.

A couple of notes on possible added-value functionality:

1. retry and backoff-algorithm in case a task fails (at present this will just be rescheduled).
2. prioritisation and selection criteria (e.g. do-not-run-earlier-than-time)
3. exlusive or (only run one task within a given set of tasks concurrently)
 

