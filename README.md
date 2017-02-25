# poc-oracle-table-for-background-tasks
Using oracle database table + skip locked to handle distributed/clustered background processing.
Useful if on an oracle database (can also be made to work on Postgres 9.6)

Similar to how Oracle SOA Suite - DBAdapter handles distributed polling.
http://www.ateam-oracle.com/db-adapter-distributed-polling-skip-locked-demystified/
https://docs.oracle.com/cd/E23943_01/integration.1111/e10231/adptr_db.htm#BGBIJHAC

The code uses a single polling thread (per jvm).  Once any tasks are found, these are dispatched on separate threads (task threads). Each task runs in its own thread, with its own transaction.
The task threads re-acquires the lock if possible and 'could do some work' and updates status to 'DONE'.  If the task threads doesn't pick up a task within 10 seconds, the polling thread will try and reschedule assuming it's failed.  Once picked up by a task thread, it checks if it's already done before it proceeds to do work, so no duplicate processing will take place.

This is not a complete example or beautified production code. No actual work is performed other than a symbolic status from 'TODO' to 'DONE'.

Connecting this to some actual 'work' should be made pluggable.  For example using a ServiceLoader or other dispatching mechanism inside the RunTask class.

For testing:
#. start the DatabasePoller class
#. insert some rows into the dbqueue table with any status but 'DONE' and commit.
#. query the table read-only until all are 'DONE'.
#. For fun, do a select * from dbqueue where id=<..> for update skip locked on a row where you've set status='TODO' in a separte window ,and then start the application.  Watch it never become 'DONE' until you commit/rollback 

A couple of notes if there is a large number of tasks generated/sec:
#. once done with work, the row processed should change status (to 'DONE') or alternatively be marked as logically deleted (sql needs updating to support).  Using logically delete rather than physical delete works better if table is highly volatile (size volatility) which improves use of statistics.  Physical delete can be done as a background task periodically.  A separate partition could be used to handle logically deleted entries
#. it might be useful to remove all stats on the table and lock down stats (to avoid automatically gathering statistics continually). The optimizer then uses dynamic sampling.

A couple of notes on desired functionality
#. retry and backoff-algorithm in case a task fails (at present this will just be rescheduled)


