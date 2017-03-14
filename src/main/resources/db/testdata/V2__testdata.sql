insert into process_task(id, priority, status, task_name) select * from
(
WITH data(r) AS (
      SELECT 1 r FROM dual
      UNION ALL
        SELECT r+1 FROM data WHERE r < 10000
      )
      select r, 1, 'TODO', 'Task ' || r  from data
  ) 
  ;