create table dbqueue (

   id number(19, 0) primary key,
   task_name varchar2(100 char) not null,
   priority number(3, 0) default 0 not null,
   status varchar2(20 char) default 'TODO' not null,
   secs_between_attempts number(5, 0) default 30,
   failed_attempts number(5,0) default 0,
   max_attempts number(5,0) default 3,
   next_attempt_after timestamp(0) default current_timestamp,
   last_attempt_ts timestamp,
   last_error clob
)
enable row movement
;

create sequence seq_dbqueue;

create index idx_dbqueue_status on dbqueue(status);

