
create table process_task (
	id number(19, 0) primary key,
    task_name varchar2(200 char) not null,
	priority number(3, 0) default 0 not null,
	status varchar2(20 char) default 'KLAR' not null,
	task_parameters varchar2(4000 char),
	
    next_run_after timestamp(0) default current_timestamp,
	secs_to_next_try number(10, 0) default 30,
	failed_attempts number(5, 0) default 0,
	max_attempts number(5, 0) default 3,
	last_run timestamp,
	last_error clob,
	last_node varchar2(50 char),
	constraint chk_process_task_status check (status in ('TODO', 'FAILED', 'WAITING_RESPONSE', 'DONE'))
)
enable row movement;

create sequence seq_process_task increment by 50 nocache nocycle;

create index idx_process_task_status on process_task(status);
