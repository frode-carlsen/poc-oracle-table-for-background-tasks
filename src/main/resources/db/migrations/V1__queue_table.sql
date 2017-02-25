create table dbqueue (

   id number(19, 0) primary key,
   priority number(3, 0) default 0 not null,
   last_try timestamp,
   status varchar2(20 char)
);

create sequence seq_dbqueue;

