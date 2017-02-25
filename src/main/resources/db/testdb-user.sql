create role myrole not identified;
grant create session, create sequence,  create view, alter session, create table to myrole;

grant myrole to testdb;

grant execute on schema testdb to testdb;
