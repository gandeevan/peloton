create table foo (a int);

begin;
insert into foo values (1);
insert into foo values (2);
select * from foo;
commit;
