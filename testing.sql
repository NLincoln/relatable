create table departments (
  id integer(8),
  dept_name varchar(20)
);

insert into departments (id, dept_name) VALUES (1, 'management'), (2, 'engineering');

create table employees (
  id integer(8),
  username varchar(20),
  department_id integer(8)
);

insert into employees (id, username, department_id) VALUES (1, 'nlincoln', 2), (2, 'manager', 1);

select * from departments;
select id, username, department_id from employees;

select departments.id, employees.id from departments, employees;
select * from departments, employees;
