create table users (
  id integer(8),
  username varchar(20)
);

insert into users (id, username) VALUE (1, 'nlincoln');
insert into users (id, username) VALUE (2, 'other');

select id, username from users;
