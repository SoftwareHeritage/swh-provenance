-- SWH provenance DB schema upgrade
-- from_version: 3
-- to_version: 4
-- description: rename db flavor (without-path heving been removed)
--              will fail if the db is using a without-path flavor.
alter type database_flavor rename value 'with-path' to 'normalized';
alter type database_flavor rename value 'with-path-denormalized' to 'denormalized';

alter type database_flavor rename to database_flavor_old;

create type database_flavor as enum (
  'normalized',
  'denormalized'
);
comment on type database_flavor is 'Flavor of the current database';

drop function swh_get_dbflavor;

alter table dbflavor
  alter column flavor type database_flavor using flavor::text::database_flavor;

create function swh_get_dbflavor() returns database_flavor language sql stable as $$
  select coalesce((select flavor from dbflavor), 'normalized');
$$;

drop type database_flavor_old;

alter table content alter column date drop not null;
alter table directory alter column date drop not null;
