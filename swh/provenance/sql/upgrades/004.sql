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

-- psql variables to get the current database flavor
select position('denormalized' in swh_get_dbflavor()::text) = 0 as dbflavor_norm \gset


\if :dbflavor_norm

--
-- normalized
--
create or replace function swh_provenance_relation_add_from_temp(
    rel_table regclass, src_table regclass, dst_table regclass
)
    returns void
    language plpgsql
    volatile
as $$
    declare
        select_fields text;
        join_location text;
    begin
        if src_table in ('content'::regclass, 'directory'::regclass) then
            select_fields := 'D.id, L.id';
            join_location := 'inner join location as L on (digest(L.path,''sha1'') = digest(V.path,''sha1''))';
        else
            select_fields := 'D.id';
            join_location := '';
        end if;

        execute format(
            'insert into %s (sha1)
              select distinct src
              from tmp_relation_add
              where not exists (select 1 from %s where %s.sha1=tmp_relation_add.src)
             on conflict do nothing',
             src_table, src_table, src_table);

        execute format(
            'insert into %s (sha1)
              select distinct dst
              from tmp_relation_add
              where not exists (select 1 from %s where %s.sha1=tmp_relation_add.dst)
             on conflict do nothing',
             dst_table, dst_table, dst_table);

        if src_table in ('content'::regclass, 'directory'::regclass) then
            insert into location(path)
              select distinct path
              from tmp_relation_add
              where not exists (select 1 from location
			    where digest(location.path,'sha1')=digest(tmp_relation_add.path,'sha1')
				)
             on conflict do nothing;
           end if;

        execute format(
            'insert into %s
               select S.id, ' || select_fields || '
               from tmp_relation_add as V
               inner join %s as S on (S.sha1 = V.src)
               inner join %s as D on (D.sha1 = V.dst)
               ' || join_location || '
             on conflict do nothing',
            rel_table, src_table, dst_table
        );
    end;
$$;

-- :dbflavor_norm
\else

--
-- denormalized
--

create or replace function swh_provenance_relation_add_from_temp(
    rel_table regclass, src_table regclass, dst_table regclass
)
    returns void
    language plpgsql
    volatile
as $$
    declare
        select_fields text;
        join_location text;
        group_entries text;
        on_conflict text;
    begin

        execute format(
            'insert into %s (sha1)
              select distinct src
              from tmp_relation_add
              where not exists (select 1 from %s where %s.sha1=tmp_relation_add.src)
             on conflict do nothing',
             src_table, src_table, src_table);

        execute format(
            'insert into %s (sha1)
              select distinct dst
              from tmp_relation_add
              where not exists (select 1 from %s where %s.sha1=tmp_relation_add.dst)
             on conflict do nothing',
             dst_table, dst_table, dst_table);

        if src_table in ('content'::regclass, 'directory'::regclass) then
            insert into location(path)
              select distinct path
              from tmp_relation_add
              where not exists (select 1 from location
			    where digest(location.path,'sha1')=digest(tmp_relation_add.path,'sha1')
				)
             on conflict do nothing;
           end if;

        if src_table in ('content'::regclass, 'directory'::regclass) then
            select_fields := 'array_agg(D.id), array_agg(L.id)';
            join_location := 'inner join location as L on (digest(L.path,''sha1'') = digest(V.path,''sha1''))';
            group_entries := 'group by S.id';
            on_conflict := format('
                (%s) do update
                set (%s, location) = (
                    with pairs as (
                        select distinct * from unnest(
                            %s.' || dst_table::text || ' || excluded.' || dst_table::text || ',
                            %s.location || excluded.location
                        ) as pair(dst, loc)
                    )
                    select array(select pairs.dst from pairs), array(select pairs.loc from pairs)
                )',
                src_table, dst_table, rel_table, rel_table, rel_table, rel_table
            );
        else
            select_fields := 'D.id';
            join_location := '';
            group_entries := '';
            on_conflict := 'do nothing';
        end if;

        execute format(
            'insert into %s
               select S.id, ' || select_fields || '
               from tmp_relation_add as V
               inner join %s as S on (S.sha1 = V.src)
               inner join %s as D on (D.sha1 = V.dst)
               ' || join_location || '
               ' || group_entries || '
             on conflict ' || on_conflict,
            rel_table, src_table, dst_table
        );
    end;
$$;


\endif
-- :dbflavor_norm
