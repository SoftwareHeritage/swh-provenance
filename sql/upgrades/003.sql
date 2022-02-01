-- SWH DB schema upgrade
-- from_version: 2
-- to_version: 3
-- description: keep unique indices for origins and locations in a hash column


insert into dbversion(version, release, description)
    values(3, now(), 'Work In Progress');

alter table location
    drop constraint if exists location_path_key;

create unique index on location(digest(path, 'sha1'));

alter table origin
    drop constraint if exists origin_url_key;


-- psql variables to get the current database flavor
select position('denormalized' in swh_get_dbflavor()::text) = 0 as dbflavor_norm \gset
select position('without-path' in swh_get_dbflavor()::text) = 0 as dbflavor_with_path \gset

\if :dbflavor_with_path

\if :dbflavor_norm
--
-- with path and normalized
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

\else
--
-- with path and denormalized
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
\endif
-- :dbflavor_with_path
