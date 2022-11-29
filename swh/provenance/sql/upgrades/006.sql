alter table content_in_revision add column revision_date timestamptz;  -- left this column null for now, needs a data migration

comment on column content_in_revision.revision_date is 'Date of the revision';

create or replace function swh_mktemp_relation_add() returns void
    language sql
as $$
    create temp table tmp_relation_add (
        src sha1_git not null,
        dst sha1_git not null,
        path unix_path,
        dst_date timestamptz
    ) on commit drop
$$;

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
        case
          when src_table = 'content'::regclass and dst_table = 'revision'::regclass then
            select_fields := 'D.id, L.id, dst_date as revision_date';
            join_location := 'inner join location as L on (digest(L.path,''sha1'') = digest(V.path,''sha1''))';
          when src_table in ('content'::regclass, 'directory'::regclass) then
            select_fields := 'D.id, L.id';
            join_location := 'inner join location as L on (digest(L.path,''sha1'') = digest(V.path,''sha1''))';
          else
            select_fields := 'D.id';
            join_location := '';
        end case;

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

create unique index on content_in_revision(content, revision_date, revision, location) where revision_date is not null;

create table content_in_revision_add_date_progress (
  updated bigint not null,
  last_content bigint not null,
  date timestamptz not null,
  single_row bool not null unique check (single_row != false) default true);

create or replace function content_in_revision_add_date(num_contents bigint default 1000)
returns setof content_in_revision_add_date_progress
language sql
as $$
  with content_ids as (
    select distinct content
    from content_in_revision
    where content > coalesce((select last_content from content_in_revision_add_date_progress limit 1), 0)
    order by content
    limit content_in_revision_add_date.num_contents
  ),
  updated_rows as (
    update content_in_revision
    set
      revision_date = (select date from revision where id=content_in_revision.revision)
    where
      revision_date is null
      and content > (select min(content) from content_ids)
      and content <= (select max(content) from content_ids)
    returning content
  ),
  updated_progress as (
    insert into content_in_revision_add_date_progress
      (updated, last_content, date)
    values
      ((select count(*) from updated_rows), (select max(content) from updated_rows), now())
    on conflict (single_row) do update set
      updated=content_in_revision_add_date_progress.updated+EXCLUDED.updated,
      last_content=EXCLUDED.last_content,
      date=EXCLUDED.date
    returning *
  )
  select * from updated_progress
$$;
