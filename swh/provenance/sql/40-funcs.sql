create or replace function swh_mktemp_relation_add() returns void
    language sql
as $$
    create temp table tmp_relation_add (
        src sha1_git not null,
        dst sha1_git not null,
        path unix_path
    ) on commit drop
$$;

create or replace function swh_provenance_content_find_first(content_id sha1_git)
    returns table (
        content sha1_git,
        revision sha1_git,
        date timestamptz,
        origin text,
        path unix_path
    )
    language sql
    stable
as $$
    select C.sha1 as content,
           R.sha1 as revision,
           R.date as date,
           O.url as origin,
           L.path as path
    from content as C
    inner join content_in_revision as CR on (CR.content = C.id)
    inner join location as L on (L.id = CR.location)
    inner join revision as R on (R.id = CR.revision)
    left join origin as O on (O.id = R.origin)
    where C.sha1 = content_id
    order by date, revision, origin, path asc limit 1
$$;

create or replace function swh_provenance_content_find_all(content_id sha1_git, early_cut int)
    returns table (
        content sha1_git,
        revision sha1_git,
        date timestamptz,
        origin text,
        path unix_path
    )
    language sql
    stable
as $$
    (select C.sha1 as content,
            R.sha1 as revision,
            R.date as date,
            O.url as origin,
            L.path as path
     from content as C
     inner join content_in_revision as CR on (CR.content = C.id)
     inner join location as L on (L.id = CR.location)
     inner join revision as R on (R.id = CR.revision)
     left join origin as O on (O.id = R.origin)
     where C.sha1 = content_id)
    union
    (select C.sha1 as content,
            R.sha1 as revision,
            R.date as date,
            O.url as origin,
            case DL.path
                when '' then CL.path
                when '.' then CL.path
                else (DL.path || '/' || CL.path)::unix_path
            end as path
     from content as C
     inner join content_in_directory as CD on (CD.content = C.id)
     inner join directory_in_revision as DR on (DR.directory = CD.directory)
     inner join revision as R on (R.id = DR.revision)
     inner join location as CL on (CL.id = CD.location)
     inner join location as DL on (DL.id = DR.location)
     left join origin as O on (O.id = R.origin)
     where C.sha1 = content_id)
    order by date, revision, origin, path limit early_cut
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

create or replace function swh_provenance_relation_get(
    rel_table regclass, src_table regclass, dst_table regclass, filter rel_flt, sha1s sha1_git[]
)
    returns table (
        src sha1_git,
        dst sha1_git,
        path unix_path
    )
    language plpgsql
    stable
as $$
    declare
        src_field text;
        dst_field text;
        join_location text;
        proj_location text;
        filter_result text;
    begin
        if rel_table = 'revision_before_revision'::regclass then
            src_field := 'prev';
            dst_field := 'next';
        else
            src_field := src_table::text;
            dst_field := dst_table::text;
        end if;

        if src_table in ('content'::regclass, 'directory'::regclass) then
            join_location := 'inner join location as L on (L.id = R.location)';
            proj_location := 'L.path';
        else
            join_location := '';
            proj_location := 'NULL::unix_path';
        end if;

        case filter
            when 'filter-src'::rel_flt then
                filter_result := 'where S.sha1 = any($1)';
            when 'filter-dst'::rel_flt  then
                filter_result := 'where D.sha1 = any($1)';
            else
                filter_result := '';
        end case;

        return query execute format(
            'select S.sha1 as src, D.sha1 as dst, ' || proj_location || ' as path
             from %s as R
             inner join %s as S on (S.id = R.' || src_field || ')
             inner join %s as D on (D.id = R.' || dst_field || ')
             ' || join_location || '
             ' || filter_result,
            rel_table, src_table, dst_table
        ) using sha1s;
    end;
$$;

