-- psql variables to get the current database flavor
select position('denormalized' in swh_get_dbflavor()::text) = 0 as dbflavor_norm \gset

create or replace function swh_mktemp_relation_add() returns void
    language sql
as $$
    create temp table tmp_relation_add (
        src sha1_git not null,
        dst sha1_git not null,
        path unix_path
    ) on commit drop
$$;

\if :dbflavor_norm

--
-- normalized
--

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

-- :dbflavor_norm
\else

--
-- denormalized
--

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
    select CL.sha1 as content,
           R.sha1 as revision,
           R.date as date,
           O.url as origin,
           L.path as path
    from (
      select C.sha1 as sha1,
             unnest(CR.revision) as revision,
             unnest(CR.location) as location
      from content_in_revision as CR
      inner join content as C on (C.id = CR.content)
      where C.sha1 = content_id
    ) as CL
    inner join revision as R on (R.id = CL.revision)
    inner join location as L on (L.id = CL.location)
    left join origin as O on (O.id = R.origin)
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
    (with
     cntrev as (
      select C.sha1 as sha1,
             unnest(CR.revision) as revision,
             unnest(CR.location) as location
      from content_in_revision as CR
      inner join content as C on (C.id = CR.content)
      where C.sha1 = content_id)
     select CR.sha1 as content,
            R.sha1 as revision,
            R.date as date,
            O.url as origin,
            L.path as path
     from cntrev as CR
     inner join revision as R on (R.id = CR.revision)
     inner join location as L on (L.id = CR.location)
     left join origin as O on (O.id = R.origin))
    union
    (with
     cntdir as (
     select C.sha1 as sha1,
            unnest(CD.directory) as directory,
            unnest(CD.location) as location
     from content as C
     inner join content_in_directory as CD on (CD.content = C.id)
     where C.sha1 = content_id),
     cntrev as (
      select CD.sha1 as sha1,
             L.path as path,
             unnest(DR.revision) as revision,
             unnest(DR.location) as location
      from cntdir as CD
      inner join directory_in_revision as DR on (DR.directory = CD.directory)
      inner join location as L on (L.id = CD.location))
     select CR.sha1 as content,
            R.sha1 as revision,
            R.date as date,
            O.url as origin,
            case DL.path
                when ''  then CR.path
                when '.' then CR.path
                else (DL.path || '/' || CR.path)::unix_path
            end as path
     from cntrev as CR
     inner join revision as R on (R.id = CR.revision)
     inner join location as DL on (DL.id = CR.location)
     left join origin as O on (O.id = R.origin))
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
        proj_dst_id text;
        proj_unnested text;
        proj_location text;
        join_location text;
        filter_inner_result text;
        filter_outer_result text;
    begin
        if rel_table = 'revision_before_revision'::regclass then
            src_field := 'prev';
            dst_field := 'next';
        else
            src_field := src_table::text;
            dst_field := dst_table::text;
        end if;

        if src_table in ('content'::regclass, 'directory'::regclass) then
            proj_unnested := 'unnest(R.' || dst_field || ') as dst, unnest(R.location) as loc';
            proj_dst_id := 'CL.dst';
            join_location := 'inner join location as L on (L.id = CL.loc)';
            proj_location := 'L.path';
        else
            proj_unnested := 'R.' || dst_field || ' as dst';
            proj_dst_id := 'CL.dst';
            join_location := '';
            proj_location := 'NULL::unix_path';
        end if;

        case filter
            when 'filter-src'::rel_flt then
                filter_inner_result := 'where S.sha1 = any($1)';
                filter_outer_result := '';
            when 'filter-dst'::rel_flt then
                filter_inner_result := '';
                filter_outer_result := 'where D.sha1 = any($1)';
            else
                filter_inner_result := '';
                filter_outer_result := '';
        end case;

        return query execute format(
            'select CL.src, D.sha1 as dst, ' || proj_location || ' as path
             from (select S.sha1 as src, ' || proj_unnested || '
                   from %s as R
                   inner join %s as S on (S.id = R.' || src_field || ')
                   ' || filter_inner_result || ') as CL
             inner join %s as D on (D.id = ' || proj_dst_id || ')
             ' || join_location || '
             ' || filter_outer_result,
            rel_table, src_table, dst_table
        ) using sha1s;
    end;
$$;

\endif
-- :dbflavor_norm
