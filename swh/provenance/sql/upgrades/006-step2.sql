alter table content_in_revision alter column revision_date set not null;

drop function if exists content_in_revision_add_date(bigint);
drop table if exists content_in_revision_add_date_progress;


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
           CR.revision_date as date,
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
            CR.revision_date as date,
            O.url as origin,
            L.path as path
     from content as C
     inner join content_in_revision as CR on (CR.content = C.id)
     inner join location as L on (L.id = CR.location)
     inner join revision as R on (R.id = CR.revision)
     left join origin as O on (O.id = R.origin)
     where C.sha1 = content_id
     order by date, revision, origin, path limit early_cut)
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
     where C.sha1 = content_id
     order by date, revision, origin, path limit early_cut)
    order by date, revision, origin, path limit early_cut
$$;

drop index if exists content_in_revision_content_revision_date_revision_location_idx;
