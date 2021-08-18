select position('denormalized' in swh_get_dbflavor()::text) = 0 as dbflavor_norm \gset
select position('with-path' in swh_get_dbflavor()::text) != 0 as dbflavor_with_path \gset

create type relation_row as (src sha1_git, dst sha1_git, loc unix_path);

\if :dbflavor_norm

\if :dbflavor_with_path
--
-- with path and normalized
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

\else
--
-- without path and normalized
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
            '\x'::unix_path as path
     from content as C
     inner join content_in_revision as CR on (CR.content = C.id)
     inner join revision as R on (R.id = CR.revision)
     left join origin as O on (O.id = R.origin)
     where C.sha1 = content_id
     order by date, revision, origin asc limit 1
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
          '\x'::unix_path as path
   from content as C
   inner join content_in_revision as CR on (CR.content = C.id)
   inner join revision as R on (R.id = CR.revision)
   left join origin as O on (O.id = R.origin)
   where C.sha1 = content_id)
  union
  (select C.sha1 as content,
          R.sha1 as revision,
          R.date as date,
          O.url as origin,
          '\x'::unix_path as path
   from content as C
   inner join content_in_directory as CD on (CD.content = C.id)
   inner join directory_in_revision as DR on (DR.directory = CD.directory)
   inner join revision as R on (R.id = DR.revision)
   left join origin as O on (O.id = R.origin)
   where C.sha1 = content_id)
   order by date, revision, origin, path limit early_cut
$$;

-- :dbflavor_with_path
\endif

-- :dbflavor_norm
\else

\if :dbflavor_with_path
--
-- with path and denormalized
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
   (with cntrev as (
      select C.sha1 as sha1,
             unnest(CR.revision) as revision,
             unnest(CR.location) as location
      from content_in_revision as CR
      inner join content as C on (C.id = CR.content)
      where C.sha1 = content_id
    )
    select CR.sha1 as content,
           R.sha1 as revision,
           R.date as date,
           O.url as origin,
           L.path as path
    from cntrev as CR
    inner join revision as R on (R.id = CR.revision)
    inner join location as L on (L.id = CR.location)
    left join origin as O on (O.id = R.origin)
    )
   union
   (with cntdir as (
     select C.sha1 as sha1,
            unnest(CD.directory) as directory,
            unnest(CD.location) as location
     from content as C
     inner join content_in_directory as CD on (CD.content = C.id)
     where C.sha1 = content_id
     ),
    cntrev as (
     select CD.sha1 as sha1,
            L.path as path,
            unnest(DR.revision) as revision,
            unnest(DR.location) as prefix
     from cntdir as CD
     inner join directory_in_revision as DR on (DR.directory = CD.directory)
     inner join location as L on (L.id = CD.location)
     )
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
   inner join location as DL on (DL.id = CR.prefix)
   left join origin as O on (O.id = R.origin)
   )
   order by date, revision, origin, path limit early_cut
$$;

\else
--
-- without path and denormalized
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
           '\x'::unix_path as path
    from (
      select C.sha1, unnest(revision) as revision
      from content_in_revision as CR
      inner join content as C on (C.id = CR.content)
      where C.sha1=content_id
    ) as CL
    inner join revision as R on (R.id = CL.revision)
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
   (with cntrev as (
      select C.sha1 as sha1,
             unnest(CR.revision) as revision
      from content_in_revision as CR
      inner join content as C on (C.id = CR.content)
      where C.sha1 = content_id
    )
    select CR.sha1 as content,
           R.sha1 as revision,
           R.date as date,
           O.url as origin,
          '\x'::unix_path as path
    from cntrev as CR
    inner join revision as R on (R.id = CR.revision)
    left join origin as O on (O.id = R.origin)
    )
   union
   (with cntdir as (
     select C.sha1 as sha1,
            unnest(CD.directory) as directory
     from content as C
     inner join content_in_directory as CD on (CD.content = C.id)
     where C.sha1 = content_id
     ),
    cntrev as (
     select CD.sha1 as sha1,
            unnest(DR.revision) as revision
     from cntdir as CD
     inner join directory_in_revision as DR on (DR.directory = CD.directory)
     )
   select CR.sha1 as content,
          R.sha1 as revision,
          R.date as date,
          O.url as origin,
          '\x'::unix_path as path
   from cntrev as CR
   inner join revision as R on (R.id = CR.revision)
   left join origin as O on (O.id = R.origin)
   )
   order by date, revision, origin, path limit early_cut
$$;

\endif
-- :dbflavor_with_path
\endif
-- :dbflavor_norm
