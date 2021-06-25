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
        origin unix_path,
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
    inner join location as L on (CR.location = L.id)
    inner join revision as R on (CR.revision = R.id)
    left join origin as O on (R.origin=O.id)
    where C.sha1=content_id
    order by date, revision, origin, path asc limit 1
$$;

create or replace function swh_provenance_content_find_all(content_id sha1_git, early_cut int)
    returns table (
        content sha1_git,
        revision sha1_git,
        date timestamptz,
        origin unix_path,
        path unix_path
    )
    language sql
    stable
as $$
  (select C.sha1 as content,
          r.sha1 as revision,
          r.date as date,
          O.url as origin,
          l.path as path
   from content as c
   inner join content_in_revision as cr on (cr.content = c.id)
   inner join location as l on (cr.location = l.id)
   inner join revision as r on (cr.revision = r.id)
   left join origin AS O on (R.origin=O.id)
   where c.sha1=content_id)
  union
  (select c.sha1 as content,
          r.sha1 as revision,
          r.date as date,
          O.url as origin,
          case dirloc.path
            when '' then cntloc.path
            when '.' then cntloc.path
            else (dirloc.path || '/' || cntloc.path)::unix_path
          end as path
   from content as c
   inner join content_in_directory as cd on (c.id = cd.content)
   inner join directory_in_revision as dr on (cd.directory = dr.directory)
   inner join revision as r on (dr.revision = r.id)
   inner join location as cntloc on (cd.location = cntloc.id)
   inner join location as dirloc on (dr.location = dirloc.id)
   left join origin as O on (R.origin=O.id)
   where C.sha1=content_id)
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
        origin unix_path,
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
     inner join revision as R on (CR.revision = R.id)
     left join origin as O on (R.origin=O.id)
     where C.sha1=content_id
     order by date, revision, origin asc limit 1
$$;

create or replace function swh_provenance_content_find_all(content_id sha1_git, early_cut int)
    returns table (
        content sha1_git,
        revision sha1_git,
        date timestamptz,
        origin unix_path,
        path unix_path
    )
    language sql
    stable
as $$
  (select C.sha1 as content,
          r.sha1 as revision,
          r.date as date,
          O.url as origin,
          '\x'::unix_path as path
   from content as c
   inner join content_in_revision as cr on (cr.content = c.id)
   inner join revision as r on (cr.revision = r.id)
   left join origin as O on (R.origin=O.id)
   where c.sha1=content_id)
  union
  (select c.sha1 as content,
          r.sha1 as revision,
          r.date as date,
          O.url as origin,
          '\x'::unix_path as path
   from content as c
   inner join content_in_directory as cd on (c.id = cd.content)
   inner join directory_in_revision as dr on (cd.directory = dr.directory)
   inner join revision as r on (dr.revision = r.id)
   left join origin as O on (R.origin=O.id)
   where C.sha1=content_id)
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
        origin unix_path,
        path unix_path
    )
    language sql
    stable
as $$
    select C_L.sha1 as content,
           R.sha1 as revision,
           R.date as date,
           O.url as origin,
           L.path as path
    from (
      select C.sha1 as sha1,
             unnest(revision) as revision,
             unnest(location) as location
      from content_in_revision as C_R
      inner join content as C on (C.id=C_R.content)
      where C.sha1=content_id
    ) as C_L
    inner join revision as R on (R.id=C_L.revision)
    inner join location as L on (L.id=C_L.location)
    left join origin as O on (R.origin=O.id)
    order by date, revision, origin, path asc limit 1
$$;

create or replace function swh_provenance_content_find_all(content_id sha1_git, early_cut int)
    returns table (
        content sha1_git,
        revision sha1_git,
        date timestamptz,
        origin unix_path,
        path unix_path
    )
    language sql
    stable
as $$
   (with cnt as (
      select c.sha1 as sha1,
             unnest(c_r.revision) as revision,
             unnest(c_r.location) as location
      from content_in_revision as c_r
      inner join content as c on (c.id = c_r.content)
      where c.sha1 = content_id
    )
    select cnt.sha1 as content,
           r.sha1 as revision,
           r.date as date,
           O.url as origin,
           l.path as path
    from cnt
    inner join revision as r on (r.id = cnt.revision)
    inner join location as l on (l.id = cnt.location)
    left join origin as O on (R.origin=O.id)
    )
   union
   (with cnt as (
     select c.sha1 as content_sha1,
            unnest(cd.directory) as directory,
            unnest(cd.location) as location
     from content as c
     inner join content_in_directory as cd on (cd.content = c.id)
     where c.sha1 = content_id
     ),
    cntdir as (
     select cnt.content_sha1 as content_sha1,
            cntloc.path as file_path,
            unnest(dr.revision) as revision,
            unnest(dr.location) as prefix_location
     from cnt
     inner join directory_in_revision as dr on (dr.directory = cnt.directory)
     inner join location as cntloc on (cntloc.id = cnt.location)
     )
   select cntdir.content_sha1 as content,
          r.sha1 as revision,
          r.date as date,
          O.url as origin,
          case dirloc.path
             when ''  then cntdir.file_path
             when '.' then cntdir.file_path
             else (dirloc.path || '/' || cntdir.file_path)::unix_path
          end as path
   from cntdir
   inner join location as dirloc on (cntdir.prefix_location = dirloc.id)
   inner join revision as r on (cntdir.revision = r.id)
   left join origin as O on (R.origin=O.id)
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
        origin unix_path,
        path unix_path
    )
    language sql
    stable
as $$
    select C_L.sha1 as content,
           R.sha1 as revision,
           R.date as date,
           O.url as origin,
           '\x'::unix_path as path
    from (
      select C.sha1, unnest(revision) as revision
      from content_in_revision as C_R
      inner join content as C on (C.id=C_R.content)
      where C.sha1=content_id
    ) as C_L
    inner join revision as R on (R.id=C_L.revision)
    left join origin as O on (R.origin=O.id)
    order by date, revision, origin, path asc limit 1
$$;


create or replace function swh_provenance_content_find_all(content_id sha1_git, early_cut int)
    returns table (
        content sha1_git,
        revision sha1_git,
        date timestamptz,
        origin unix_path,
        path unix_path
    )
    language sql
    stable
as $$
   (with cnt as (
      select c.sha1 as sha1,
             unnest(c_r.revision) as revision
      from content_in_revision as c_r
      inner join content as c on (c.id = c_r.content)
      where c.sha1 = content_id
    )
    select cnt.sha1 as content,
           r.sha1 as revision,
           r.date as date,
           O.url as origin,
          '\x'::unix_path as path
    from cnt
    inner join revision as r on (r.id = cnt.revision)
    left join origin as O on (r.origin=O.id)
    )
   union
   (with cnt as (
     select c.sha1 as content_sha1,
            unnest(cd.directory) as directory
     from content as c
     inner join content_in_directory as cd on (cd.content = c.id)
     where c.sha1 = content_id
     ),
    cntdir as (
     select cnt.content_sha1 as content_sha1,
            unnest(dr.revision) as revision
     from cnt
     inner join directory_in_revision as dr on (dr.directory = cnt.directory)
     )
   select cntdir.content_sha1 as content,
          r.sha1 as revision,
          r.date as date,
          O.url as origin,
          '\x'::unix_path as path
   from cntdir
   inner join revision as r on (cntdir.revision = r.id)
   left join origin as O on (r.origin=O.id)
   )
   order by date, revision, origin, path limit early_cut
$$;

\endif
-- :dbflavor_with_path
\endif
-- :dbflavor_norm
