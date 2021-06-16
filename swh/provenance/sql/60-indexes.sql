-- psql variables to get the current database flavor
select position('denormalized' in swh_get_dbflavor()::text) = 0 as dbflavor_norm \gset

-- create unique indexes (instead of pkey) because location might be null for
-- the without-path flavor
\if :dbflavor_norm
create unique index on content_in_revision(content, revision, location);
create unique index on directory_in_revision(directory, revision, location);
create unique index on content_in_directory(content, directory, location);
\else
create unique index on content_in_revision(content);
create unique index on directory_in_revision(directory);
create unique index on content_in_directory(content);
\endif


alter table revision_in_origin add primary key (revision, origin);
alter table revision_before_revision add primary key (prev, next);
