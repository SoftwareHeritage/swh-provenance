-- create unique indexes (instead of pkey) because location might be null for
-- the without-path flavor
create unique index on content_in_revision(content, revision, location);
create unique index on directory_in_revision(directory, revision, location);
create unique index on content_in_directory(content, directory, location);

create unique index on location(digest(path, 'sha1'));
create index on directory(sha1) where flat=false;

alter table revision_in_origin add primary key (revision, origin);
alter table revision_before_revision add primary key (prev, next);
