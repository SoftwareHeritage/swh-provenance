-- a Git object ID, i.e., a Git-style salted SHA1 checksum
drop domain if exists sha1_git cascade;
create domain sha1_git as bytea check (length(value) = 20);

-- UNIX path (absolute, relative, individual path component, etc.)
drop domain if exists unix_path cascade;
create domain unix_path as bytea;


drop table if exists content;
create table content
(
    id      sha1_git primary key,   -- id of the content blob
    date    timestamptz not null    -- timestamp of the revision where the blob appears early
);

comment on column content.id is 'Content identifier';
comment on column content.date is 'Earliest timestamp for the content (first seen time)';


drop table if exists content_early_in_rev;
create table content_early_in_rev
(
    blob    sha1_git not null,  -- id of the content blob
    rev     sha1_git not null,  -- id of the revision where the blob appears for the first time
    path    unix_path not null, -- path to the content relative to the revision root directory
    primary key (blob, rev, path)
    -- foreign key (blob) references content (id),
    -- foreign key (rev) references revision (id)
);

comment on column content_early_in_rev.blob is 'Content identifier';
comment on column content_early_in_rev.rev is 'Revision identifier';
comment on column content_early_in_rev.path is 'Path to content in revision';


drop table if exists content_in_dir;
create table content_in_dir
(
    blob    sha1_git not null,  -- id of the content blob
    dir     sha1_git not null,  -- id of the directory contaning the blob
    path    unix_path not null, -- path name relative to its parent on the isochrone frontier
    primary key (blob, dir, path)
    -- foreign key (blob) references content (id),
    -- foreign key (dir) references directory (id)
);

comment on column content_in_dir.blob is 'Content identifier';
comment on column content_in_dir.dir is 'Directory identifier';
comment on column content_in_dir.path is 'Path to content in directory';


drop table if exists directory;
create table directory
(
    id      sha1_git primary key,   -- id of the directory appearing in an isochrone inner frontier
    date    timestamptz not null    -- max timestamp among those of the directory children's
);

comment on column directory.id is 'Directory identifier';
comment on column directory.date is 'Latest timestamp for the content in the directory';


drop table if exists directory_in_rev;
create table directory_in_rev
(
    dir     sha1_git not null,  -- id of the directory appearing in the revision
    rev     sha1_git not null,  -- id of the revision containing the directory
    path    unix_path not null, -- path to the directory relative to the revision root directory
    primary key (dir, rev, path)
    -- foreign key (dir) references directory (id),
    -- foreign key (rev) references revision (id)
);

comment on column directory_in_rev.dir is 'Directory identifier';
comment on column directory_in_rev.rev is 'Revision identifier';
comment on column directory_in_rev.path is 'Path to directory in revision';

drop table if exists origin;
create table origin
(
    id      bigserial primary key,  -- id of the origin
    url     unix_path unique        -- url of the origin
);

comment on column origin.id is 'Origin internal identifier';
comment on column origin.url is 'URL of the origin';


drop table if exists revision;
create table revision
(
    id      sha1_git primary key,   -- id of the revision
    date    timestamptz not null,   -- timestamp of the revision
    org     bigint                  -- id of the prefered origin
);

comment on column revision.id is 'Revision identifier';
comment on column revision.date is 'Revision timestamp';
comment on column revision.org is 'Prefered origin for the revision';


drop table if exists revision_before_rev;
create table revision_before_rev
(
    prev    sha1_git not null,      -- id of the source revision
    next    sha1_git not null,      -- id of the destination revision
    primary key (prev, next)
);

comment on column revision_before_rev.prev is 'Source revision identifier';
comment on column revision_before_rev.next is 'Destination revision identifier';


drop table if exists revision_in_org;
create table revision_in_org
(
    rev     sha1_git not null,      -- id of the revision poined by the origin
    org     bigint not null,        -- id of the origin that points to the revision
    primary key (rev, org)
);

comment on column revision_in_org.rev is 'Revision identifier';
comment on column revision_in_org.org is 'Origin identifier';
