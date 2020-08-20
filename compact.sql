-- a Git object ID, i.e., a Git-style salted SHA1 checksum
drop domain if exists sha1_git cascade;
create domain sha1_git as bytea check (length(value) = 20);

-- UNIX path (absolute, relative, individual path component, etc.)
drop domain if exists unix_path cascade;
create domain unix_path as bytea;


drop table if exists content;
create table content
(
    blob    sha1_git not null,      -- id of the content blob
    rev     sha1_git not null,      -- id of the revision where the blob appears for the first time
    date    timestamptz not null,   -- timestamp of the revision where the blob appears early
    path    unix_path not null,     -- path to the content relative to the revision root directory
    primary key (blob, rev)
);

comment on column content.blob is 'Content identifier';
comment on column content.rev is 'Revision identifier';
comment on column content.date is 'First seen time';
comment on column content.path is 'Path to content in revision';


drop table if exists directory;
create table directory
(
    id      sha1_git primary key,   -- id of the directory
    date    timestamptz not null    -- timestamp of the revision where the directory appears for the first time
);

comment on column directory.id is 'Directory identifier';
comment on column directory.date is 'First seen time';


drop table if exists revision;
create table revision
(
    id      sha1_git primary key,   -- id of the revision
    date    timestamptz not null    -- timestamp of the revision
);

comment on column revision.id is 'Revision identifier';
comment on column revision.date is 'First seen time';


-- TODO: consider merging this table with 'content'
-- drop table if exists content_early_in_rev;
-- create table content_early_in_rev
-- (
--     blob    sha1_git not null,  -- id of the content blob
--     rev     sha1_git not null,  -- id of the revision where the blob appears for the first time
--     path    unix_path not null, -- path to the content relative to the revision root directory
--     primary key (blob, rev, path)
--     -- foreign key (blob) references content (id),
--     -- foreign key (rev) references revision (id)
-- );

-- comment on column content_early_in_rev.blob is 'Content identifier';
-- comment on column content_early_in_rev.rev is 'Revision identifier';
-- comment on column content_early_in_rev.path is 'Path to content in revision';


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
-- comment on column content_early_in_rev.path is 'Path to content in directory';


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
