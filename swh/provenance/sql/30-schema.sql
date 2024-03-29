-- a Git object ID, i.e., a Git-style salted SHA1 checksum
create domain sha1_git as bytea check (length(value) = 20);

-- UNIX path (absolute, relative, individual path component, etc.)
create domain unix_path as bytea;

-- relation filter options for querying
create type rel_flt as enum (
  'filter-src',
  'filter-dst',
  'no-filter'
);
comment on type rel_flt is 'Relation get filter types';

-- entity tables
create table content
(
    id      bigserial primary key,          -- internal identifier of the content blob
    sha1    sha1_git unique not null,       -- intrinsic identifier of the content blob
    date    timestamptz                     -- timestamp of the revision where the blob appears early
);
comment on column content.id is 'Content internal identifier';
comment on column content.sha1 is 'Content intrinsic identifier';
comment on column content.date is 'Earliest timestamp for the content (first seen time)';

create table directory
(
    id      bigserial primary key,          -- internal identifier of the directory appearing in an isochrone inner frontier
    sha1    sha1_git unique not null,       -- intrinsic identifier of the directory
    date    timestamptz,                    -- max timestamp among those of the directory children's
    flat    boolean not null default false  -- flag acknowledging if the directory is flattenned in the model
);
comment on column directory.id is 'Directory internal identifier';
comment on column directory.sha1 is 'Directory intrinsic identifier';
comment on column directory.date is 'Latest timestamp for the content in the directory';

create table revision
(
    id      bigserial primary key,          -- internal identifier of the revision
    sha1    sha1_git unique not null,       -- intrinsic identifier of the revision
    date    timestamptz,                    -- timestamp of the revision
    origin  bigint                          -- id of the preferred origin
    -- foreign key (origin) references origin (id)
);
comment on column revision.id is 'Revision internal identifier';
comment on column revision.sha1 is 'Revision intrinsic identifier';
comment on column revision.date is 'Revision timestamp';
comment on column revision.origin is 'preferred origin for the revision';

create table location
(
    id      bigserial primary key,          -- internal identifier of the location
    path    unix_path                       -- path to the location
);
comment on column location.id is 'Location internal identifier';
comment on column location.path is 'Path to the location';

create table origin
(
    id      bigserial primary key,          -- internal identifier of the origin
    sha1    sha1_git unique not null,       -- intrinsic identifier of the origin
    url     text                            -- url of the origin
);
comment on column origin.id is 'Origin internal identifier';
comment on column origin.sha1 is 'Origin intrinsic identifier';
comment on column origin.url is 'URL of the origin';

-- relation tables
create table content_in_revision
(
    content  bigint not null,               -- internal identifier of the content blob
    revision bigint not null,               -- internal identifier of the revision where the blob appears for the first time
    location bigint,                        -- location of the content relative to the revision's root directory
    revision_date timestamptz not null      -- date of the revision where the blob appears for the first time
    -- foreign key (content) references content (id),
    -- foreign key (revision) references revision (id),
    -- foreign key (location) references location (id)
);
comment on column content_in_revision.content is 'Content internal identifier';
comment on column content_in_revision.revision is 'Revision internal identifier';
comment on column content_in_revision.location is 'Location of content in revision';
comment on column content_in_revision.revision_date is 'Date of the revision';

create table content_in_directory
(
    content   bigint not null,              -- internal identifier of the content blob
    directory bigint not null,              -- internal identifier of the directory containing the blob
    location  bigint                        -- location of the content relative to its parent directory in the isochrone frontier
    -- foreign key (content) references content (id),
    -- foreign key (directory) references directory (id),
    -- foreign key (location) references location (id)
);
comment on column content_in_directory.content is 'Content internal identifier';
comment on column content_in_directory.directory is 'Directory internal identifier';
comment on column content_in_directory.location is 'Location of content in directory';

create table directory_in_revision
(
    directory bigint not null,              -- internal identifier of the directory appearing in the revision
    revision  bigint not null,              -- internal identifier of the revision containing the directory
    location  bigint                        -- location of the directory relative to the revision's root directory
    -- foreign key (directory) references directory (id),
    -- foreign key (revision) references revision (id),
    -- foreign key (location) references location (id)
);
comment on column directory_in_revision.directory is 'Directory internal identifier';
comment on column directory_in_revision.revision is 'Revision internal identifier';
comment on column directory_in_revision.location is 'Location of content in revision';

create table revision_in_origin
(
    revision bigint not null,               -- internal identifier of the revision poined by the origin
    origin   bigint not null                -- internal identifier of the origin that points to the revision
    -- foreign key (revision) references revision (id),
    -- foreign key (origin) references origin (id)
);
comment on column revision_in_origin.revision is 'Revision internal identifier';
comment on column revision_in_origin.origin is 'Origin internal identifier';

create table revision_before_revision
(
    prev    bigserial not null,             -- internal identifier of the source revision
    next    bigserial not null              -- internal identifier of the destination revision
    -- foreign key (prev) references revision (id),
    -- foreign key (next) references revision (id)
);
comment on column revision_before_revision.prev is 'Source revision internal identifier';
comment on column revision_before_revision.next is 'Destination revision internal identifier';
