-- a Git object ID, i.e., a Git-style salted SHA1 checksum
create domain sha1_git as bytea check (length(value) = 20);

-- UNIX path (absolute, relative, individual path component, etc.)
create domain unix_path as bytea;

create table content
(
    id      bigserial primary key,      -- internal identifier of the content blob
    sha1    sha1_git unique not null,   -- intrinsic identifier of the content blob
    date    timestamptz not null        -- timestamp of the revision where the blob appears early
);
comment on column content.id is 'Content internal identifier';
comment on column content.sha1 is 'Content intrinsic identifier';
comment on column content.date is 'Earliest timestamp for the content (first seen time)';

create table directory
(
    id      bigserial primary key,      -- internal identifier of the directory appearing in an isochrone inner frontier
    sha1    sha1_git unique not null,   -- intrinsic identifier of the directory
    date    timestamptz not null        -- max timestamp among those of the directory children's
);
comment on column directory.id is 'Directory internal identifier';
comment on column directory.sha1 is 'Directory intrinsic identifier';
comment on column directory.date is 'Latest timestamp for the content in the directory';

create table origin
(
    id      bigserial primary key,      -- internal identifier of the origin
    url     unix_path unique not null   -- url of the origin
);
comment on column origin.id is 'Origin internal identifier';
comment on column origin.url is 'URL of the origin';

create table revision
(
    id      bigserial primary key,      -- internal identifier of the revision
    sha1    sha1_git unique not null,   -- intrinsic identifier of the revision
    date    timestamptz not null,       -- timestamp of the revision
    org     bigint                      -- id of the preferred origin
    -- foreign key (org) references origin (id)
);
comment on column revision.id is 'Revision internal identifier';
comment on column revision.sha1 is 'Revision intrinsic identifier';
comment on column revision.date is 'Revision timestamp';
comment on column revision.org is 'preferred origin for the revision';

create table revision_before_rev
(
    prev    bigserial not null,         -- internal identifier of the source revision
    next    bigserial not null,         -- internal identifier of the destination revision
    primary key (prev, next)
    -- foreign key (prev) references revision (id),
    -- foreign key (next) references revision (id)
);
comment on column revision_before_rev.prev is 'Source revision internal identifier';
comment on column revision_before_rev.next is 'Destination revision internal identifier';

create table revision_in_org
(
    rev     bigint not null,            -- internal identifier of the revision poined by the origin
    org     bigint not null,            -- internal identifier of the origin that points to the revision
    primary key (rev, org)
    -- foreign key (rev) references revision (id),
    -- foreign key (org) references origin (id)
);
comment on column revision_in_org.rev is 'Revision internal identifier';
comment on column revision_in_org.org is 'Origin internal identifier';
