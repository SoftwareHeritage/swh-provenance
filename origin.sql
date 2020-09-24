-- -- a Git object ID, i.e., a Git-style salted SHA1 checksum
-- drop domain if exists sha1_git cascade;
-- create domain sha1_git as bytea check (length(value) = 20);


drop table if exists origin;
create table origin
(
    id      bigserial primary key,  -- id of the origin
    url     unix_path unique        -- url of the origin
);

comment on column origin.id is 'Origin internal identifier';
comment on column origin.url is 'URL of the origin';


drop table if exists revision_in_org;
create table revision_in_org
(
    rev     sha1_git not null,      -- id of the revision poined by the origin
    org     bigint not null,        -- id of the origin that points to the revision
    primary key (rev, org)
);

comment on column revision_in_org.rev is 'Revision identifier';
comment on column revision_in_org.org is 'Origin identifier';


drop table if exists revision_before_rev;
create table revision_before_rev
(
    prev    sha1_git not null,      -- id of the source revision
    next    sha1_git not null,      -- id of the destination revision
    primary key (prev, next)
);

comment on column revision_before_rev.prev is 'Source revision identifier';
comment on column revision_before_rev.next is 'Destination revision identifier';
