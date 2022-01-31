-- SWH DB schema upgrade
-- from_version: 2
-- to_version: 3
-- description: keep unique indices for origins and locations in a hash column


insert into dbversion(version, release, description)
    values(3, now(), 'Work In Progress');

drop index if exists location_path_key;
create unique index on location(digest(path::bytea, 'sha1'::text));

drop index if exists origin_url_key;
