-- SWH DB schema upgrade
-- from_version: 2
-- to_version: 3
-- description: keep unique indices for origins and locations in a hash column


insert into dbversion(version, release, description)
    values(3, now(), 'Work In Progress');

alter table location
    drop constraint if exists location_path_key;

create unique index on location(digest(path, 'sha1'));

alter table origin
    drop constraint if exists origin_url_key;
