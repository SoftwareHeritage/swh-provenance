-- SWH DB schema upgrade
-- from_version: 1
-- to_version: 2
-- description: add flag to acknowledge directories' flattening


insert into dbversion(version, release, description)
    values(2, now(), 'Work In Progress');

alter table content
    alter column date set not null;

alter table directory
    add column flat boolean not null default false;

alter table directory
    alter column date set not null;
