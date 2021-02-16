create table content_early_in_rev
(
    blob    bigint not null,            -- internal identifier of the content blob
    rev     bigint not null,            -- internal identifier of the revision where the blob appears for the first time
    loc     bigint not null             -- location of the content relative to the revision root directory
    -- foreign key (blob) references content (id),
    -- foreign key (rev) references revision (id),
    -- foreign key (loc) references location (id)
);
comment on column content_early_in_rev.blob is 'Content internal identifier';
comment on column content_early_in_rev.rev is 'Revision internal identifier';
comment on column content_early_in_rev.loc is 'Location of content in revision';

create table content_in_dir
(
    blob    bigint not null,            -- internal identifier of the content blob
    dir     bigint not null,            -- internal identifier of the directory containing the blob
    loc     bigint not null             -- location of the content relative to its parent directory in the isochrone frontier
    -- foreign key (blob) references content (id),
    -- foreign key (dir) references directory (id),
    -- foreign key (loc) references location (id)
);
comment on column content_in_dir.blob is 'Content internal identifier';
comment on column content_in_dir.dir is 'Directory internal identifier';
comment on column content_in_dir.loc is 'Location of content in directory';

create table directory_in_rev
(
    dir     bigint not null,            -- internal identifier of the directory appearing in the revision
    rev     bigint not null,            -- internal identifier of the revision containing the directory
    loc     bigint not null             -- location of the directory relative to the revision root directory
    -- foreign key (dir) references directory (id),
    -- foreign key (rev) references revision (id),
    -- foreign key (loc) references location (id)
);
comment on column directory_in_rev.dir is 'Directory internal identifier';
comment on column directory_in_rev.rev is 'Revision internal identifier';
comment on column directory_in_rev.loc is 'Location of directory in revision';

create table location
(
    id      bigserial primary key,      -- internal identifier of the location
    path    unix_path unique not null   -- path to the location
);
comment on column location.id is 'Location internal identifier';
comment on column location.path is 'Path to the location';
