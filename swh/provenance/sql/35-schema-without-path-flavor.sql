create table content_early_in_rev
(
    blob    bigint not null,            -- internal identifier of the content blob
    rev     bigint not null             -- internal identifier of the revision where the blob appears for the first time
    -- foreign key (blob) references content (id),
    -- foreign key (rev) references revision (id)
);
comment on column content_early_in_rev.blob is 'Content internal identifier';
comment on column content_early_in_rev.rev is 'Revision internal identifier';

create table content_in_dir
(
    blob    bigint not null,            -- internal identifier of the content blob
    dir     bigint not null             -- internal identifier of the directory containing the blob
    -- foreign key (blob) references content (id),
    -- foreign key (dir) references directory (id)
);
comment on column content_in_dir.blob is 'Content internal identifier';
comment on column content_in_dir.dir is 'Directory internal identifier';

create table directory_in_rev
(
    dir     bigint not null,            -- internal identifier of the directory appearing in the revision
    rev     bigint not null             -- internal identifier of the revision containing the directory
    -- foreign key (dir) references directory (id),
    -- foreign key (rev) references revision (id)
);
comment on column directory_in_rev.dir is 'Directory internal identifier';
comment on column directory_in_rev.rev is 'Revision internal identifier';
