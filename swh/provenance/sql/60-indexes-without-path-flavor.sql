alter table content_early_in_rev add primary key (blob, rev);
alter table content_in_dir add primary key (blob, dir);
alter table directory_in_rev add primary key (dir, rev);
