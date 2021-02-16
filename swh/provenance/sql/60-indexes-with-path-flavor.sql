alter table content_early_in_rev add primary key (blob, rev, loc);
alter table content_in_dir add primary key (blob, dir, loc);
alter table directory_in_rev add primary key (dir, rev, loc);
