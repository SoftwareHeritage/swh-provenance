-- psql variables to get the current database flavor
select swh_get_dbflavor() = 'with-path' as dbflavor_with_path \gset

\if :dbflavor_with_path
alter table content_early_in_rev add primary key (blob, rev, loc);
alter table content_in_dir add primary key (blob, dir, loc);
alter table directory_in_rev add primary key (dir, rev, loc);
\else
alter table content_early_in_rev add primary key (blob, rev);
alter table content_in_dir add primary key (blob, dir);
alter table directory_in_rev add primary key (dir, rev);
\endif
