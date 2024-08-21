create domain unix_path as bytea;
create domain sha1_git as bytea check (length(value) = 20);
create domain sha1 as bytea check (length(value) = 20);

create table directory_in_revision
(
        directory sha1_git not null,
        max_author_date timestamp not null,
        revision  sha1_git not null,
        author_date     timestamp not null,
        path      unix_path
);

create table directory_in_release
(
        directory sha1_git not null,
        max_author_date timestamp not null,
        release   sha1_git not null,
        author_date     timestamp not null,
        path      unix_path
);

create table content_in_directory
(
        content   sha1_git not null,
        directory sha1_git not null,
        path      unix_path
);

create table content_in_revision
(
        content   sha1_git not null,
        revision  sha1_git not null,
        date      timestamp not null,
        path      unix_path
);

create table content_in_release
(
        content   sha1_git not null,
        release   sha1_git not null,
        date      timestamp not null,
        path      unix_path
);

create table content
(
  sha1       sha1 not null,
  sha1_git   sha1_git not null
);
