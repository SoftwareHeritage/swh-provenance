import logging
import os
import psycopg2

from .db_utils import (
    adapt_conn,
    execute_sql
)
from .model import (
    DirectoryEntry,
    FileEntry,
    Tree
)
from .revision import RevisionEntry

from datetime import datetime
from pathlib import PosixPath

from swh.core.db import db_utils    # TODO: remove this in favour of local db_utils module
from swh.model.hashutil import hash_to_hex
from swh.storage.interface import StorageInterface


def create_database(
    conn: psycopg2.extensions.connection,
    conninfo: str,
    name: str
):
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    # Create new database dropping previous one if exists
    cursor = conn.cursor();
    cursor.execute(f'''DROP DATABASE IF EXISTS {name}''')
    cursor.execute(f'''CREATE DATABASE {name};''');
    conn.close()

    # Reconnect to server selecting newly created database to add tables
    conn = db_utils.connect_to_conninfo(os.path.join(conninfo, name))
    adapt_conn(conn)

    sqldir = os.path.dirname(os.path.realpath(__file__))
    execute_sql(conn, os.path.join(sqldir, 'db/provenance.sql'))


def revision_add(
    conn: psycopg2.extensions.connection,
    storage: StorageInterface,
    revision: RevisionEntry
):
    with conn.cursor() as cursor:
        try:
            # Processed content starting from the revision's root directory
            directory = Tree(storage, revision.root).root
            revision_process_dir(cursor, revision, directory)

            # Add current revision to the compact DB
            cursor.execute('INSERT INTO revision VALUES (%s,%s,NULL)',
                            (revision.id, revision.date))

            # Commit changes (one transaction per revision)
            conn.commit()
            return True

        except psycopg2.DatabaseError:
            # Database error occurred, rollback all changes
            conn.rollback()
            # TODO: maybe serialize and auto-merge transations.
            # The only conflicts are on:
            #   - content: we keep the earliest date
            #   - directory: we keep the earliest date
            #   - content_in_dir: there should be just duplicated entries.
            return False

        except Exception as error:
            # Unexpected error occurred, rollback all changes and log message
            logging.warning(f'Unexpected error: {error}')
            conn.rollback()
            return False


def content_find_first(
    cursor: psycopg2.extensions.cursor,
    blobid: str
):
    logging.info(f'Retrieving first occurrence of content {hash_to_hex(blobid)}')
    cursor.execute('''SELECT blob, rev, date, path
                      FROM content_early_in_rev JOIN revision ON revision.id=content_early_in_rev.rev
                      WHERE content_early_in_rev.blob=%s ORDER BY date, rev, path ASC LIMIT 1''', (blobid,))
    return cursor.fetchone()


def content_find_all(
    cursor: psycopg2.extensions.cursor,
    blobid: str
):
    logging.info(f'Retrieving all occurrences of content {hash_to_hex(blobid)}')
    cursor.execute('''(SELECT blob, rev, date, path
                      FROM content_early_in_rev JOIN revision ON revision.id=content_early_in_rev.rev
                      WHERE content_early_in_rev.blob=%s)
                      UNION
                      (SELECT content_in_rev.blob, content_in_rev.rev, revision.date, content_in_rev.path
                      FROM (SELECT content_in_dir.blob, directory_in_rev.rev,
                                CASE directory_in_rev.path
                                    WHEN '.' THEN content_in_dir.path
                                    ELSE (directory_in_rev.path || '/' || content_in_dir.path)::unix_path
                                END AS path
                            FROM content_in_dir
                            JOIN directory_in_rev ON content_in_dir.dir=directory_in_rev.dir
                            WHERE content_in_dir.blob=%s) AS content_in_rev
                      JOIN revision ON revision.id=content_in_rev.rev)
                      ORDER BY date, rev, path''', (blobid, blobid))
                      # POSTGRESQL EXPLAIN
    yield from cursor.fetchall()


################################################################################
################################################################################
################################################################################

def normalize(path: PosixPath) -> PosixPath:
    spath = str(path)
    if spath.startswith('./'):
        return PosixPath(spath[2:])
    return path


def content_get_early_date(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    blob: FileEntry
) -> datetime:
    logging.debug(f'Getting content {hash_to_hex(blob.id)} early date')
    if blob.id in cache['content'].keys():
        return cache['content'][blob.id]
    else:
        cursor.execute('SELECT date FROM content WHERE id=%s',
                        (blob.id,))
        row = cursor.fetchone()
        return row[0] if row is not None else None


def content_set_early_date(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    blob: FileEntry,
    date: datetime
):
    logging.debug(f'EARLY occurrence of blob {hash_to_hex(blob.id)} (timestamp: {date})')
    # cursor.execute('''INSERT INTO content VALUES (%s,%s)
    #                   ON CONFLICT (id) DO UPDATE SET date=%s''',
    #                   (blob.id, date, date))
    cache['content'][blob.id] = date


def content_add_to_dir(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    directory: DirectoryEntry,
    blob: FileEntry,
    prefix: PosixPath
):
    logging.debug(f'NEW occurrence of content {hash_to_hex(blob.id)} in directory {hash_to_hex(directory.id)} (path: {prefix / blob.name})')
    # cursor.execute('INSERT INTO content_in_dir VALUES (%s,%s,%s)',
    #                 (blob.id, directory.id, bytes(normalize(prefix / blob.name))))
    cache['content_in_dir'].append(
        (blob.id, directory.id, bytes(normalize(prefix / blob.name)))
    )


def content_add_to_rev(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    revision: RevisionEntry,
    blob: FileEntry,
    prefix: PosixPath
):
    logging.debug(f'EARLY occurrence of blob {hash_to_hex(blob.id)} in revision {hash_to_hex(revision.id)} (path: {prefix / blob.name})')
    # cursor.execute('INSERT INTO content_early_in_rev VALUES (%s,%s,%s)',
    #                 (blob.id, revision.id, bytes(normalize(prefix / blob.name))))
    cache['content_early_in_rev'].append(
        (blob.id, revision.id, bytes(normalize(prefix / blob.name)))
    )


def directory_get_early_date(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    directory: DirectoryEntry
) -> datetime:
    logging.debug(f'Getting directory {hash_to_hex(directory.id)} early date')
    if directory.id in cache['directory'].keys():
        return cache['directory'][directory.id]
    else:
        cursor.execute('SELECT date FROM directory WHERE id=%s',
                        (directory.id,))
        row = cursor.fetchone()
        return row[0] if row is not None else None


def directory_set_early_date(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    directory: DirectoryEntry,
    date: datetime
):
    logging.debug(f'EARLY occurrence of directory {hash_to_hex(directory.id)} on the ISOCHRONE FRONTIER (timestamp: {date})')
    # cursor.execute('''INSERT INTO directory VALUES (%s,%s)
    #                   ON CONFLICT (id) DO UPDATE SET date=%s''',
    #                   (directory.id, date, date))
    cache['directory'][directory.id] = date


def directory_add_to_rev(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    revision: RevisionEntry,
    directory: DirectoryEntry,
    path: PosixPath
):
    logging.debug(f'NEW occurrence of directory {hash_to_hex(directory.id)} on the ISOCHRONE FRONTIER of revision {hash_to_hex(revision.id)} (path: {path})')
    # cursor.execute('INSERT INTO directory_in_rev VALUES (%s,%s,%s)',
    #                 (directory.id, revision.id, bytes(normalize(path))))
    cache['directory_in_rev'].append(
        (directory.id, revision.id, bytes(normalize(path)))
    )


def directory_process_content(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    directory: DirectoryEntry,
    relative: DirectoryEntry,
    prefix: PosixPath
):
    stack = [(directory, prefix)]

    while stack:
        dir, path = stack.pop()

        for child in iter(dir):
            if isinstance(child, FileEntry):
                # Add content to the relative directory with the computed path.
                content_add_to_dir(cursor, cache, relative, child, path)
            else:
                # Recursively walk the child directory.
                # directory_process_content(cursor, child, relative, path / child.name)
                stack.append((child, path / child.name))


def revision_process_dir(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    directory: DirectoryEntry
):
    stack = [(directory, directory.name)]
    cache = {
        "content": dict(),
        "content_early_in_rev": list(),
        "content_in_dir": list(),
        "directory": dict(),
        "directory_in_rev": list()
    }

    while stack:
        dir, path = stack.pop()

        date = directory_get_early_date(cursor, cache, dir)

        if date is None:
            # The directory has never been seen on the isochrone graph of a
            # revision. Its children should be checked.
            children = []
            for child in iter(dir):
                if isinstance(child, FileEntry):
                    children.append((child, content_get_early_date(cursor, cache, child)))
                else:
                    children.append((child, directory_get_early_date(cursor, cache, child)))
            dates = [child[1] for child in children]

            if dates != [] and None not in dates and max(dates) <= revision.date:
                # The directory belongs to the isochrone frontier of the current
                # revision, and this is the first time it appears as such.
                directory_set_early_date(cursor, cache, dir, max(dates))
                directory_add_to_rev(cursor, cache, revision, dir, path)
                directory_process_content(cursor, cache, directory=dir, relative=dir, prefix=PosixPath('.'))
            else:
                # The directory is not on the isochrone frontier of the current
                # revision. Its child nodes should be analyzed.
                # revision_process_content(cursor, revision, dir, path)
                ################################################################
                for child, date in children:
                    if isinstance(child, FileEntry):
                        if date is None or revision.date < date:
                            content_set_early_date(cursor, cache, child, revision.date)
                        content_add_to_rev(cursor, cache, revision, child, path)
                    else:
                        # revision_process_dir(cursor, revision, child, path / child.name)
                        stack.append((child, path / child.name))
                ################################################################

        elif revision.date < date:
            # The directory has already been seen on the isochrone frontier of a
            # revision, but current revision is earlier. Its children should be
            # updated.
            # revision_process_content(cursor, revision, dir, path)
            ####################################################################
            for child in iter(dir):
                if isinstance(child, FileEntry):
                    date = content_get_early_date(cursor, cache, child)
                    if date is None or revision.date < date:
                        content_set_early_date(cursor, cache, child, revision.date)
                    content_add_to_rev(cursor, cache, revision, child, path)
                else:
                    # revision_process_dir(cursor, revision, child, path / child.name)
                    stack.append((child, path / child.name))
            ####################################################################
            directory_set_early_date(cursor, cache, dir, revision.date)

        else:
            # The directory has already been seen on the isochrone frontier of an
            # earlier revision. Just add it to the current revision.
            directory_add_to_rev(cursor, cache, revision, dir, path)

    perform_insertions(cursor, cache)


def perform_insertions(
    cursor: psycopg2.extensions.cursor,
    cache: dict
):
    psycopg2.extras.execute_values(
        cursor,
        '''INSERT INTO content(id, date) VALUES %s
           ON CONFLICT (id) DO UPDATE SET date=excluded.date''',
        cache['content'].items()
    )

    psycopg2.extras.execute_values(
        cursor,
        '''INSERT INTO content_early_in_rev VALUES %s''',
        cache['content_early_in_rev']
    )

    psycopg2.extras.execute_values(
        cursor,
        '''INSERT INTO content_in_dir VALUES %s''',
        cache['content_in_dir']
    )

    psycopg2.extras.execute_values(
        cursor,
        '''INSERT INTO directory(id, date) VALUES %s
           ON CONFLICT (id) DO UPDATE SET date=excluded.date''',
        cache['directory'].items()
    )

    psycopg2.extras.execute_values(
        cursor,
        '''INSERT INTO directory_in_rev VALUES %s''',
        cache['directory_in_rev']
    )
