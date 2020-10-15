import logging
import os
import psycopg2

from .db_utils import adapt_conn, execute_sql
from .model import DirectoryEntry, FileEntry, Tree
from .origin import OriginEntry
from .revision import RevisionEntry

from datetime import datetime
from pathlib import PosixPath

from swh.core.db import db_utils    # TODO: remove this in favour of local db_utils module
from swh.model.hashutil import hash_to_hex
from swh.storage.interface import StorageInterface


def normalize(path: PosixPath) -> PosixPath:
    spath = str(path)
    if spath.startswith('./'):
        return PosixPath(spath[2:])
    return path


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


################################################################################
################################################################################
################################################################################

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


def origin_add(
    conn: psycopg2.extensions.connection,
    storage: StorageInterface,
    origin: OriginEntry
):
    with conn.cursor() as cursor:
        origin.id = origin_get_id(cursor, origin)

        for revision in origin.revisions:
            logging.info(f'Processing revision {hash_to_hex(revision.id)} from origin {origin.url}')
            origin_add_revision(cursor, storage, origin, revision)

            # Commit after each revision
            conn.commit()


def origin_add_revision(
    cursor: psycopg2.extensions.cursor,
    storage: StorageInterface,
    origin: OriginEntry,
    revision: RevisionEntry
):
    stack = [(None, revision)]

    while stack:
        relative, rev = stack.pop()

        # Check if current revision has no prefered origin and update if necessary.
        prefered = revision_get_prefered_org(cursor, rev)
        logging.debug(f'Prefered origin for revision {hash_to_hex(rev.id)}: {prefered}')

        if prefered is None:
            revision_set_prefered_org(cursor, origin, rev)
        ########################################################################

        if relative is None:
            # This revision is pointed directly by the origin.
            visited = revision_visited(cursor, rev)
            logging.debug(f'Revision {hash_to_hex(rev.id)} in origin {origin.id}: {visited}')

            logging.debug(f'Adding revision {hash_to_hex(rev.id)} to origin {origin.id}')
            revision_add_to_org(cursor, origin, rev)

            if not visited:
                # revision_walk_history(cursor, origin, rev.id, rev)
                stack.append((rev, rev))

        else:
            # This revision is a parent of another one in the history of the
            # relative revision.
            for parent in iter(rev):
                visited = revision_visited(cursor, parent)
                logging.debug(f'Parent {hash_to_hex(parent.id)} in some origin: {visited}')

                if not visited:
                    # The parent revision has never been seen before pointing
                    # directly to an origin.
                    known = revision_in_history(cursor, parent)
                    logging.debug(f'Revision {hash_to_hex(parent.id)} before revision: {known}')

                    if known:
                        # The parent revision is already known in some other
                        # revision's history. We should point it directly to
                        # the origin and (eventually) walk its history.
                        logging.debug(f'Adding revision {hash_to_hex(parent.id)} directly to origin {origin.id}')
                        # origin_add_revision(cursor, origin, parent)
                        stack.append((None, parent))
                    else:
                        # The parent revision was never seen before. We should
                        # walk its history and associate it with the same
                        # relative revision.
                        logging.debug(f'Adding parent revision {hash_to_hex(parent.id)} to revision {hash_to_hex(relative.id)}')
                        revision_add_before_rev(cursor, relative, parent)
                        # revision_walk_history(cursor, origin, relative, parent)
                        stack.append((relative, parent))
                else:
                    # The parent revision already points to an origin, so its
                    # history was properly processed before. We just need to
                    # make sure it points to the current origin as well.
                    logging.debug(f'Adding parent revision {hash_to_hex(parent.id)} to origin {origin.id}')
                    revision_add_to_org(cursor, origin, parent)


def origin_get_id(
    cursor: psycopg2.extensions.cursor,
    origin: OriginEntry
) -> int:
    if origin.id is None:
        # Check if current origin is already known and retrieve its internal id.
        cursor.execute('''SELECT id FROM origin WHERE url=%s''', (origin.url,))
        row = cursor.fetchone()

        if row is None:
            # If the origin is seen for the first time, current revision is
            # the prefered one.
            cursor.execute('''INSERT INTO origin (url) VALUES (%s) RETURNING id''',
                              (origin.url,))
            return cursor.fetchone()[0]
        else:
            return row[0]
    else:
        return origin.id


def revision_add(
    conn: psycopg2.extensions.connection,
    storage: StorageInterface,
    revision: RevisionEntry
):
    try:
        with conn.cursor() as cursor:
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


def revision_add_before_rev(
    cursor: psycopg2.extensions.cursor,
    relative: RevisionEntry,
    revision: RevisionEntry
):
    cursor.execute('''INSERT INTO revision_before_rev VALUES (%s,%s)''',
                      (revision.id, relative.id))


def revision_add_to_org(
    cursor: psycopg2.extensions.cursor,
    origin: OriginEntry,
    revision: RevisionEntry
):
    cursor.execute('''INSERT INTO revision_in_org VALUES (%s,%s)
                      ON CONFLICT DO NOTHING''',
                      (revision.id, origin.id))


def revision_get_prefered_org(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry
) -> int:
    cursor.execute('''SELECT COALESCE(org,0) FROM revision WHERE id=%s''',
                      (revision.id,))
    row = cursor.fetchone()
    # None means revision is not in database
    # 0 means revision has no prefered origin
    return row[0] if row is not None and row[0] != 0 else None


def revision_in_history(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry
) -> bool:
    cursor.execute('''SELECT 1 FROM revision_before_rev WHERE prev=%s''',
                      (revision.id,))
    return cursor.fetchone() is not None


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

    # Performe insertions with cached information
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


def revision_set_prefered_org(
    cursor: psycopg2.extensions.cursor,
    origin: OriginEntry,
    revision: RevisionEntry
):
    cursor.execute('''UPDATE revision SET org=%s WHERE id=%s''',
                     (origin.id, revision.id))


def revision_visited(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry
) -> bool:
    cursor.execute('''SELECT 1 FROM revision_in_org WHERE rev=%s''',
                      (revision.id,))
    return cursor.fetchone() is not None
