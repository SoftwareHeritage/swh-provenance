import logging
import os
import psycopg2
import threading

from .db_utils import (
    adapt_conn,
    execute_sql
)
from .model import (
    DirectoryEntry,
    FileEntry,
    TreeEntry,
    Tree
)
from .revision import (
    RevisionEntry,
    RevisionIterator,
    ArchiveRevisionIterator,
    FileRevisionIterator
)

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

    dir = os.path.dirname(os.path.realpath(__file__))
    execute_sql(conn, os.path.join(dir, 'db/provenance.sql'))


def revision_add(
    cursor: psycopg2.extensions.cursor,
    storage: StorageInterface,
    revision: RevisionEntry,
    id : int
):
    logging.info(f'Thread {id} - Processing revision {hash_to_hex(revision.swhid)} (timestamp: {revision.timestamp})')
    # Processed content starting from the revision's root directory
    directory = Tree(storage, revision.directory).root
    revision_process_directory(cursor, revision, directory, directory.name)
    # Add current revision to the compact DB
    cursor.execute('INSERT INTO revision VALUES (%s,%s, NULL)', (revision.swhid, revision.timestamp))


def content_find_first(
    cursor: psycopg2.extensions.cursor,
    swhid: str
):
    logging.info(f'Retrieving first occurrence of content {hash_to_hex(swhid)}')
    cursor.execute('''SELECT blob, rev, date, path
                      FROM content_early_in_rev JOIN revision ON revision.id=content_early_in_rev.rev
                      WHERE content_early_in_rev.blob=%s ORDER BY date, rev, path ASC LIMIT 1''', (swhid,))
    return cursor.fetchone()


def content_find_all(
    cursor: psycopg2.extensions.cursor,
    swhid: str
):
    logging.info(f'Retrieving all occurrences of content {hash_to_hex(swhid)}')
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
                      ORDER BY date, rev, path''', (swhid, swhid))
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


def content_get_early_timestamp(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    blob: FileEntry
):
    logging.debug(f'Getting content {hash_to_hex(blob.swhid)} early timestamp')
    if blob.swhid in cache['content'].keys():
        return cache['content'][blob.swhid]
    else:
        cursor.execute('SELECT date FROM content WHERE id=%s',
                        (blob.swhid,))
        row = cursor.fetchone()
        return row[0] if row is not None else None


def content_set_early_timestamp(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    blob: FileEntry,
    timestamp: datetime
):
    logging.debug(f'EARLY occurrence of blob {hash_to_hex(blob.swhid)} (timestamp: {timestamp})')
    # cursor.execute('''INSERT INTO content VALUES (%s,%s)
    #                   ON CONFLICT (id) DO UPDATE SET date=%s''',
    #                   (blob.swhid, timestamp, timestamp))
    cache['content'][blob.swhid] = timestamp


def content_add_to_directory(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    directory: DirectoryEntry,
    blob: FileEntry,
    prefix: PosixPath
):
    logging.debug(f'NEW occurrence of content {hash_to_hex(blob.swhid)} in directory {hash_to_hex(directory.swhid)} (path: {prefix / blob.name})')
    # cursor.execute('INSERT INTO content_in_dir VALUES (%s,%s,%s)',
    #                 (blob.swhid, directory.swhid, bytes(normalize(prefix / blob.name))))
    cache['content_in_dir'].append(
        (blob.swhid, directory.swhid, bytes(normalize(prefix / blob.name)))
    )


def content_add_to_revision(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    revision: RevisionEntry,
    blob: FileEntry,
    prefix: PosixPath
):
    logging.debug(f'EARLY occurrence of blob {hash_to_hex(blob.swhid)} in revision {hash_to_hex(revision.swhid)} (path: {prefix / blob.name})')
    # cursor.execute('INSERT INTO content_early_in_rev VALUES (%s,%s,%s)',
    #                 (blob.swhid, revision.swhid, bytes(normalize(prefix / blob.name))))
    cache['content_early_in_rev'].append(
        (blob.swhid, revision.swhid, bytes(normalize(prefix / blob.name)))
    )


def directory_get_early_timestamp(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    directory: DirectoryEntry
):
    logging.debug(f'Getting directory {hash_to_hex(directory.swhid)} early timestamp')
    if directory.swhid in cache['directory'].keys():
        return cache['directory'][directory.swhid]
    else:
        cursor.execute('SELECT date FROM directory WHERE id=%s',
                        (directory.swhid,))
        row = cursor.fetchone()
        return row[0] if row is not None else None


def directory_set_early_timestamp(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    directory: DirectoryEntry,
    timestamp: datetime
):
    logging.debug(f'EARLY occurrence of directory {hash_to_hex(directory.swhid)} on the ISOCHRONE FRONTIER (timestamp: {timestamp})')
    # cursor.execute('''INSERT INTO directory VALUES (%s,%s)
    #                   ON CONFLICT (id) DO UPDATE SET date=%s''',
    #                   (directory.swhid, timestamp, timestamp))
    cache['directory'][directory.swhid] = timestamp


def directory_add_to_revision(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    revision: RevisionEntry,
    directory: DirectoryEntry,
    path: PosixPath
):
    logging.debug(f'NEW occurrence of directory {hash_to_hex(directory.swhid)} on the ISOCHRONE FRONTIER of revision {hash_to_hex(revision.swhid)} (path: {path})')
    # cursor.execute('INSERT INTO directory_in_rev VALUES (%s,%s,%s)',
    #                 (directory.swhid, revision.swhid, bytes(normalize(path))))
    cache['directory_in_rev'].append(
        (directory.swhid, revision.swhid, bytes(normalize(path)))
    )


def directory_process_content(
    cursor: psycopg2.extensions.cursor,
    cache: dict,
    directory: DirectoryEntry,
    relative: DirectoryEntry,
    prefix: PosixPath
):
    stack = [(directory, relative, prefix)]

    while stack:
        directory, relative, prefix = stack.pop()

        for child in iter(directory):
            if isinstance(child, FileEntry):
                # Add content to the relative directory with the computed prefix.
                content_add_to_directory(cursor, cache, relative, child, prefix)
            else:
                # Recursively walk the child directory.
                # directory_process_content(cursor, child, relative, prefix / child.name)
                stack.append((child, relative, prefix / child.name))


def revision_process_directory(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    directory: DirectoryEntry,
    path: PosixPath
):
    stack = [(revision, directory, path)]
    cache = {
        "content": dict(),
        "content_early_in_rev": list(),
        "content_in_dir": list(),
        "directory": dict(),
        "directory_in_rev": list()
    }

    while stack:
        revision, directory, path = stack.pop()

        timestamp = directory_get_early_timestamp(cursor, cache, directory)

        if timestamp is None:
            # The directory has never been seen on the isochrone graph of a
            # revision. Its children should be checked.
            children = []
            for child in iter(directory):
                if isinstance(child, FileEntry):
                    children.append((child, content_get_early_timestamp(cursor, cache, child)))
                else:
                    children.append((child, directory_get_early_timestamp(cursor, cache, child)))
            timestamps = [x[1] for x in children]
            # timestamps = list(zip(*children))[1]

            if timestamps != [] and None not in timestamps and max(timestamps) <= revision.timestamp:
                # The directory belongs to the isochrone frontier of the current
                # revision, and this is the first time it appears as such.
                directory_set_early_timestamp(cursor, cache, directory, max(timestamps))
                directory_add_to_revision(cursor, cache, revision, directory, path)
                directory_process_content(cursor, cache, directory=directory, relative=directory, prefix=PosixPath('.'))
            else:
                # The directory is not on the isochrone frontier of the current
                # revision. Its child nodes should be analyzed.
                # revision_process_content(cursor, revision, directory, path)
                ################################################################
                for child, timestamp in children:
                    if isinstance(child, FileEntry):
                        if timestamp is None or revision.timestamp < timestamp:
                            content_set_early_timestamp(cursor, cache, child, revision.timestamp)
                        content_add_to_revision(cursor, cache, revision, child, path)
                    else:
                        # revision_process_directory(cursor, revision, child, path / child.name)
                        stack.append((revision, child, path / child.name))
                ################################################################

        elif revision.timestamp < timestamp:
            # The directory has already been seen on the isochrone frontier of a
            # revision, but current revision is earlier. Its children should be
            # updated.
            # revision_process_content(cursor, revision, directory, path)
            ####################################################################
            for child in iter(directory):
                if isinstance(child, FileEntry):
                    timestamp = content_get_early_timestamp(cursor, cache, child)
                    if timestamp is None or revision.timestamp < timestamp:
                        content_set_early_timestamp(cursor, cache, child, revision.timestamp)
                    content_add_to_revision(cursor, cache, revision, child, path)
                else:
                    # revision_process_directory(cursor, revision, child, path / child.name)
                    stack.append((revision, child, path / child.name))
            ####################################################################
            directory_set_early_timestamp(cursor, cache, directory, revision.timestamp)

        else:
            # The directory has already been seen on the isochrone frontier of an
            # earlier revision. Just add it to the current revision.
            directory_add_to_revision(cursor, cache, revision, directory, path)

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


################################################################################
################################################################################
################################################################################


class RevisionWorker(threading.Thread):
    def __init__(
        self,
        id: int,
        conninfo: str,
        storage: StorageInterface,
        revisions: RevisionIterator
    ):
        super().__init__()
        self.id = id
        self.conninfo = conninfo
        self.revisions = revisions
        self.storage = storage


    def run(self):
        conn = db_utils.connect_to_conninfo(self.conninfo)
        adapt_conn(conn)
        with conn.cursor() as cursor:
            while True:
                processed = False
                revision = self.revisions.next()
                if revision is None: break

                while not processed:
                    try:
                        revision_add(cursor, self.storage, revision, self.id)
                        conn.commit()
                        processed = True
                    except psycopg2.DatabaseError:
                        logging.warning(f'Thread {self.id} - Failed to process revision {hash_to_hex(revision.swhid)} (timestamp: {revision.timestamp})')
                        conn.rollback()
                        # TODO: maybe serialize and auto-merge transations.
                        # The only conflicts are on:
                        #   - content: we keep the earliest timestamp
                        #   - directory: we keep the earliest timestamp
                        #   - content_in_dir: there should be just duplicated entries.
                    except Exception as error:
                        logging.warning(f'Exection: {error}')
        conn.close()
