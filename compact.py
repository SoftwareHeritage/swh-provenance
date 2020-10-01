import click
import logging
import psycopg2
import swh.storage
import sys
import time
import threading
import utils

from datetime import datetime
from pathlib import PosixPath

from iterator import (
    RevisionEntry,
    RevisionIterator,
    ArchiveRevisionIterator,
    FileRevisionIterator
)
from model import (
    DirectoryEntry,
    FileEntry,
    TreeEntry,
    Tree
)

from swh.model.identifiers import identifier_to_str


def revision_add(
    cursor: psycopg2.extensions.cursor,
    storage: swh.storage.interface.StorageInterface,
    revision: RevisionEntry,
    id : int
):
    logging.info(f'Thread {id} - Processing revision {identifier_to_str(revision.swhid)} (timestamp: {revision.timestamp})')
    # Processed content starting from the revision's root directory
    directory = Tree(storage, revision.directory).root
    revision_process_directory(cursor, revision, directory, directory.name)
    # Add current revision to the compact DB
    cursor.execute('INSERT INTO revision VALUES (%s,%s, NULL)', (revision.swhid, revision.timestamp))


def content_find_first(
    cursor: psycopg2.extensions.cursor,
    swhid: str
):
    logging.info(f'Retrieving first occurrence of content {identifier_to_str(swhid)}')
    cursor.execute('''SELECT blob, rev, date, path
                      FROM content_early_in_rev JOIN revision ON revision.id=content_early_in_rev.rev
                      WHERE content_early_in_rev.blob=%s ORDER BY date, rev, path ASC LIMIT 1''', (swhid,))
    return cursor.fetchone()


def content_find_all(
    cursor: psycopg2.extensions.cursor,
    swhid: str
):
    logging.info(f'Retrieving all occurrences of content {identifier_to_str(swhid)}')
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
    blob: FileEntry
):
    logging.debug(f'Getting content {identifier_to_str(blob.swhid)} early timestamp')
    start = time.perf_counter_ns()
    cursor.execute('SELECT date FROM content WHERE id=%s', (blob.swhid,))
    row = cursor.fetchone()
    stop = time.perf_counter_ns()
    logging.debug(f'  Time elapsed: {stop-start}ns')
    return row[0] if row is not None else None


def content_set_early_timestamp(
    cursor: psycopg2.extensions.cursor,
    blob: FileEntry,
    timestamp: datetime
):
    logging.debug(f'EARLY occurrence of blob {identifier_to_str(blob.swhid)} (timestamp: {timestamp})')
    start = time.perf_counter_ns()
    cursor.execute('''INSERT INTO content VALUES (%s,%s)
                      ON CONFLICT (id) DO UPDATE SET date=%s''',
                      (blob.swhid, timestamp, timestamp))
    stop = time.perf_counter_ns()
    logging.debug(f'  Time elapsed: {stop-start}ns')


def content_add_to_directory(
    cursor: psycopg2.extensions.cursor,
    directory: DirectoryEntry,
    blob: FileEntry,
    prefix: PosixPath
):
    logging.debug(f'NEW occurrence of content {identifier_to_str(blob.swhid)} in directory {identifier_to_str(directory.swhid)} (path: {prefix / blob.name})')
    start = time.perf_counter_ns()
    cursor.execute('INSERT INTO content_in_dir VALUES (%s,%s,%s)',
                    (blob.swhid, directory.swhid, bytes(normalize(prefix / blob.name))))
    stop = time.perf_counter_ns()
    logging.debug(f'  Time elapsed: {stop-start}ns')


def content_add_to_revision(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    blob: FileEntry,
    prefix: PosixPath
):
    logging.debug(f'EARLY occurrence of blob {identifier_to_str(blob.swhid)} in revision {identifier_to_str(revision.swhid)} (path: {prefix / blob.name})')
    start = time.perf_counter_ns()
    cursor.execute('INSERT INTO content_early_in_rev VALUES (%s,%s,%s)',
                    (blob.swhid, revision.swhid, bytes(normalize(prefix / blob.name))))
    stop = time.perf_counter_ns()
    logging.debug(f'  Time elapsed: {stop-start}ns')


def directory_get_early_timestamp(
    cursor: psycopg2.extensions.cursor,
    directory: DirectoryEntry
):
    logging.debug(f'Getting directory {identifier_to_str(directory.swhid)} early timestamp')
    start = time.perf_counter_ns()
    cursor.execute('SELECT date FROM directory WHERE id=%s', (directory.swhid,))
    row = cursor.fetchone()
    stop = time.perf_counter_ns()
    logging.debug(f'  Time elapsed: {stop-start}ns')
    return row[0] if row is not None else None


def directory_set_early_timestamp(
    cursor: psycopg2.extensions.cursor,
    directory: DirectoryEntry,
    timestamp: datetime
):
    logging.debug(f'EARLY occurrence of directory {identifier_to_str(directory.swhid)} on the ISOCHRONE FRONTIER (timestamp: {timestamp})')
    start = time.perf_counter_ns()
    cursor.execute('''INSERT INTO directory VALUES (%s,%s)
                      ON CONFLICT (id) DO UPDATE SET date=%s''',
                      (directory.swhid, timestamp, timestamp))
    stop = time.perf_counter_ns()
    logging.debug(f'  Time elapsed: {stop-start}ns')


def directory_add_to_revision(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    directory: DirectoryEntry,
    path: PosixPath
):
    logging.debug(f'NEW occurrence of directory {identifier_to_str(directory.swhid)} on the ISOCHRONE FRONTIER of revision {identifier_to_str(revision.swhid)} (path: {path})')
    start = time.perf_counter_ns()
    cursor.execute('INSERT INTO directory_in_rev VALUES (%s,%s,%s)',
                    (directory.swhid, revision.swhid, bytes(normalize(path))))
    stop = time.perf_counter_ns()
    logging.debug(f'  Time elapsed: {stop-start}ns')


def directory_process_content(
    cursor: psycopg2.extensions.cursor,
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
                content_add_to_directory(cursor, relative, child, prefix)
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

    while stack:
        revision, directory, path = stack.pop()

        timestamp = directory_get_early_timestamp(cursor, directory)
        logging.debug(timestamp)

        if timestamp is None:
            # The directory has never been seen on the isochrone graph of a
            # revision. Its children should be checked.
            timestamps = []
            for child in iter(directory):
                logging.debug(f'child {child}')
                if isinstance(child, FileEntry):
                    timestamps.append(content_get_early_timestamp(cursor, child))
                else:
                    timestamps.append(directory_get_early_timestamp(cursor, child))
            logging.debug(timestamps)

            if timestamps != [] and None not in timestamps and max(timestamps) <= revision.timestamp:
                # The directory belongs to the isochrone frontier of the current
                # revision, and this is the first time it appears as such.
                directory_set_early_timestamp(cursor, directory, max(timestamps))
                directory_add_to_revision(cursor, revision, directory, path)
                directory_process_content(cursor, directory, directory, PosixPath('.'))
            else:
                # The directory is not on the isochrone frontier of the current
                # revision. Its child nodes should be analyzed.
                # revision_process_content(cursor, revision, directory, path)
                ################################################################
                for child in iter(directory):
                    if isinstance(child, FileEntry):
                        timestamp = content_get_early_timestamp(cursor, child)
                        if timestamp is None or revision.timestamp < timestamp:
                            content_set_early_timestamp(cursor, child, revision.timestamp)
                        content_add_to_revision(cursor, revision, child, path)
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
                    timestamp = content_get_early_timestamp(cursor, child)
                    if timestamp is None or revision.timestamp < timestamp:
                        content_set_early_timestamp(cursor, child, revision.timestamp)
                    content_add_to_revision(cursor, revision, child, path)
                else:
                    # revision_process_directory(cursor, revision, child, path / child.name)
                    stack.append((revision, child, path / child.name))
            ####################################################################
            directory_set_early_timestamp(cursor, directory, revision.timestamp)

        else:
            # The directory has already been seen on the isochrone frontier of an
            # earlier revision. Just add it to the current revision.
            directory_add_to_revision(cursor, revision, directory, path)


################################################################################
################################################################################
################################################################################


class Worker(threading.Thread):
    def __init__(
        self,
        id : int,
        conf : str,
        database : str,
        storage : swh.storage.interface.StorageInterface,
        revisions : RevisionIterator
    ):
        super().__init__()
        self.id = id
        self.conf = conf
        self.database = database
        self.revisions = revisions
        self.storage = storage


    def run(self):
        conn = utils.connect(self.conf, self.database)
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
                        logging.warning(f'Thread {self.id} - Failed to process revision {identifier_to_str(revision.swhid)} (timestamp: {revision.timestamp})')
                        conn.rollback()
                    except Exception as error:
                        logging.warning(f'Exection: {error}')
        conn.close()


@click.command()
@click.argument('count', type=int)
@click.option('-c', '--compact', nargs=2, required=True)
@click.option('-a', '--archive', nargs=2)
@click.option('-d', '--database', nargs=2)
@click.option('-f', '--filename')
@click.option('-l', '--limit', type=int)
@click.option('-t', '--threads', type=int, default=1)
def cli(count, compact, archive, database, filename, limit, threads):
    """Compact model revision-content layer utility."""
    logging.basicConfig(level=logging.INFO)
    # logging.basicConfig(filename='compact.log', level=logging.DEBUG)

    click.echo(f'{count} {compact} {archive} {database} {filename} {limit}')
    if not database: database = None
    if not archive: archive = None

    reset = database is not None or filename is not None
    if reset and archive is None:
        logging.error('Error: -a option is compulsatory when -d or -f options are set')
        exit()

    # TODO: improve the way a connection to PSQL is configured.
    comp_conn = utils.connect(compact[0], compact[1])
    cursor = comp_conn.cursor()

    if reset:
        utils.execute_sql(comp_conn, 'compact.sql')   # Create tables dopping existing ones

        if database is not None:
            logging.info(f'Reconstructing compact model from {database} database (limit={limit})')
            data_conn = utils.connect(database[0], database[1])
            revisions = ArchiveRevisionIterator(data_conn, limit=limit)
        else:
            logging.info(f'Reconstructing compact model from {filename} CSV file (limit={limit})')
            revisions = FileRevisionIterator(filename, limit=limit)

        # TODO: take this arguments from the command line.
        kwargs = {
            "cls" : "remote",
            "url" : "http://uffizi.internal.softwareheritage.org:5002"
        }
        storage = swh.storage.get_storage(**kwargs)

        workers = []
        for id in range(threads):
            worker = Worker(id, compact[0], compact[1], storage, revisions)
            worker.start()
            workers.append(worker)

        for worker in workers:
            worker.join()

        if database is not None:
            data_conn.close()

    cursor.execute(f'SELECT DISTINCT id FROM content ORDER BY id LIMIT {count}')
    for idx, row in enumerate(cursor.fetchall()):
        swhid = row[0]
        print(f'Test blob {idx}: {identifier_to_str(swhid)}')

        fst = content_find_first(cursor, swhid)
        print(f'  First occurrence:')
        print(f'    {identifier_to_str(fst[0])}, {identifier_to_str(fst[1])}, {fst[2]}, {fst[3].decode("utf-8")}')

        print(f'  All occurrences:')
        for row in content_find_all(cursor, swhid):
            print(f'    {identifier_to_str(row[0])}, {identifier_to_str(row[1])}, {row[2]}, {row[3].decode("utf-8")}')

        print(f'========================================')

    comp_conn.close()
