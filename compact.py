import io
import logging
import psycopg2
import sys

from configparser import ConfigParser
from datetime import datetime
from pathlib import PosixPath

from iterator import (
    RevisionEntry,
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


def config(filename: PosixPath, section: str):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return db


def typecast_bytea(value, cur):
    if value is not None:
        data = psycopg2.BINARY(value, cur)
        return data.tobytes()


def adapt_conn(conn):
    """Makes psycopg2 use 'bytes' to decode bytea instead of
    'memoryview', for this connection."""
    t_bytes = psycopg2.extensions.new_type((17,), "bytea", typecast_bytea)
    psycopg2.extensions.register_type(t_bytes, conn)

    t_bytes_array = psycopg2.extensions.new_array_type((1001,), "bytea[]", t_bytes)
    psycopg2.extensions.register_type(t_bytes_array, conn)


def connect(filename: PosixPath, section: str):
    """ Connect to the PostgreSQL database server """
    conn = None

    try:
        # read connection parameters
        params = config(filename, section)

        # connect to the PostgreSQL server
        # print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        adapt_conn(conn)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return conn


def create_tables(conn: psycopg2.extensions.cursor, filename: PosixPath='compact.sql'):
    with io.open(filename) as file:
        cur = conn.cursor()
        cur.execute(file.read())
        cur.close()
        conn.commit()


def revision_add(
    cursor: psycopg2.extensions.cursor,
    archive: psycopg2.extensions.connection,
    revision: RevisionEntry,
):
    logging.info(f'Processing revision {identifier_to_str(revision.swhid)} (timestamp: {revision.timestamp})')
    # Processed content starting from the revision's root directory
    directory = Tree(archive, revision.directory).root
    revision_process_directory(cursor, revision, directory, directory.name)
    # Add current revision to the compact DB
    cursor.execute('INSERT INTO revision VALUES (%s,%s)', (revision.swhid, revision.timestamp))


def content_find_first(
    cursor: psycopg2.extensions.cursor,
    swhid: str
):
    logging.info(f'Retrieving first occurrence of content {identifier_to_str(swhid)}')
    cursor.execute('''SELECT blob, rev, date, path
                      FROM content_early_in_rev JOIN revision ON revision.id=content_early_in_rev.rev
                      WHERE content_early_in_rev.blob=%s ORDER BY date ASC LIMIT 1''', (swhid,))
    return cursor.fetchone()


def content_find_all(
    cursor: psycopg2.extensions.cursor,
    swhid: str
):
    logging.info(f'Retrieving all occurrences of content {identifier_to_str(swhid)}')
    cursor.execute('''(SELECT blob, rev, date, path, 1 AS early
                      FROM content_early_in_rev JOIN revision ON revision.id=content_early_in_rev.rev
                      WHERE content_early_in_rev.blob=%s)
                      UNION
                      (SELECT content_in_rev.blob, content_in_rev.rev, revision.date, content_in_rev.path, 2 AS early
                      FROM (SELECT content_in_dir.blob, directory_in_rev.rev, (directory_in_rev.path || '/' || content_in_dir.path)::unix_path AS path
                            FROM content_in_dir
                            JOIN directory_in_rev ON content_in_dir.dir=directory_in_rev.dir
                            WHERE content_in_dir.blob=%s) AS content_in_rev
                      JOIN revision ON revision.id=content_in_rev.rev)
                      ORDER BY date, early''', (swhid, swhid))
    yield from cursor.fetchall()


################################################################################
################################################################################
################################################################################

def content_get_early_timestamp(
    cursor: psycopg2.extensions.cursor,
    blob: FileEntry
):
    cursor.execute('SELECT date FROM content WHERE id=%s', (blob.swhid,))
    row = cursor.fetchone()
    return row[0] if row is not None else None


def content_set_early_timestamp(
    cursor: psycopg2.extensions.cursor,
    blob: FileEntry,
    timestamp: datetime,
    depth
):
    logging.debug(f'{"    "*depth}EARLY occurrence of blob {identifier_to_str(blob.swhid)} (timestamp: {timestamp})')
    cursor.execute('''INSERT INTO content VALUES (%s,%s)
                      ON CONFLICT (id) DO UPDATE SET date=%s''',
                      (blob.swhid, timestamp, timestamp))


def content_add_to_directory(
    cursor: psycopg2.extensions.cursor,
    directory: DirectoryEntry,
    blob: FileEntry,
    prefix: PosixPath,
    depth
):
    logging.debug(f'{"    "*depth}NEW occurrence of content {identifier_to_str(blob.swhid)} in directory {identifier_to_str(directory.swhid)} (path: {prefix / blob.name})')
    cursor.execute('INSERT INTO content_in_dir VALUES (%s,%s,%s)',
                    (blob.swhid, directory.swhid, bytes(prefix / blob.name)))


def content_add_to_revision(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    blob: FileEntry,
    prefix: PosixPath,
    depth
):
    logging.debug(f'{"    "*depth}EARLY occurrence of blob {identifier_to_str(blob.swhid)} in revision {identifier_to_str(revision.swhid)} (path: {prefix / blob.name})')
    cursor.execute('INSERT INTO content_early_in_rev VALUES (%s,%s,%s)',
                    (blob.swhid, revision.swhid, bytes(prefix / blob.name)))


def directory_get_early_timestamp(
    cursor: psycopg2.extensions.cursor,
    directory: DirectoryEntry
):
    cursor.execute('SELECT date FROM directory WHERE id=%s', (directory.swhid,))
    row = cursor.fetchone()
    return row[0] if row is not None else None


def directory_set_early_timestamp(
    cursor: psycopg2.extensions.cursor,
    directory: DirectoryEntry,
    timestamp: datetime,
    depth
):
    logging.debug(f'{"    "*depth}EARLY occurrence of directory {identifier_to_str(directory.swhid)} on the ISOCHRONE FRONTIER (timestamp: {timestamp})')
    cursor.execute('''INSERT INTO directory VALUES (%s,%s)
                      ON CONFLICT (id) DO UPDATE SET date=%s''',
                      (directory.swhid, timestamp, timestamp))


def directory_add_to_revision(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    directory: DirectoryEntry,
    path: PosixPath,
    depth
):
    logging.debug(f'{"    "*depth}NEW occurrence of directory {identifier_to_str(directory.swhid)} on the ISOCHRONE FRONTIER of revision {identifier_to_str(revision.swhid)} (path: {path})')
    cursor.execute('INSERT INTO directory_in_rev VALUES (%s,%s,%s)',
                    (directory.swhid, revision.swhid, bytes(path)))


def directory_process_content(
    cursor: psycopg2.extensions.cursor,
    directory: DirectoryEntry,
    relative: DirectoryEntry,
    prefix: PosixPath,
    depth
):
    for child in iter(directory):
        if isinstance(child, FileEntry):
            # Add content to the relative directory with the computed prefix.
            content_add_to_directory(cursor, relative, child, prefix, depth)

        else:
            # Recursively walk the child directory.
            directory_process_content(cursor, child, relative, prefix / child.name, depth)


def revision_process_directory(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    directory: DirectoryEntry,
    path: PosixPath,
    depth=0
):
    timestamp = directory_get_early_timestamp(cursor, directory)

    if timestamp is None:
        # The directory has never been seen on the isochrone graph of a
        # revision. Its children should be checked.
        timestamps = []
        for child in iter(directory):
            if isinstance(child, FileEntry):
                timestamps.append(content_get_early_timestamp(cursor, child))
            else:
                timestamps.append(directory_get_early_timestamp(cursor, child))

        if None not in timestamps and max(timestamps) <= revision.timestamp:
            # The directory belongs to the isochrone frontier of the current
            # revision, and this is the first time it appears as such.
            directory_set_early_timestamp(cursor, directory, max(timestamps), depth)
            directory_add_to_revision(cursor, revision, directory, path, depth)
            directory_process_content(cursor, directory, directory, PosixPath('.'), depth)

        else:
            # The directory is not on the isochrone frontier of the current
            # revision. Its child nodes should be analyzed.
            for child in iter(directory):
                if isinstance(child, FileEntry):
                    ts = content_get_early_timestamp(cursor, child)
                    if ts is None or ts <= revision.timestamp:
                        content_set_early_timestamp(cursor, child, revision.timestamp, depth)
                    content_add_to_revision(cursor, revision, child, path, depth)

                else:
                    revision_process_directory(cursor, revision, child, path / child.name, depth=depth+1)

    elif revision.timestamp < timestamp:
        # The directory has already been seen on the isochrone frontier of a
        # revision, but current revision is earlier. Its children should be
        # updated.
        for child in iter(directory):
            if isinstance(child, FileEntry):
                ts = content_get_early_timestamp(cursor, child)
                if ts is None or ts <= revision.timestamp:
                    content_set_early_timestamp(cursor, child, revision.timestamp, depth)
                content_add_to_revision(cursor, revision, child, path, depth)

            else:
                revision_process_directory(cursor, revision, child, path / child.name, depth=depth+1)

    else:
        # The directory has already been seen on the isochrone frontier of an
        # earlier revision. Just add it to the current revision.
        directory_add_to_revision(cursor, revision, directory, path, depth)


################################################################################
################################################################################
################################################################################


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) % 2 != 0:
        print('usage: compact [options] count')
        print('  -a database    database name to retrieve directories/content information')
        print('  -d database    database name to retrieve revisions')
        print('  -f filename    local CSV file to retrieve revisions')
        print('  -l limit       max number of revisions to use')
        print('  count          number of random blobs to query for testing')
        exit()

    reset = False
    limit = None
    count = int(sys.argv[-1])

    archname = None
    dataname = None
    filename = None
    for idx in range(len(sys.argv)):
        reset = reset or (sys.argv[idx] in ['-d', '-f'])
        if sys.argv[idx] == '-a':
            archname = sys.argv[idx+1]
        if sys.argv[idx] == '-d':
            dataname = sys.argv[idx+1]
        if sys.argv[idx] == '-f':
            filename = sys.argv[idx+1]
        if sys.argv[idx] == '-l':
            limit = int(sys.argv[idx+1])

    if (dataname is not None or filename is not None) and archname is None:
        print('Error: -a option is compulsatory when -d or -f options are set')
        exit()

    compact = connect('database.conf', 'compact')
    cursor = compact.cursor()

    if reset:
        create_tables(compact)

        if dataname is not None:
            print(f'Reconstructing compact model from {dataname} database (limit={limit})')
            database = connect('database.conf', dataname)
            revisions = ArchiveRevisionIterator(database, limit=limit)
        else:
            print(f'Reconstructing compact model from {filename} CSV file (limit={limit})')
            revisions = FileRevisionIterator(filename, limit=limit)

        archive = connect('database.conf', archname)
        for revision in revisions:
            revision_add(cursor, archive, revision)
            compact.commit()
        archive.close()

        if dataname is not None:
            database.close()

        print(f'========================================')

    cursor.execute(f'SELECT DISTINCT id FROM content LIMIT {count}')
    for idx, row in enumerate(cursor.fetchall()):
        swhid = row[0]
        print(f'Test blob {idx}: {identifier_to_str(swhid)}')

        fst = content_find_first(cursor, swhid)
        print(f'  First occurrence:\n    {identifier_to_str(fst[0])}, {identifier_to_str(fst[1])}, {fst[2]}, {fst[3].decode("utf-8")}')

        print(f'  All occurrences:')
        for row in content_find_all(cursor, swhid):
            print(f'    {row[4]}, {identifier_to_str(row[0])}, {identifier_to_str(row[1])}, {row[2]}, {row[3].decode("utf-8")}')

        print(f'========================================')

    compact.close()
