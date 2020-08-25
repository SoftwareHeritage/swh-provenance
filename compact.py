import io
import logging
import psycopg2
import sys

from configparser import ConfigParser
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
    directory_compute_early_timestamp(cursor, revision, directory, directory.name)
    # Add current revision to the compact DB
    cursor.execute('INSERT INTO revision VALUES (%s,%s)', (revision.swhid, revision.timestamp))


def content_find_first(
    cursor: psycopg2.extensions.cursor,
    swhid: str
):
    logging.info(f'Retrieving first ocurrence of content {identifier_to_str(swhid)}')
    cursor.execute('''SELECT blob, rev, date, path
                      FROM content_early_in_rev JOIN revision ON revision.id=content_early_in_rev.rev
                      WHERE content_early_in_rev.blob=%s ORDER BY date ASC LIMIT 1''', (swhid,))
    return cursor.fetchone()


def content_find_all(
    cursor: psycopg2.extensions.cursor,
    swhid: str
):
    logging.info(f'Retrieving all ocurrences of content {identifier_to_str(swhid)}')
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


def content_get_known_timestamp(
    cursor: psycopg2.extensions.cursor,
    blob: FileEntry
):
    cursor.execute('SELECT date FROM content WHERE id=%s', (blob.swhid,))
    row = cursor.fetchone()
    return row[0] if row is not None else None


def content_compute_early_timestamp(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    blob: FileEntry,
    path: PosixPath
):
    # This method computes the early timestamp of a blob based on the given
    # revision and the information in the database from previously visited ones.
    # It updates the database if the given revision comes out-of-order.
    timestamp = content_get_known_timestamp(cursor, blob)

    if timestamp is None:
        # This blob has never been seen before. Just add it to 'content' table.
        timestamp = revision.timestamp
        cursor.execute('INSERT INTO content VALUES (%s,%s)',
                        (blob.swhid, timestamp))

    elif revision.timestamp < timestamp:
        # This is an out-of-order early occurrence of the blob. Update its
        # timestamp in 'content' table.
        timestamp = revision.timestamp
        cursor.execute('UPDATE content SET date=%s WHERE id=%s',
                        (timestamp, blob.swhid))

    cursor.execute('INSERT INTO content_early_in_rev VALUES (%s,%s,%s)',
                    (blob.swhid, revision.swhid, bytes(path)))

    return timestamp


def directory_get_known_timestamp(
    cursor: psycopg2.extensions.cursor,
    directory: DirectoryEntry
):
    cursor.execute('SELECT date FROM directory WHERE id=%s',
                    (directory.swhid,))
    row = cursor.fetchone()
    return row[0] if row is not None else None


def directory_compute_early_timestamp(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    directory: DirectoryEntry,
    path: PosixPath
):
    # This method computes the early timestamp of a directory based on the
    # given revision and the information in the database from previously
    # visited ones. It updates the database if the given revision comes
    # out-of-order.
    dir_timestamp = directory_get_known_timestamp(cursor, directory)

    if dir_timestamp is not None:
        # Current directory has already been seen in the isochrone frontier of
        # an already processed revision.
        if revision.timestamp < dir_timestamp:
            # Current revision is out-of-order. All the content of current
            # directory should be processed again. Removed entry from
            # 'directory' and try again.
            cursor.execute('DELETE FROM directory WHERE id=%s',
                            (directory.swhid,))
            return directory_compute_early_timestamp(
                cursor, revision, directory, path)

        else:
            # Current directory has already been seen in the isochrone frontier.
            # Just add an entry to the 'directory_in_rev' relation that
            # associates the directory with current revision and computed path.
            cursor.execute('INSERT INTO directory_in_rev VALUES (%s,%s,%s)',
                            (directory.swhid, revision.swhid, bytes(path)))
            return dir_timestamp

    else:
        # Current directory has never been seen before in the isochrone
        # frontier of a revision. Compute early timestamp for all its childen.
        children_timestamps = []
        for child in iter(directory):
            if isinstance(child, FileEntry):
                # Compute early timestamp of current blob.
                children_timestamps.append(
                    content_compute_early_timestamp(
                        cursor, revision, child, path / child.name)
                )

            else:
                # Recursively compute the early timestamp.
                child_timestamp = directory_compute_early_timestamp(
                    cursor, revision, child, path / child.name)

                # Ignore any sub-tree with empty directories on the leaves.
                if child_timestamp is not None:
                    children_timestamps.append(child_timestamp)

        if not children_timestamps:
             # Current directory does not recursively contain any blob.
             return None

        else:
            dir_timestamp = max(children_timestamps)
            if revision.timestamp < dir_timestamp:
                # Current revision is out-of-order. This should not happen
                # early timestamps for children in this branch of the algorithm
                # are computed taking current revision into account.
                logging.warning("UNEXPECTED SITUATION WITH OUT-OF-ORDER REVISION")
                return revision.timestamp

            else:
                # This is the first time that current directory is seen in the
                # isochrone frontier. Add current directory to 'directory' with
                # the computed timestamp.
                cursor.execute('INSERT INTO directory VALUES (%s,%s)',
                                (directory.swhid, dir_timestamp))

                # Add an entry to the 'directory_in_rev' relation that
                # associates the directory with current revision and computed
                # path.
                cursor.execute('INSERT INTO directory_in_rev VALUES (%s,%s,%s)',
                                (directory.swhid, revision.swhid, bytes(path)))

                # Recursively find all content within current directory and
                # add them to 'content_in_dir' with their path relative to
                # current directory.
                process_content_in_dir(
                    cursor, directory, directory.swhid, PosixPath('.'))

                return dir_timestamp


def process_content_in_dir(
    cursor: psycopg2.extensions.cursor,
    directory: DirectoryEntry,
    relative: str,
    prefix: PosixPath
):
    for child in iter(directory):
        if isinstance(child, FileEntry):
            # Add an entry to 'content_in_dir' in dir for the current blob.
            cursor.execute('INSERT INTO content_in_dir VALUES (%s,%s,%s)',
                            (child.swhid, relative, bytes(prefix / child.name)))

        else:
            # Recursively walk the child directory.
            process_content_in_dir(cursor, child, relative, prefix / child.name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

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
