import io
import logging
import psycopg2
import sys

from configparser import ConfigParser
from pathlib import PosixPath

from iterator import (
    RevisionEntry,
    RevisionIterator
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
    # Add current revision to the compact DB and start processing its root directory
    cursor.execute('INSERT INTO revision VALUES (%s,%s)', (revision.swhid, revision.timestamp))
    directory = Tree(archive, revision.directory).root
    process(cursor, revision, directory, directory, directory.name)


def content_find_first(
    cursor: psycopg2.extensions.cursor,
    swhid: str
):
    logging.info(f'Retrieving first ocurrence of content {identifier_to_str(swhid)}')
    cursor.execute('SELECT * FROM content WHERE blob=%s ORDER BY date ASC LIMIT 1', (swhid,))
    return cursor.fetchone()


def content_find_all(
    cursor: psycopg2.extensions.cursor,
    swhid: str
):
    logging.info(f'Retrieving all ocurrences of content {identifier_to_str(swhid)}')
    cursor.execute('''(SELECT blob, rev, date, path, 1 AS early FROM content WHERE blob=%s)
                      UNION
                      (SELECT content_in_rev.blob, content_in_rev.rev, revision.date, content_in_rev.path, 2 AS early
                      FROM (SELECT content_in_dir.blob, directory_in_rev.rev, (directory_in_rev.path || '/' || content_in_dir.path)::unix_path AS path
                            FROM content_in_dir
                            JOIN directory_in_rev ON content_in_dir.dir=directory_in_rev.dir
                            WHERE content_in_dir.blob=%s) AS content_in_rev
                      JOIN revision ON revision.id=content_in_rev.rev)
                      ORDER BY date, early''', (swhid, swhid))
    yield from cursor.fetchall()


def process(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    entry: TreeEntry,
    relative: DirectoryEntry,
    prefix: PosixPath,
    ingraph: bool=True
):
    logging.info(f'Processing element {identifier_to_str(entry.swhid)} ({entry.name}) relative to directory {identifier_to_str(relative.swhid)} ({relative.name}) with prefix {prefix} {"inside" if ingraph else "outside"} the isochrone graph')
    if isinstance(entry, DirectoryEntry):
        process_dir(cursor, revision, entry, relative, prefix, ingraph)
    else:
        process_file(cursor, revision, relative, entry, prefix)


def process_dir(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    directory: DirectoryEntry,
    relative: DirectoryEntry,
    prefix: PosixPath,
    ingraph: bool=True
):
    if ingraph:
        cursor.execute('SELECT date FROM directory WHERE id=%s', (directory.swhid,))

        row = cursor.fetchone()
        if row is None or row[0] > revision.timestamp:
            logging.info(f'New EARLY occurrence of directory {identifier_to_str(directory.swhid)} ({directory.name})')
            # This directory belongs to the isochrone graph of the revision.
            # Add directory with the current revision's timestamp as date, and
            # process recursively looking for new content.
            cursor.execute('''INSERT INTO directory VALUES (%s,%s)
                            ON CONFLICT (id) DO UPDATE
                            SET date=%s''',
                            (directory.swhid, revision.timestamp, revision.timestamp))

            for child in iter(directory):
                process(cursor, revision, child, relative, prefix / child.name)

        else:
            logging.info(f'New occurrence of directory {identifier_to_str(directory.swhid)} ({directory.name}) in the isochrone graph frontier')
            # This directory is just beyond the isochrone graph
            # frontier. Check whether it has already been visited before to
            # avoid recursively processing its children.
            cursor.execute('SELECT dir FROM directory_in_rev WHERE dir=%s', (directory.swhid,))
            visited = cursor.fetchone() is not None

            # Add an entry to the 'directory_in_rev' relation that associates
            # the directory with current revision and computed prefix.
            cursor.execute('INSERT INTO directory_in_rev VALUES (%s,%s,%s)',
                            (directory.swhid, revision.swhid, bytes(prefix)))

            if not visited:
                logging.info(f'+>  Recursive walk required to adjust children\'s relative path')
                # The directory hasn't been visited before. Continue to process
                # recursively looking only for blobs (ie. 'ingraph=False').
                # From now on path is relative to current directory (ie.
                # relative=directory)
                for child in iter(directory):
                    process(cursor, revision, child, directory, child.name, ingraph=False)

    else:
        logging.info(f'Walking through directory {identifier_to_str(directory.swhid)} ({directory.name}) outside the isochrone graph')
        # This directory is completely outside the isochrone graph (far
        # from the frontier). We are just looking for blobs here.
        for child in iter(directory):
            process(cursor, revision, child, relative, prefix / child.name, ingraph=False)


def process_file(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    directory: DirectoryEntry,
    blob: FileEntry,
    path: PosixPath
):
    cursor.execute('SELECT date FROM content WHERE blob=%s ORDER BY date ASC LIMIT 1', (blob.swhid,))
    # cursor.execute('SELECT MIN(date) FROM content WHERE blob=%s', (blob.swhid,))

    row = cursor.fetchone()
    if row is None or row[0] > revision.timestamp:
        logging.info(f'New EARLY occurrence of content {identifier_to_str(blob.swhid)} ({blob.name})')
        # This is an earlier occurrence of the blob. Add it with the current
        # revision's timestamp as date.
        cursor.execute('''INSERT INTO content VALUES (%s,%s,%s,%s)''',
                          (blob.swhid, revision.swhid, revision.timestamp, bytes(path)))

    else:
        logging.info(f'New occurrence of content {identifier_to_str(blob.swhid)} ({blob.name})')
        # This blob was seen before but this occurrence is older. Add
        # an entry to the 'content_in_dir' relation with the path
        # relative to the parent directory in the isochrone graph
        # frontier.
        cursor.execute('''INSERT INTO content_in_dir VALUES (%s,%s,%s)
                          ON CONFLICT DO NOTHING''',
                          (blob.swhid, directory.swhid, bytes(path)))
        # WARNING: There seem to be duplicated directories within the same
        #          revision. Hence, their blobs may appear many times with the
        #          same directory ID and 'relative' path. That's why we need
        #          the 'ON CONFLICT DO NOTHING' statement.


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    if len(sys.argv) != 4:
        print('usage: compact <reset> <limit> <count>')
        print('  <reset> : bool     reconstruct compact model database')
        print('  <limit> : int      number of revision to use')
        print('  <count> : int      number of blobs to query for testing')
        exit()

    reset = sys.argv[1].lower() == 'true'
    limit = int(sys.argv[2])
    count = int(sys.argv[3])

    compact = connect('database.conf', 'compact')
    cursor = compact.cursor()

    if reset:
        print(f'Reconstructing compact model database with {limit} revisions')

        archive = connect('database.conf', 'archive')
        create_tables(compact)

        revisions = RevisionIterator(archive, limit=limit)
        for idx, revision in enumerate(revisions):
            revision_add(cursor, archive, revision)
            compact.commit()

        archive.close()

        print(f'========================================')

    cursor.execute(f'SELECT DISTINCT blob FROM content LIMIT {count}')
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
