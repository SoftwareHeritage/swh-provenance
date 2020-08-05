import io
import os
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
from swh.storage.db import Db


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
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def connect(filename: PosixPath, section: str):
    """ Connect to the PostgreSQL database server """
    conn = None

    try:
        # read connection parameters
        params = config(filename, section)

        # connect to the PostgreSQL server
        # print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

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
    # Add current revision to the compact DB and start walking its root directory
    cursor.execute('INSERT INTO revision VALUES (%s,%s)', (revision.swhid, revision.timestamp))
    tree = Tree(archive, revision.directory)
    walk(cursor, revision, tree.root, tree.root, tree.root.name)


def content_find_first(
    cursor: psycopg2.extensions.cursor,
    swhid: str
):
    cursor.execute('SELECT * FROM content WHERE blob=%s ORDER BY date ASC LIMIT 1', (swhid,))
    return cursor.fetchone()


def content_find_all(
    cursor: psycopg2.extensions.cursor,
    swhid: str
):
    cursor.execute('''(SELECT blob, rev, date, path FROM content WHERE blob=%s)
                      UNION
                      (SELECT content_in_rev.blob, content_in_rev.rev, revision.date, content_in_rev.path
                      FROM (SELECT content_in_dir.blob, directory_in_rev.rev, (directory_in_rev.path || '/' || content_in_dir.path)::unix_path AS path
                            FROM content_in_dir
                            JOIN directory_in_rev ON content_in_dir.dir=directory_in_rev.dir
                            WHERE content_in_dir.blob=%s) AS content_in_rev
                      JOIN revision ON revision.id=content_in_rev.rev)
                      ORDER BY date''', (swhid, swhid))
    yield from cursor.fetchall()


def walk(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    directory: DirectoryEntry,
    relative: DirectoryEntry,
    prefix: PosixPath,
    ingraph: bool=True
):
    # TODO: add logging support!
    # print("dir: ", identifier_to_str(revision.swhid), revision.timestamp, identifier_to_str(directory.swhid), identifier_to_str(relative.swhid), prefix, ingraph)
    if ingraph:
        cursor.execute('SELECT date FROM directory WHERE id=%s', (directory.swhid,))

        row = cursor.fetchone()
        if row is None or row[0] > revision.timestamp:
            # This directory belongs to the isochrone graph of the revision.
            # Add directory with the current revision's timestamp as date, and
            # walk recursively looking for new content.
            cursor.execute('''INSERT INTO directory VALUES (%s,%s)
                            ON CONFLICT (id) DO UPDATE
                            SET date=%s''',
                            (directory.swhid, revision.timestamp, revision.timestamp))

            for child in directory.children:
                process_child(cursor, revision, child, relative, prefix / child.name)

        else:
            # This directory is just beyond the isochrone graph
            # frontier. Add an entry to the 'directory_in_rev' relation
            # with the path relative to 'prefix', and continue to walk
            # recursively looking only for blobs (ie. 'ingraph=False').
            cursor.execute('INSERT INTO directory_in_rev VALUES (%s,%s,%s)',
                            (directory.swhid, revision.swhid, bytes(prefix)))

            for child in directory.children:
                # From now on path is relative to current directory (ie. relative=directory)
                process_child(cursor, revision, child, directory, PosixPath('.') / child.name, ingraph=False)

    else:
        # This directory is completely outside the isochrone graph (far
        # from the frontier). We are just looking for blobs here.
        for child in directory.children:
            process_child(cursor, revision, child, relative, prefix / child.name, ingraph=False)


def process_child(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    entry: TreeEntry,
    relative: DirectoryEntry,
    prefix: PosixPath,
    ingraph: bool=True
):
    if isinstance(entry, DirectoryEntry):
        walk(cursor, revision, entry, relative, prefix, ingraph)
    else:
        process_file(cursor, revision, relative, entry, prefix)


def process_file(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry,
    directory: DirectoryEntry,
    blob: FileEntry,
    path: PosixPath
):
    # TODO: add logging support!
    # print("blob:", identifier_to_str(revision.swhid), revision.timestamp, identifier_to_str(directory), identifier_to_str(blob), path)
    cursor.execute('SELECT date FROM content WHERE blob=%s ORDER BY date ASC LIMIT 1', (blob.swhid,))
    # cursor.execute('SELECT MIN(date) FROM content WHERE blob=%s', (blob.swhid,))

    row = cursor.fetchone()
    if row is None or row[0] > revision.timestamp:
        # This is an earlier occurrence of the blob. Add it with the current
        # revision's timestamp as date.
        cursor.execute('''INSERT INTO content VALUES (%s,%s,%s,%s)''',
        # cursor.execute('''INSERT INTO content VALUES (%s,%s,%s,%s)
        #                   ON CONFLICT (blob, rev) DO UPDATE
        #                   SET date=%s, path=%s''',
                          (blob.swhid, revision.swhid, revision.timestamp, bytes(path)))

    else:
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
    if len(sys.argv) != 4:
        print('usage: compact <reset> <limit> <count>')
        print('     <reset> : bool      reconstruct compact model database')
        print('     <limit> : int       number of revision to add')
        print('     <count> : int       number of blobs to query')

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
            # TODO: add logging support!
            print(f'{idx} - id: {identifier_to_str(revision.swhid)} - date: {revision.timestamp} - dir: {identifier_to_str(revision.directory)}')
            revision_add(cursor, archive, revision)
            compact.commit()

        archive.close()

        print(f'========================================')

    cursor.execute(f'SELECT DISTINCT blob FROM content LIMIT {count}')
    for idx, row in enumerate(cursor.fetchall()):
        swhid = row[0]
        print(f'Test blob {idx}: {identifier_to_str(bytes(swhid))}')

        fst = content_find_first(cursor, swhid)
        print(f'  First occurrence:\n    {identifier_to_str(bytes(fst[0]))}, {identifier_to_str(bytes(fst[1]))}, {fst[2]}, {bytes(fst[3]).decode("utf-8")}')

        print(f'  All occurrences:')
        for row in content_find_all(cursor, swhid):
            print(f'    {identifier_to_str(bytes(row[0]))}, {identifier_to_str(bytes(row[1]))}, {row[2]}, {bytes(row[3]).decode("utf-8")}')

        print(f'========================================')

    compact.close()
