# import aiohttp
# import asyncio
import io
import os
import psycopg2

from configparser import ConfigParser
from pathlib import PosixPath

from iterator import RevisionIterator
from model import (
    DirectoryEntry,
    FileEntry,
    TreeEntry,
    Tree
)

from swh.model.identifiers import identifier_to_str
from swh.storage.db import Db


def config(filename, section):
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


def connect(filename, section):
    """ Connect to the PostgreSQL database server """
    conn = None

    try:
        # read connection parameters
        params = config(filename, section)

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return conn


def create_tables(conn, filename='compact.sql'):
    with io.open(filename) as file:
        cur = conn.cursor()
        cur.execute(file.read())
        cur.close()
        conn.commit()


def walk(cursor: psycopg2.extensions.cursor, revision, directory: DirectoryEntry, relative: DirectoryEntry, prefix: PosixPath, ingraph=True):
    # print(directory, name)
    # print("dir: ", identifier_to_str(revision['id']), revision['date'], identifier_to_str(directory.swhid), identifier_to_str(relative.swhid), prefix, ingraph)
    if ingraph:
        cursor.execute('SELECT date FROM directory WHERE id=%s', (directory.swhid,))

        row = cursor.fetchone()
        if row is None or row[0] > revision['date']:
            # This directory belongs to the isochrone graph of the revision.
            # Add directory with the current revision's timestamp as date, and
            # walk recursively looking for new content.
            cursor.execute('''INSERT INTO directory VALUES (%s,%s)
                            ON CONFLICT (id) DO UPDATE
                            SET date=%s''',
                            (directory.swhid, revision['date'], revision['date']))

            for child in directory.children:
                process_child(cursor, revision, child, relative, prefix / child.name)

        else:
            # This directory is just beyond the isochrone graph
            # frontier. Add an entry to the 'directory_in_rev' relation
            # with the path relative to 'prefix', and continue to walk
            # recursively looking only for blobs (ie. 'ingraph=False').
            cursor.execute('INSERT INTO directory_in_rev VALUES (%s,%s,%s)',
                            (directory.swhid, revision['id'], bytes(prefix)))

            for child in directory.children:
                # From now on path is relative to current directory (ie. relative=directory)
                process_child(cursor, revision, child, directory, PosixPath('.') / child.name, ingraph=False)

    else:
        # This directory is completely outside the isochrone graph (far
        # from the frontier). We are just looking for blobs here.
        for child in directory.children:
            process_child(cursor, revision, child, relative, prefix / child.name, ingraph=False)


def process_child(cursor: psycopg2.extensions.cursor, revision, entry: TreeEntry, relative: DirectoryEntry, prefix: PosixPath, ingraph=True):
    if isinstance(entry, DirectoryEntry):
        walk(cursor, revision, entry, relative, prefix, ingraph)
    else:
        process_file(cursor, revision, relative, entry, prefix)


def process_file(cursor: psycopg2.extensions.cursor, revision, directory: DirectoryEntry, blob: FileEntry, path: PosixPath):
    # print(blob.swhid, path)
    # TODO: add logging support!
    # print("blob:", identifier_to_str(revision['id']), revision['date'], identifier_to_str(directory), identifier_to_str(blob), path)
    cursor.execute('SELECT date FROM content WHERE id=%s', (blob.swhid,))

    row = cursor.fetchone()
    if row is None or row[0] > revision['date']:
        # This is an earlier occurrence of the blob. Add it with the current
        # revision's timestamp as date.
        cursor.execute('''INSERT INTO content VALUES (%s,%s,%s,%s)
                          ON CONFLICT (id) DO UPDATE
                          SET date=%s, rev=%s, path=%s''',
                          (blob.swhid, revision['date'], revision['id'], bytes(path),
                           revision['date'], revision['id'], bytes(path)))

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
    archive = connect('database.conf', 'archive')
    compact = connect('database.conf', 'compact')

    create_tables(compact)

    # This call changes the way bytes are codified in the connection
    storage = Db(archive)
    cursor = compact.cursor()
    revisions = RevisionIterator(archive, limit=1000)
    for idx, revision in enumerate(revisions):
        print(f'{idx} - id: {identifier_to_str(revision["id"])} - date: {revision["date"]} - dir: {identifier_to_str(revision["dir"])}')
        # Add current revision to the compact DB and start walking its root directory
        cursor.execute('INSERT INTO revision VALUES (%s,%s)', (revision['id'], revision['date']))

        # walk_directory(cursor, storage, revision, revision["dir"], revision["dir"])
        tree = Tree(archive, revision["dir"])
        walk(cursor, revision, tree.root, tree.root, tree.root.name)

        compact.commit()

    compact.close()
    archive.close()
