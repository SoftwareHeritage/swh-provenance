# import aiohttp
# import asyncio
import io
import os
import psycopg2
from configparser import ConfigParser

# from isochrone import IsochroneGraph
from iterator import RevisionIterator

# from swh.core.api import RemoteException
from swh.model.identifiers import (
    # identifier_to_bytes,
    identifier_to_str
)
# from swh.storage.api.client import RemoteStorage
# from swh.storage.backfill import fetch
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


def make_record(elem):
    return {'type' : elem[1], 'id' : elem[2], 'path' : elem[3].decode('utf-8')}


# TODO: refactor this method to take the whole directory structure as parameter
# and avoid multiple requies (using swh.storage.db.directory_walk prior to
# calling the function, instead of swh.storage.db.directory_walk_one within it)
def walk_directory(cursor, storage, revision, directory, relative, name='./', ingraph=True):
    # print("dir: ", identifier_to_str(revision['id']), revision['date'], identifier_to_str(directory), identifier_to_str(relative), name, ingraph)
    if ingraph:
        cursor.execute('SELECT date FROM directory WHERE id=%s', (directory,))

        row = cursor.fetchone()
        if row is None or row[0] > revision['date']:
            # This directory belongs to the isochrone graph of the revision.
            # Add directory with the current revision's timestamp as date, and
            # walk recursively looking for new content.
            cursor.execute('''INSERT INTO directory VALUES (%s,%s)
                            ON CONFLICT (id) DO UPDATE
                            SET date=%s''',
                            (directory, revision['date'], revision['date']))

            for entry in storage.directory_walk_one(directory):
                child = make_record(entry)
                path = os.path.join(name, child['path'])

                if child['type'] == 'dir':
                    walk_directory(cursor, storage, revision, child['id'], relative, name=path)

                elif child['type'] == 'file':
                    process_file(cursor, storage, revision, relative, child['id'], path)

        else:
            # This directory is just beyond the isochrone graph
            # frontier. Add an entry to the 'directory_in_rev' relation
            # with the path relative to 'name', and continue to walk
            # recursively looking only for blobs (ie. 'ingraph=False').
            cursor.execute('INSERT INTO directory_in_rev VALUES (%s,%s,%s)', (directory, revision['id'], name))

            for entry in storage.directory_walk_one(directory):
                child = make_record(entry)
                # From now on path is relative to current directory (ie. relative=directory)
                path = os.path.join('.', child['path'])

                if child['type'] == 'dir':
                    walk_directory(cursor, storage, revision, child['id'], directory, name=path, ingraph=False)

                elif child['type'] == 'file':
                    process_file(cursor, storage, revision, directory, child['id'], path)

    else:
        # This directory is completely outside the isochrone graph (far
        # from the frontier). We are just looking for blobs here.
        for entry in storage.directory_walk_one(directory):
            child = make_record(entry)
            path = os.path.join(name, child['path'])

            if child['type'] == 'dir':
                walk_directory(cursor, storage, revision, child['id'], relative, name=path, ingraph=False)

            elif child['type'] == 'file':
                process_file(cursor, storage, revision, relative, child['id'], path)


def process_file(cursor, storage, revision, directory, blob, name):
    # TODO: add logging support!
    # print("blob:", identifier_to_str(revision['id']), revision['date'], identifier_to_str(directory), identifier_to_str(blob), name)
    cursor.execute('SELECT date FROM content WHERE id=%s', (blob,))

    row = cursor.fetchone()
    if row is None or row[0] > revision['date']:
        # This is an earlier occurrence of the blob. Add it with the current
        # revision's timestamp as date, and set a record for
        # 'content_early_in_rev' with the 'path = name'.
        cursor.execute('''INSERT INTO content VALUES (%s,%s,%s,%s)
                          ON CONFLICT (id) DO UPDATE
                          SET date=%s, rev=%s, path=%s''',
                          (blob, revision['date'], revision['id'], name, revision['date'], revision['id'], name))

    else:
        # This blob was seen before but this occurrence is older. Add
        # an entry to the 'content_in_dir' relation with the path
        # relative to the parent directory in the isochrone graph
        # frontier.
        cursor.execute('''INSERT INTO content_in_dir VALUES (%s,%s,%s)
                          ON CONFLICT DO NOTHING''',
                          (blob, directory, name))
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
        walk_directory(cursor, storage, revision, revision["dir"], revision["dir"])
        compact.commit()

    compact.close()
    archive.close()
