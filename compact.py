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


# def walk_directory(cursor, storage, revision, directory, prefix='./', ingraph=True):
#     for entry in storage.directory_walk_one(directory):
#         record = make_record(entry)

#         if record['type'] == 'dir':
#             if ingraph:
#                 cursor.execute('SELECT date FROM directory WHERE id=%s', (record['id'],))

#                 row = cursor.fetchone()
#                 if row is None:
#                     # This directory belongs to the isochrone graph of the
#                     # revision. Add directory with the current revision's
#                     # timestamp as date, and walk recursively looking for new
#                     # content.
#                     cursor.execute('INSERT INTO directory VALUES (%s,%s)',
#                         (record['id'], revision['date']))
#                     path = os.path.join(prefix, record['path'])
#                     walk_directory(cursor, storage, revision, record['id'], path)

#                 elif row[0] > revision['date']:
#                     # This directory belongs to the isochrone graph of the
#                     # revision. Update its date to match the current revision's
#                     # timestamp.
#                     cursor.execute('UPDATE directory SET date=%s WHERE id=%s',
#                         (revision['date'], record['id']))
#                     # TODO: update entries from 'directory_in_rev' pointing to
#                     #       this directory to now point to its children?

#                 else:
#                     # This directory is just beyond the isochrone graph
#                     # frontier. Add an entry to the 'directory_in_rev' relation
#                     # with the path relative to 'prefix', and continue to walk
#                     # recursively looking only for blobs (ie. 'ingraph=False').
#                     path = os.path.join(prefix, record['path'])
#                     cursor.execute('INSERT INTO directory_in_rev VALUES (%s,%s,%s)',
#                         (record['id'], revision['id'], path))
#                     # From now on prefix is relative to current directory
#                     walk_directory(cursor, storage, revision, record['id'], ingraph=False)

#             else:
#                 # This directory is completely outside the isochrone graph (far
#                 # from the frontier). We are just looking for blobs here.
#                 path = os.path.join(prefix, record['path'])
#                 walk_directory(cursor, storage, revision, record['id'], path, ingraph=False)

#         elif record['type'] == 'file':
#             cursor.execute('SELECT date FROM content WHERE id=%s', (record['id'],))

#             row = cursor.fetchone()
#             if row is None:
#                 # This blob was never seen before. Add blob with the current
#                 # revision's timestamp as date, and set a record for
#                 # 'content_early_in_rev' with the 'path = prefix + blob_name'.
#                 pass

#             elif row[0] > revision['date']:
#                 # This is an earlier occurrance of an already seen blob. Update
#                 # its date to match the current revision's timestamp.
#                 # TODO: update entries from 'content_early_in_rev' with current
#                 #       path, and move previous entry to 'content_in_rev' with
#                 #       its path now relative to the parent directory in the
#                 #       isochrone graph frontier?
#                 pass

#             else:
#                 # This blob was seen before but this occurrence is older. Add
#                 # an entry to the 'content_in_rev' relation with the path
#                 # relative to the parent directory in the isochrone graph
#                 # frontier.
#                 pass


def walk_directory(cursor, storage, revision, directory, name='./', ingraph=True):
    if ingraph:
        cursor.execute('SELECT date FROM directory WHERE id=%s', (directory,))

        row = cursor.fetchone()
        if row is None:
            # This directory belongs to the isochrone graph of the
            # revision. Add directory with the current revision's
            # timestamp as date, and walk recursively looking for new
            # content.
            # cursor.execute('INSERT INTO directory VALUES (%s,%s)', (directory, revision['date']))

            for entry in storage.directory_walk_one(directory):
                record = make_record(entry)
                path = os.path.join(name, record['path'])

                if record['type'] == 'dir':
                    walk_directory(cursor, storage, revision, record['id'], name=path)

                elif record['type'] == 'file':
                    process_file(cursor, storage, revision, record['id'], name=path)

        elif row[0] > revision['date']:
            # This directory belongs to the isochrone graph of the
            # revision. Update its date to match the current revision's
            # timestamp.
            # cursor.execute('UPDATE directory SET date=%s WHERE id=%s', (revision['date'], directory))
            pass
            # TODO: update entries from 'directory_in_rev' pointing to
            #       this directory to now point to its children?

        else:
            # This directory is just beyond the isochrone graph
            # frontier. Add an entry to the 'directory_in_rev' relation
            # with the path relative to 'prefix', and continue to walk
            # recursively looking only for blobs (ie. 'ingraph=False').
            # cursor.execute('INSERT INTO directory_in_rev VALUES (%s,%s,%s)', (directory, revision['id'], name))

            for entry in storage.directory_walk_one(directory):
                record = make_record(entry)

                # From now on prefix is relative to current directory
                path = os.path.join('.', record['path'])

                if record['type'] == 'dir':
                    walk_directory(cursor, storage, revision, record['id'], name=path, ingraph=False)

                elif record['type'] == 'file':
                    process_file(cursor, storage, revision, record['id'], name=path, ingraph=False)

    else:
        # This directory is completely outside the isochrone graph (far
        # from the frontier). We are just looking for blobs here.
        for entry in storage.directory_walk_one(directory):
            record = make_record(entry)

            # From now on prefix is relative to current directory
            path = os.path.join(name, record['path'])

            if record['type'] == 'dir':
                walk_directory(cursor, storage, revision, record['id'], name=path, ingraph=False)

            elif record['type'] == 'file':
                process_file(cursor, storage, revision, record['id'], name=path, ingraph=False)


def process_file(cursor, storage, revision, blob, name='./', ingraph=True):
    cursor.execute('SELECT date FROM content WHERE id=%s', (blob,))

    row = cursor.fetchone()
    if row is None:
        # This blob was never seen before. Add blob with the current
        # revision's timestamp as date, and set a record for
        # 'content_early_in_rev' with the 'path = name'.
        pass

    elif row[0] > revision['date']:
        # This is an earlier occurrance of an already seen blob. Update
        # its date to match the current revision's timestamp.
        # TODO: update entries from 'content_early_in_rev' with current
        #       path, and move previous entry to 'content_in_rev' with
        #       its path now relative to the parent directory in the
        #       isochrone graph frontier?
        pass

    else:
        # This blob was seen before but this occurrence is older. Add
        # an entry to the 'content_in_rev' relation with the path
        # relative to the parent directory in the isochrone graph
        # frontier.
        pass


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
        walk_directory(cursor, storage, revision, revision["dir"])
        compact.commit()

    compact.close()
    archive.close()
