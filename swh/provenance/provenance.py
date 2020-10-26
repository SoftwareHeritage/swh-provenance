import itertools
import logging
import os
import psycopg2
import psycopg2.extras

from .archive import ArchiveInterface
from .db_utils import connect, execute_sql
from .model import DirectoryEntry, FileEntry, Tree
from .origin import OriginEntry
from .revision import RevisionEntry

from datetime import datetime
from pathlib import PosixPath
from typing import Dict, List

from swh.model.hashutil import hash_to_hex


def normalize(path: PosixPath) -> PosixPath:
    spath = str(path)
    if spath.startswith('./'):
        return PosixPath(spath[2:])
    return path


def create_database(
    conn: psycopg2.extensions.connection,
    conninfo: dict,
    name: str
):
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    # Create new database dropping previous one if exists
    cursor = conn.cursor();
    cursor.execute(f'''DROP DATABASE IF EXISTS {name}''')
    cursor.execute(f'''CREATE DATABASE {name}''');
    conn.close()

    # Reconnect to server selecting newly created database to add tables
    conninfo['database'] = name
    conn = connect(conninfo)

    sqldir = os.path.dirname(os.path.realpath(__file__))
    execute_sql(conn, os.path.join(sqldir, 'db/provenance.sql'))


################################################################################
################################################################################
################################################################################

class ProvenanceInterface:
    # TODO: turn this into a real interface and move PostgreSQL implementation
    # to a separate file
    def __init__(self, conn: psycopg2.extensions.connection):
        # TODO: consider addind a mutex for thread safety
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.insert_cache = None
        self.select_cache = None
        self.clear_caches()


    def clear_caches(self):
        self.insert_cache = {
            "content": dict(),
            "content_early_in_rev": list(),
            "content_in_dir": list(),
            "directory": dict(),
            "directory_in_rev": list(),
            "revision": dict()
        }
        self.select_cache = {
            "content": dict(),
            "directory": dict(),
            "revision": dict()
        }


    def commit(self):
        result = False
        try:
            self.insert_all()
            self.conn.commit()
            result = True

        except psycopg2.DatabaseError:
            # Database error occurred, rollback all changes
            self.conn.rollback()
            # TODO: maybe serialize and auto-merge transations.
            # The only conflicts are on:
            #   - content: we keep the earliest date
            #   - directory: we keep the earliest date
            #   - content_in_dir: there should be just duplicated entries.

        except Exception as error:
            # Unexpected error occurred, rollback all changes and log message
            logging.warning(f'Unexpected error: {error}')
            self.conn.rollback()

        finally:
            self.clear_caches()

        return result


    def content_add_to_directory(
        self,
        directory: DirectoryEntry,
        blob: FileEntry,
        prefix: PosixPath
    ):
        # logging.debug(f'NEW occurrence of content {hash_to_hex(blob.id)} in directory {hash_to_hex(directory.id)} (path: {prefix / blob.name})')
        # self.cursor.execute('''INSERT INTO content_in_dir VALUES (%s,%s,%s)''',
        #                        (blob.id, directory.id, bytes(normalize(prefix / blob.name))))
        self.insert_cache['content_in_dir'].append(
            (blob.id, directory.id, bytes(normalize(prefix / blob.name)))
        )


    def content_add_to_revision(
        self,
        revision: RevisionEntry,
        blob: FileEntry,
        prefix: PosixPath
    ):
        # logging.debug(f'EARLY occurrence of blob {hash_to_hex(blob.id)} in revision {hash_to_hex(revision.id)} (path: {prefix / blob.name})')
        # self.cursor.execute('''INSERT INTO content_early_in_rev VALUES (%s,%s,%s)''',
        #                        (blob.id, revision.id, bytes(normalize(prefix / blob.name))))
        self.insert_cache['content_early_in_rev'].append(
            (blob.id, revision.id, bytes(normalize(prefix / blob.name)))
        )


    def content_find_first(self, blobid: str):
        logging.info(f'Retrieving first occurrence of content {hash_to_hex(blobid)}')
        self.cursor.execute('''SELECT blob, rev, date, path
                          FROM content_early_in_rev JOIN revision ON revision.id=content_early_in_rev.rev
                          WHERE content_early_in_rev.blob=%s ORDER BY date, rev, path ASC LIMIT 1''', (blobid,))
        return self.cursor.fetchone()


    def content_find_all(self, blobid: str):
        logging.info(f'Retrieving all occurrences of content {hash_to_hex(blobid)}')
        self.cursor.execute('''(SELECT blob, rev, date, path
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
        yield from self.cursor.fetchall()


    # def content_get_early_date(self, blob: FileEntry) -> datetime:
    #     logging.debug(f'Getting content {hash_to_hex(blob.id)} early date')
    #     # First check if the date is being modified by current transection.
    #     date = self.insert_cache['content'].get(blob.id, None)
    #     if date is None:
    #         # If not, check whether it's been query before
    #         date = self.select_cache['content'].get(blob.id, None)
    #         if date is None:
    #             # Otherwise, query the database and cache the value
    #             self.cursor.execute('''SELECT date FROM content WHERE id=%s''',
    #                                    (blob.id,))
    #             row = self.cursor.fetchone()
    #             date = row[0] if row is not None else None
    #             self.select_cache['content'][blob.id] = date
    #     return date


    def content_get_early_dates(self, blobs: List[FileEntry]) -> Dict[bytes, datetime]:
        dates = {}
        pending = []
        for blob in blobs:
            # First check if the date is being modified by current transection.
            date = self.insert_cache['content'].get(blob.id, None)
            if date is not None:
                dates[blob.id] = date
            else:
                # If not, check whether it's been query before
                date = self.select_cache['content'].get(blob.id, None)
                if date is not None:
                    dates[blob.id] = date
                else:
                    pending.append(blob.id)
        if pending:
            # Otherwise, query the database and cache the values
            values = ', '.join(itertools.repeat('%s', len(pending)))
            self.cursor.execute(f'''SELECT id, date FROM content WHERE id IN ({values})''',
                                    tuple(pending))
            for row in self.cursor.fetchall():
                dates[row[0]] = row[1]
                self.select_cache['content'][row[0]] = row[1]
        return dates


    def content_set_early_date(self, blob: FileEntry, date: datetime):
        # logging.debug(f'EARLY occurrence of blob {hash_to_hex(blob.id)} (timestamp: {date})')
        # self.cursor.execute('''INSERT INTO content VALUES (%s,%s)
        #                        ON CONFLICT (id) DO UPDATE SET date=%s''',
        #                        (blob.id, date, date))
        self.insert_cache['content'][blob.id] = date


    def directory_add_to_revision(
        self,
        revision: RevisionEntry,
        directory: DirectoryEntry,
        path: PosixPath
    ):
        # logging.debug(f'NEW occurrence of directory {hash_to_hex(directory.id)} on the ISOCHRONE FRONTIER of revision {hash_to_hex(revision.id)} (path: {path})')
        # self.cursor.execute('''INSERT INTO directory_in_rev VALUES (%s,%s,%s)''',
        #                        (directory.id, revision.id, bytes(normalize(path))))
        self.insert_cache['directory_in_rev'].append(
            (directory.id, revision.id, bytes(normalize(path)))
        )


    def directory_date_in_isochrone_frontier(self, directory: DirectoryEntry) -> datetime:
        # logging.debug(f'Getting directory {hash_to_hex(directory.id)} early date')
        # First check if the date is being modified by current transection.
        date = self.insert_cache['directory'].get(directory.id, None)
        if date is None:
            # If not, check whether it's been query before
            date = self.select_cache['directory'].get(directory.id, None)
            if date is None:
                # Otherwise, query the database and cache the value
                self.cursor.execute('''SELECT date FROM directory WHERE id=%s''',
                                       (directory.id,))
                row = self.cursor.fetchone()
                date = row[0] if row is not None else None
                self.select_cache['directory'][directory.id] = date
        return date


    def directory_get_early_dates(self, dirs: List[DirectoryEntry]) -> Dict[bytes, datetime]:
        dates = {}
        pending = []
        for dir in dirs:
            # First check if the date is being modified by current transection.
            date = self.insert_cache['directory'].get(dir.id, None)
            if date is not None:
                dates[dir.id] = date
            else:
                # If not, check whether it's been query before
                date = self.select_cache['directory'].get(dir.id, None)
                if date is not None:
                    dates[dir.id] = date
                else:
                    pending.append(dir.id)
        if pending:
            # Otherwise, query the database and cache the values
            values = ', '.join(itertools.repeat('%s', len(pending)))
            self.cursor.execute(f'''SELECT id, date FROM directory WHERE id IN ({values})''',
                                    tuple(pending))
            for row in self.cursor.fetchall():
                dates[row[0]] = row[1]
                self.select_cache['directory'][row[0]] = row[1]
        return dates


    def directory_add_to_isochrone_frontier(self, directory: DirectoryEntry,date: datetime):
        # logging.debug(f'EARLY occurrence of directory {hash_to_hex(directory.id)} on the ISOCHRONE FRONTIER (timestamp: {date})')
        # self.cursor.execute('''INSERT INTO directory VALUES (%s,%s)
        #                        ON CONFLICT (id) DO UPDATE SET date=%s''',
        #                        (directory.id, date, date))
        self.insert_cache['directory'][directory.id] = date


    def insert_all(self):
        # Performe insertions with cached information
        psycopg2.extras.execute_values(
            self.cursor,
            '''INSERT INTO content(id, date) VALUES %s
               ON CONFLICT (id) DO UPDATE SET date=excluded.date''',    # TODO: keep earliest date on conflict
            self.insert_cache['content'].items()
        )

        psycopg2.extras.execute_values(
            self.cursor,
            '''INSERT INTO content_early_in_rev VALUES %s
               ON CONFLICT DO NOTHING''',
            self.insert_cache['content_early_in_rev']
        )

        psycopg2.extras.execute_values(
            self.cursor,
            '''INSERT INTO content_in_dir VALUES %s
               ON CONFLICT DO NOTHING''',
            self.insert_cache['content_in_dir']
        )

        psycopg2.extras.execute_values(
            self.cursor,
            '''INSERT INTO directory(id, date) VALUES %s
               ON CONFLICT (id) DO UPDATE SET date=excluded.date''',    # TODO: keep earliest date on conflict
            self.insert_cache['directory'].items()
        )

        psycopg2.extras.execute_values(
            self.cursor,
            '''INSERT INTO directory_in_rev VALUES %s
               ON CONFLICT DO NOTHING''',
            self.insert_cache['directory_in_rev']
        )

        psycopg2.extras.execute_values(
            self.cursor,
            '''INSERT INTO revision(id, date) VALUES %s
               ON CONFLICT (id) DO UPDATE SET date=excluded.date''',    # TODO: keep earliest date on conflict
            self.insert_cache['revision'].items()
        )


    def origin_get_id(self, origin: OriginEntry) -> int:
        if origin.id is None:
            # Check if current origin is already known and retrieve its internal id.
            self.cursor.execute('''SELECT id FROM origin WHERE url=%s''', (origin.url,))
            row = self.cursor.fetchone()

            if row is None:
                # If the origin is seen for the first time, current revision is
                # the prefered one.
                self.cursor.execute('''INSERT INTO origin (url) VALUES (%s) RETURNING id''',
                                  (origin.url,))
                return self.cursor.fetchone()[0]
            else:
                return row[0]
        else:
            return origin.id


    def revision_add(self, revision: RevisionEntry):
        # Add current revision to the compact DB
        self.insert_cache['revision'][revision.id] = revision.date


    def revision_add_before_revision(self, relative: RevisionEntry, revision: RevisionEntry):
        self.cursor.execute('''INSERT INTO revision_before_rev VALUES (%s,%s)''',
                               (revision.id, relative.id))


    def revision_add_to_origin(self, origin: OriginEntry, revision: RevisionEntry):
        self.cursor.execute('''INSERT INTO revision_in_org VALUES (%s,%s)
                               ON CONFLICT DO NOTHING''',
                               (revision.id, origin.id))


    def revision_get_early_date(self, revision: RevisionEntry) -> datetime:
        # logging.debug(f'Getting revision {hash_to_hex(revision.id)} early date')
        # First check if the date is being modified by current transection.
        date = self.insert_cache['revision'].get(revision.id, None)
        if date is None:
            # If not, check whether it's been query before
            date = self.select_cache['revision'].get(revision.id, None)
            if date is None:
                # Otherwise, query the database and cache the value
                self.cursor.execute('''SELECT date FROM revision WHERE id=%s''',
                                       (revision.id,))
                row = self.cursor.fetchone()
                date = row[0] if row is not None else None
                self.select_cache['revision'][revision.id] = date
        return date


    def revision_get_prefered_origin(self, revision: RevisionEntry) -> int:
        self.cursor.execute('''SELECT COALESCE(org,0) FROM revision WHERE id=%s''',
                               (revision.id,))
        row = self.cursor.fetchone()
        # None means revision is not in database
        # 0 means revision has no prefered origin
        return row[0] if row is not None and row[0] != 0 else None


    def revision_in_history(self, revision: RevisionEntry) -> bool:
        self.cursor.execute('''SELECT 1 FROM revision_before_rev WHERE prev=%s''',
                               (revision.id,))
        return self.cursor.fetchone() is not None


    def revision_set_prefered_origin(self, origin: OriginEntry, revision: RevisionEntry):
        self.cursor.execute('''UPDATE revision SET org=%s WHERE id=%s''',
                               (origin.id, revision.id))


    def revision_visited(self, revision: RevisionEntry) -> bool:
        self.cursor.execute('''SELECT 1 FROM revision_in_org WHERE rev=%s''',
                               (revision.id,))
        return self.cursor.fetchone() is not None


################################################################################
################################################################################
################################################################################

def directory_process_content(
    provenance: ProvenanceInterface,
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
                provenance.content_add_to_directory(relative, child, path)
            else:
                # Recursively walk the child directory.
                stack.append((child, path / child.name))


def origin_add(
    provenance: ProvenanceInterface,
    origin: OriginEntry
):
    origin.id = provenance.origin_get_id(cursor, origin)

    for revision in origin.revisions:
        # logging.info(f'Processing revision {hash_to_hex(revision.id)} from origin {origin.url}')
        origin_add_revision(provenance, origin, revision)

        # Commit after each revision
        provenance.commit()      # TODO: verify this!


def origin_add_revision(
    provenance: ProvenanceInterface,
    origin: OriginEntry,
    revision: RevisionEntry
):
    stack = [(None, revision)]

    while stack:
        relative, rev = stack.pop()

        # Check if current revision has no prefered origin and update if necessary.
        prefered = provenance.revision_get_prefered_origin(rev)
        # logging.debug(f'Prefered origin for revision {hash_to_hex(rev.id)}: {prefered}')

        if prefered is None:
            provenance.revision_set_prefered_origin(origin, rev)
        ########################################################################

        if relative is None:
            # This revision is pointed directly by the origin.
            visited = provenance.revision_visited(rev)
            logging.debug(f'Revision {hash_to_hex(rev.id)} in origin {origin.id}: {visited}')

            logging.debug(f'Adding revision {hash_to_hex(rev.id)} to origin {origin.id}')
            provenance.revision_add_to_origin(origin, rev)

            if not visited:
                stack.append((rev, rev))

        else:
            # This revision is a parent of another one in the history of the
            # relative revision.
            for parent in iter(rev):
                visited = provenance.revision_visited(parent)
                logging.debug(f'Parent {hash_to_hex(parent.id)} in some origin: {visited}')

                if not visited:
                    # The parent revision has never been seen before pointing
                    # directly to an origin.
                    known = provenance.revision_in_history(parent)
                    logging.debug(f'Revision {hash_to_hex(parent.id)} before revision: {known}')

                    if known:
                        # The parent revision is already known in some other
                        # revision's history. We should point it directly to
                        # the origin and (eventually) walk its history.
                        logging.debug(f'Adding revision {hash_to_hex(parent.id)} directly to origin {origin.id}')
                        stack.append((None, parent))
                    else:
                        # The parent revision was never seen before. We should
                        # walk its history and associate it with the same
                        # relative revision.
                        logging.debug(f'Adding parent revision {hash_to_hex(parent.id)} to revision {hash_to_hex(relative.id)}')
                        provenance.revision_add_before_revision(relative, parent)
                        stack.append((relative, parent))
                else:
                    # The parent revision already points to an origin, so its
                    # history was properly processed before. We just need to
                    # make sure it points to the current origin as well.
                    logging.debug(f'Adding parent revision {hash_to_hex(parent.id)} to origin {origin.id}')
                    provenance.revision_add_to_origin(origin, parent)


def revision_add(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    revision: RevisionEntry
):
    # Processed content starting from the revision's root directory
    directory = Tree(archive, revision.root).root
    date = provenance.revision_get_early_date(revision)
    if date is None or revision.date < date:
        provenance.revision_add(revision)
        revision_process_content(provenance, revision, directory)
    return provenance.commit()


def revision_process_content(
    provenance: ProvenanceInterface,
    revision: RevisionEntry,
    directory: DirectoryEntry
):
    stack = [(directory, provenance.directory_date_in_isochrone_frontier(directory), directory.name)]

    while stack:
        dir, date, path = stack.pop()

        if date is None:
            # The directory has never been seen on the isochrone graph of a
            # revision. Its children should be checked.
            blobs = [child for child in iter(dir) if isinstance(child, FileEntry)]
            dirs = [child for child in iter(dir) if isinstance(child, DirectoryEntry)]

            blobdates = provenance.content_get_early_dates(blobs)
            dirdates = provenance.directory_get_early_dates(dirs)

            if blobs + dirs:
                dates = list(blobdates.values()) + list(dirdates.values())

                if len(dates) == len(blobs) + len(dirs) and max(dates) <= revision.date:
                    # The directory belongs to the isochrone frontier of the
                    # current revision, and this is the first time it appears
                    # as such.
                    provenance.directory_add_to_isochrone_frontier(dir, max(dates))
                    provenance.directory_add_to_revision(revision, dir, path)
                    directory_process_content(
                        provenance,
                        directory=dir,
                        relative=dir,
                        prefix=PosixPath('.')
                    )

                else:
                    # The directory is not on the isochrone frontier of the
                    # current revision. Its child nodes should be analyzed.
                    ############################################################
                    for child in blobs:
                        date = blobdates.get(child.id, None)
                        if date is None or revision.date < date:
                            provenance.content_set_early_date(child, revision.date)
                        provenance.content_add_to_revision(revision, child, path)

                    for child in dirs:
                        date = dirdates.get(child.id, None)
                        stack.append((child, date, path / child.name))
                    ############################################################

        elif revision.date < date:
            # The directory has already been seen on the isochrone frontier of
            # a revision, but current revision is earlier. Its children should
            # be updated.
            blobs = [child for child in iter(dir) if isinstance(child, FileEntry)]
            dirs = [child for child in iter(dir) if isinstance(child, DirectoryEntry)]

            blobdates = provenance.content_get_early_dates(blobs)
            dirdates = provenance.directory_get_early_dates(dirs)

            ####################################################################
            for child in blobs:
                date = blobdates.get(child.id, None)
                if date is None or revision.date < date:
                    provenance.content_set_early_date(child, revision.date)
                provenance.content_add_to_revision(revision, child, path)

            for child in dirs:
                date = dirdates.get(child.id, None)
                stack.append((child, date, path / child.name))
            ####################################################################

            provenance.directory_add_to_isochrone_frontier(dir, revision.date)

        else:
            # The directory has already been seen on the isochrone frontier of
            # an earlier revision. Just add it to the current revision.
            provenance.directory_add_to_revision(revision, dir, path)


def get_provenance(conninfo: dict) -> ProvenanceInterface:
    # TODO: improve this methos to allow backend selection
    conn = connect(conninfo)
    return ProvenanceInterface(conn)
