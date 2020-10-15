import logging
import psycopg2

import swh.storage
import swh.storage.algos.origin
import swh.storage.algos.snapshot

from .revision import RevisionEntry

from swh.model.hashutil import hash_to_hex
from swh.model.model import Origin, ObjectType, TargetType
from swh.storage.interface import StorageInterface


class OriginEntry:
    def __init__(self, url, revisions, id=None):
        self.id = id
        self.url = url
        self.revisions = revisions

    # def __str__(self):
    #     # return f'{type(self).__name__}(id={self.id}, url={self.url}, revisions={list(map(str, self.revisions))})'
    #     return f'{type(self).__name__}(id={self.id}, url={self.url})'


################################################################################
################################################################################

class OriginIterator:
    """Iterator interface."""

    def __iter__(self):
        pass

    def __next__(self):
        pass


class FileOriginIterator(OriginIterator):
    """Iterator over origins present in the given CSV file."""

    def __init__(self, filename: str, storage: StorageInterface, limit: int=None):
        self.file = open(filename)
        self.limit = limit
        # self.mutex = threading.Lock()
        self.storage = storage

    def __iter__(self):
        yield from iterate_origin_visit_statuses(
            [Origin(url.strip()) for url in self.file],
            self.storage,
            self.limit
        )


class ArchiveOriginIterator:
    """Iterator over origins present in the given storage."""

    def __init__(self, storage: StorageInterface, limit: int=None):
        self.limit = limit
        # self.mutex = threading.Lock()
        self.storage = storage

    def __iter__(self):
        yield from iterate_origin_visit_statuses(
            swh.storage.algos.origin.iter_origins(self.storage),
            self.storage,
            self.limit
        )


def iterate_origin_visit_statuses(origins, storage: StorageInterface, limit: int=None):
    idx = 0
    for origin in origins:
        for visit in swh.storage.algos.origin.iter_origin_visits(storage, origin.url):
            for status in swh.storage.algos.origin.iter_origin_visit_statuses(storage, origin.url, visit.visit):
                # TODO: may filter only those whose status is 'full'??
                targets = []
                releases = []

                snapshot = swh.storage.algos.snapshot.snapshot_get_all_branches(storage, status.snapshot)
                if snapshot is not None:
                    for branch in snapshot.branches:
                        if snapshot.branches[branch].target_type == TargetType.REVISION:
                            targets.append(snapshot.branches[branch].target)

                        elif snapshot.branches[branch].target_type == TargetType.RELEASE:
                            releases.append(snapshot.branches[branch].target)

                # This is done to keep the query in release_get small, hence avoiding a timeout.
                limit = 100
                for i in range(0, len(releases), limit):
                    for release in storage.release_get(releases[i:i+limit]):
                        if revision is not None:
                            if release.target_type == ObjectType.REVISION:
                                targets.append(release.target)

                # This is done to keep the query in revision_get small, hence avoiding a timeout.
                revisions = []
                limit = 100
                for i in range(0, len(targets), limit):
                    for revision in storage.revision_get(targets[i:i+limit]):
                        if revision is not None:
                            parents = list(map(lambda id: RevisionEntry(storage, id), revision.parents))
                            revisions.append(RevisionEntry(storage, revision.id, parents=parents))

                yield OriginEntry(status.origin, revisions)

                idx = idx + 1
                if idx == limit: return


################################################################################
################################################################################

def revision_add_before_rev(
    cursor: psycopg2.extensions.cursor,
    relative: RevisionEntry,
    revision: RevisionEntry
):
    cursor.execute('''INSERT INTO revision_before_rev VALUES (%s,%s)''',
                      (revision.id, relative.id))


def revision_add_to_org(
    cursor: psycopg2.extensions.cursor,
    origin: OriginEntry,
    revision: RevisionEntry
):
    cursor.execute('''INSERT INTO revision_in_org VALUES (%s,%s)
                      ON CONFLICT DO NOTHING''',
                      (revision.id, origin.id))


def revision_get_prefered_org(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry
) -> int:
    cursor.execute('''SELECT COALESCE(org,0) FROM revision WHERE id=%s''',
                      (revision.id,))
    row = cursor.fetchone()
    # None means revision is not in database
    # 0 means revision has no prefered origin
    return row[0] if row is not None and row[0] != 0 else None


def revision_set_prefered_org(
    cursor: psycopg2.extensions.cursor,
    origin: OriginEntry,
    revision: RevisionEntry
):
    cursor.execute('''UPDATE revision SET org=%s WHERE id=%s''',
                     (origin.id, revision.id))


def revision_visited(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry
) -> bool:
    cursor.execute('''SELECT 1 FROM revision_in_org WHERE rev=%s''',
                      (revision.id,))
    return cursor.fetchone() is not None


def revision_in_history(
    cursor: psycopg2.extensions.cursor,
    revision: RevisionEntry
) -> bool:
    cursor.execute('''SELECT 1 FROM revision_before_rev WHERE prev=%s''',
                      (revision.id,))
    return cursor.fetchone() is not None


def origin_add_revision(
    cursor: psycopg2.extensions.cursor,
    storage: StorageInterface,
    origin: OriginEntry,
    revision: RevisionEntry
):
    stack = [(None, revision)]

    while stack:
        relative, rev = stack.pop()

        # Check if current revision has no prefered origin and update if necessary.
        prefered = revision_get_prefered_org(cursor, rev)
        logging.debug(f'Prefered origin for revision {hash_to_hex(rev.id)}: {prefered}')

        if prefered is None:
            revision_set_prefered_org(cursor, origin, rev)
        ########################################################################

        if relative is None:
            # This revision is pointed directly by the origin.
            visited = revision_visited(cursor, rev)
            logging.debug(f'Revision {hash_to_hex(rev.id)} in origin {origin.id}: {visited}')

            logging.debug(f'Adding revision {hash_to_hex(rev.id)} to origin {origin.id}')
            revision_add_to_org(cursor, origin, rev)

            if not visited:
                # revision_walk_history(cursor, origin, rev.id, rev)
                stack.append((rev, rev))

        else:
            # This revision is a parent of another one in the history of the
            # relative revision.
            for parent in iter(rev):
                visited = revision_visited(cursor, parent)
                logging.debug(f'Parent {hash_to_hex(parent.id)} in some origin: {visited}')

                if not visited:
                    # The parent revision has never been seen before pointing
                    # directly to an origin.
                    known = revision_in_history(cursor, parent)
                    logging.debug(f'Revision {hash_to_hex(parent.id)} before revision: {known}')

                    if known:
                        # The parent revision is already known in some other
                        # revision's history. We should point it directly to
                        # the origin and (eventually) walk its history.
                        logging.debug(f'Adding revision {hash_to_hex(parent.id)} directly to origin {origin.id}')
                        # origin_add_revision(cursor, origin, parent)
                        stack.append((None, parent))
                    else:
                        # The parent revision was never seen before. We should
                        # walk its history and associate it with the same
                        # relative revision.
                        logging.debug(f'Adding parent revision {hash_to_hex(parent.id)} to revision {hash_to_hex(relative.id)}')
                        revision_add_before_rev(cursor, relative, parent)
                        # revision_walk_history(cursor, origin, relative, parent)
                        stack.append((relative, parent))
                else:
                    # The parent revision already points to an origin, so its
                    # history was properly processed before. We just need to
                    # make sure it points to the current origin as well.
                    logging.debug(f'Adding parent revision {hash_to_hex(parent.id)} to origin {origin.id}')
                    revision_add_to_org(cursor, origin, parent)


def origin_get_id(
    cursor: psycopg2.extensions.cursor,
    origin: OriginEntry
) -> int:
    if origin.id is None:
        # Check if current origin is already known and retrieve its internal id.
        cursor.execute('''SELECT id FROM origin WHERE url=%s''', (origin.url,))
        row = cursor.fetchone()

        if row is None:
            # If the origin is seen for the first time, current revision is
            # the prefered one.
            cursor.execute('''INSERT INTO origin (url) VALUES (%s) RETURNING id''',
                              (origin.url,))
            return cursor.fetchone()[0]
        else:
            return row[0]
    else:
        return origin.id


def origin_add(
    conn: psycopg2.extensions.connection,
    storage: StorageInterface,
    origin: OriginEntry
):
    with conn.cursor() as cursor:
        origin.id = origin_get_id(cursor, origin)

        for revision in origin.revisions:
            logging.info(f'Processing revision {hash_to_hex(revision.id)} from origin {origin.url}')
            origin_add_revision(cursor, storage, origin, revision)

            # Commit after each revision
            conn.commit()
