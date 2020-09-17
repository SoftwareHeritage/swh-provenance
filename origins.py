import psycopg2
import utils

import swh.storage
import swh.storage.algos.origin
import swh.storage.algos.snapshot
import swh.storage.interface

# from swh.model.model import TargetType
from swh.model.identifiers import identifier_to_str


class VisitStatusIterator:
    def __init__(self, storage: swh.storage.interface.StorageInterface):
        self.storage = storage

    def __iter__(self):
        yield from self.iterate()

    def iterate(self):
        for idx, origin in enumerate(swh.storage.algos.origin.iter_origins(self.storage)):
            if idx == 10: break
            print(f'##################################################################')
            print(f'{idx:03} -> {origin}')

            origin = origin.to_dict()
            for visit in swh.storage.algos.origin.iter_origin_visits(self.storage, origin['url']):
                print(f'    +--> {visit}')
                visit = visit.to_dict()
                if 'visit' in visit:
                    for status in swh.storage.algos.origin.iter_origin_visit_statuses(self.storage, origin['url'], visit['visit']):
                        print(f'      +--> {status}')
                        # TODO: may filter only those whose status is 'full'??
                        yield status.to_dict()


def origin_add_revision(
    cursor: psycopg2.extensions.cursor,
    origin: int,    # TODO: use OriginEntry structure
    revision: dict  # TODO: use RevisionEntry structure
):
    cursor.execute('''SELECT 1 FROM revision_in_org WHERE rev=%s''', (revision['id'],))
    visited = cursor.fetchone() is not None

    cursor.execute('''INSERT INTO revision_in_org VALUES (%s, %s)
                      ON CONFLICT DO NOTHING''',
                      (revision['id'], origin))

    if not visited:
        revision_walk_history(cursor, origin, revision['id'], revision)


def revision_walk_history(
    cursor: psycopg2.extensions.cursor,
    origin: int,        # TODO: use OriginEntry structure
    relative: bytes,    # TODO: use OriginEntry structure
    revision: dict      # TODO: use RevisionEntry structure
):
    to_org = []
    to_rev = []
    
    for parent in revision['parents']:
        cursor.execute('''SELECT 1 FROM revision_in_org WHERE rev=%s''', (parent,))
        visited = cursor.fetchone() is not None

        if not visited:
            # The parent revision has never been seen before pointing directly
            # to an origin.
            cursor.execute('''SELECT 1 FROM revision_before_rev WHERE prev=%s''', (parent,))
            known = cursor.fetchone() is not None

            if known:
                # The parent revision is already known in some other revision's
                # history. We should point it directly to the origin and
                # (eventually) walk its history.
                to_org.append(parent)
            else:
                # The parent revision was never seen before. We should walk its
                # history and associate it with the same relative revision.
                to_rev.append(parent)

        else:
            # The parent revision already points to an origin, so its history
            # was properly processed before. We just need to make sure it points
            # to the current origin as well
            cursor.execute('''INSERT INTO revision_in_org VALUES (%s,%s)
                              ON CONFLICT DO NOTHING''', (parent, origin))

    for parent in storage.revision_get(to_org):
        if parent is not None:
            origin_add_revision(cursor, origin, parent.to_dict())

    for parent in storage.revision_get(to_rev):
        if parent is not None:
            parent = parent.to_dict()
            cursor.execute('''INSERT INTO revision_before_rev VALUES (%s,%s)''',
                              (parent['id'], relative))
            revision_walk_history(cursor, origin, relative, parent)


if __name__ == "__main__":
    comp_conn = utils.connect('database.conf', 'compact')
    cursor = comp_conn.cursor()

    utils.execute_sql(comp_conn, 'origins.sql') # Create tables dopping existing ones

    kwargs = {
        "cls" : "remote",
        "url" : "http://uffizi.internal.softwareheritage.org:5002"
    }
    storage = swh.storage.get_storage(**kwargs)

    for status in VisitStatusIterator(storage):
        # Check if current origin is already known and retrieve its internal id.
        cursor.execute('''SELECT id FROM origin WHERE url=%s''', (status['origin'],))
        row = cursor.fetchone()
        origin = row[0] if row is not None else None

        revisions = []
        releases = []

        snapshot = swh.storage.algos.snapshot.snapshot_get_all_branches(storage, status['snapshot'])
        if snapshot is not None:
            branches = snapshot.to_dict()['branches']
            for branch in branches:
                print(f'        +--> {branch} : {branches[branch]}')
                target = branches[branch]['target']
                target_type = branches[branch]['target_type']

                if target_type == 'revision':
                    revisions.append(target)

                elif target_type == 'release':
                    releases.append(target)

        print(f'      ### RELEASES ###############################################')
        # TODO: limit the size of this query!
        for release in storage.release_get(releases):
            print(f'** {release}')

            release = release.to_dict()
            target = release['target']
            target_type = release['target_type']

            if target_type == 'revision':
                revisions.append(target)

        print(f'      ############################################################')
        print(list(map(identifier_to_str, revisions)))

        print(f'      ### REVISIONS ##############################################')
        # TODO: limit the size of this query!
        for revision in storage.revision_get(revisions):
            print(f'** {revision}')
            if revision is not None:
                revision = revision.to_dict()

                if origin is None:
                    # If the origin is seen for the first time, current revision is
                    # the prefered one.
                    cursor.execute('''INSERT INTO origin (url, rev) VALUES (%s,%s)''',
                                      (status['origin'], revision['id']))

                    # Retrieve current origin's internal id (just generated).
                    cursor.execute('''SELECT id FROM origin WHERE url=%s''', (status['origin'],))
                    origin = cursor.fetchone()[0]

                else:
                    # TODO: we should check whether current revision is prefered
                    # over the stored one to perform the update.
                    pass
                    # cursor.execute('''UPDATE origin SET rev=%s WHERE id=%s''',
                    #                   (revision['id'], origin))

                origin_add_revision(cursor, origin, revision)
                comp_conn.commit()
        print(f'      ############################################################')

    comp_conn.close()
