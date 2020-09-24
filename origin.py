import logging
import psycopg2
import utils

import swh.storage
import swh.storage.algos.origin
import swh.storage.algos.snapshot
import swh.storage.interface

from swh.model.identifiers import identifier_to_str


class OriginEntry:
    def __init__(self, url, revisions, id=None):
        self.id = id
        self.url = url
        self.revisions = revisions

    def __str__(self):
        # return f'{type(self).__name__}(id={self.id}, url={self.url}, revisions={list(map(str, self.revisions))})'
        return f'{type(self).__name__}(id={self.id}, url={self.url})'


class RevisionEntry:
    def __init__(self, swhid, parents):
        self.swhid = swhid
        self.parents = parents

    def __str__(self):
        return f'{type(self).__name__}(swhid={identifier_to_str(self.swhid)}, parents={list(map(identifier_to_str, self.parents))})'


class OriginIterator:
    def __init__(self, storage: swh.storage.interface.StorageInterface):
        self.storage = storage

    def __iter__(self):
        yield from self.iterate()

    def iterate(self):
        idx = 0
        for origin in swh.storage.algos.origin.iter_origins(self.storage):
            # print(f'{idx:03} -> {origin}')
            origin = origin.to_dict()

            for visit in swh.storage.algos.origin.iter_origin_visits(self.storage, origin['url']):
                # print(f'    +--> {visit}')
                visit = visit.to_dict()

                if 'visit' in visit:
                    for status in swh.storage.algos.origin.iter_origin_visit_statuses(self.storage, origin['url'], visit['visit']):
                        # print(f'      +--> {status}')
                        # TODO: may filter only those whose status is 'full'??
                        status = status.to_dict()

                        targets = []
                        releases = []

                        snapshot = swh.storage.algos.snapshot.snapshot_get_all_branches(storage, status['snapshot'])
                        if snapshot is not None:
                            branches = snapshot.to_dict()['branches']
                            for branch in branches:
                                # print(f'        +--> {branch} : {branches[branch]}')
                                target = branches[branch]['target']
                                target_type = branches[branch]['target_type']

                                if target_type == 'revision':
                                    targets.append(target)

                                elif target_type == 'release':
                                    releases.append(target)

                        # print(f'      ############################################################')
                        # print(list(map(identifier_to_str, releases)))

                        # print(f'      ### RELEASES ###############################################')
                        # This is done to keep the query in release_get small, hence avoiding a timeout.
                        limit = 100
                        for i in range(0, len(releases), limit):
                            for release in storage.release_get(releases[i:i+limit]):
                                if release is not None:
                                    # print(f'** {release}')

                                    release = release.to_dict()
                                    target = release['target']
                                    target_type = release['target_type']

                                    if target_type == 'revision':
                                        targets.append(target)

                        # print(f'      ############################################################')
                        # print(list(map(identifier_to_str, targets)))

                        # print(f'      ### REVISIONS ##############################################')
                        # This is done to keep the query in revision_get small, hence avoiding a timeout.
                        revisions = []
                        limit = 100
                        for i in range(0, len(targets), limit):
                            for revision in storage.revision_get(targets[i:i+limit]):
                                if revision is not None:
                                    # print(f'** {revision}')
                                    revision = revision.to_dict()
                                    revisions.append(RevisionEntry(revision['id'], revision['parents']))

                        yield OriginEntry(status['origin'], revisions)

                        idx = idx + 1
                        if idx == 1: return


def origin_add_revision(
    cursor: psycopg2.extensions.cursor,
    origin: OriginEntry,
    revision: RevisionEntry
):
    env = [(origin, None, revision)]

    while env:
        origin, relative, revision = env.pop()

        if relative is None:
            # This revision is pointed directly by the origin.
            logging.debug(f'Adding revision {identifier_to_str(revision.swhid)} to origin {origin.id}')
            cursor.execute('''SELECT 1 FROM revision_in_org WHERE rev=%s''', (revision.swhid,))
            visited = cursor.fetchone() is not None
            print(f'Revision {identifier_to_str(revision.swhid)} in origin {origin.id}: {visited}')

            cursor.execute('''INSERT INTO revision_in_org VALUES (%s, %s)
                              ON CONFLICT DO NOTHING''',
                              (revision.swhid, origin.id))

            if not visited:
                # revision_walk_history(cursor, origin, revision.swhid, revision, depth)
                env.append((origin, revision.swhid, revision))

        else:
            # This revision a parent of another one in the history of the
            # relative revision.
            to_org = []
            to_rev = []

            for parent in revision.parents:
                cursor.execute('''SELECT 1 FROM revision_in_org WHERE rev=%s''',
                                  (parent,))
                visited = cursor.fetchone() is not None
                print(f'Parent {identifier_to_str(parent)} in origin {origin.id}: {visited}')

                if not visited:
                    # The parent revision has never been seen before pointing
                    # directly to an origin.
                    cursor.execute('''SELECT 1 FROM revision_before_rev WHERE prev=%s''', (parent,))
                    known = cursor.fetchone() is not None
                    print(f'Revision {identifier_to_str(parent)} before revision: {visited}')

                    if known:
                        # The parent revision is already known in some other
                        # revision's history. We should point it directly to
                        # the origin and (eventually) walk its history.
                        to_org.append(parent)
                    else:
                        # The parent revision was never seen before. We should
                        # walk its history and associate it with the same
                        # relative revision.
                        to_rev.append(parent)

                else:
                    # The parent revision already points to an origin, so its
                    # history was properly processed before. We just need to
                    # make sure it points to the current origin as well.
                    logging.debug(f'Adding parent revision {identifier_to_str(parent)} to origin {origin.id}')
                    cursor.execute('''INSERT INTO revision_in_org VALUES (%s,%s)
                                      ON CONFLICT DO NOTHING''',
                                      (parent, origin.id))

            for parent in storage.revision_get(to_org):
                if parent is not None:
                    parent = parent.to_dict()
                    parent = RevisionEntry(parent['id'], parent['parents'])
                    # origin_add_revision(cursor, origin, parent, depth+1)
                    env.append((origin, None, parent))

            for parent in storage.revision_get(to_rev):
                if parent is not None:
                    parent = parent.to_dict()
                    parent = RevisionEntry(parent['id'], parent['parents'])
                    logging.debug(f'Adding parent revision {identifier_to_str(parent.swhid)} to revision {identifier_to_str(relative)}')
                    cursor.execute('''INSERT INTO revision_before_rev VALUES (%s,%s)''',
                                      (parent.swhid, relative))
                    # revision_walk_history(cursor, origin, relative, parent, depth+1)
                    env.append((origin, relative, parent))


if __name__ == "__main__":
    """Compact model origin-revision layer utility."""
    # logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(filename='origin.log', level=logging.DEBUG)

    comp_conn = utils.connect('database.conf', 'compact')
    cursor = comp_conn.cursor()

    utils.execute_sql(comp_conn, 'origin.sql') # Create tables dopping existing ones

    kwargs = {
        "cls" : "remote",
        "url" : "http://uffizi.internal.softwareheritage.org:5002"
    }
    storage = swh.storage.get_storage(**kwargs)

    for origin in OriginIterator(storage):
        print(f'* {origin}')

        # Check if current origin is already known and retrieve its internal id.
        cursor.execute('''SELECT id FROM origin WHERE url=%s''', (origin.url,))
        row = cursor.fetchone()
        origin.id = row[0] if row is not None else None

        for revision in origin.revisions:
            print(f'** {revision}')

            if origin.id is None:
                # If the origin is seen for the first time, current revision is
                # the prefered one.
                cursor.execute('''INSERT INTO origin (url, rev) VALUES (%s,%s)''',
                                  (origin.url, revision.swhid))

                # Retrieve current origin's internal id (just generated).
                cursor.execute('''SELECT id FROM origin WHERE url=%s''', (origin.url,))
                origin.id = cursor.fetchone()[0]

            else:
                # TODO: we should check whether current revision is prefered
                # over the stored one to perform an update.
                pass
                # cursor.execute('''UPDATE origin SET rev=%s WHERE id=%s''',
                #                   (revision.swhid, origin.id))

            origin_add_revision(cursor, origin, revision)
            comp_conn.commit()
        print(f'##################################################################')

    comp_conn.close()
