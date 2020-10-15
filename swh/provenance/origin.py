from .revision import RevisionEntry

from swh.model.model import Origin, ObjectType, TargetType
from swh.storage.algos.origin import iter_origin_visits, iter_origin_visit_statuses
from swh.storage.algos.snapshot import snapshot_get_all_branches
from swh.storage.interface import StorageInterface


class OriginEntry:
    def __init__(self, url, revisions, id=None):
        self.id = id
        self.url = url
        self.revisions = revisions


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
        yield from iterate_statuses(
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
        yield from iterate_statuses(
            swh.storage.algos.origin.iter_origins(self.storage),
            self.storage,
            self.limit
        )


def iterate_statuses(origins, storage: StorageInterface, limit: int=None):
    idx = 0
    for origin in origins:
        for visit in iter_origin_visits(storage, origin.url):
            for status in iter_origin_visit_statuses(storage, origin.url, visit.visit):
                # TODO: may filter only those whose status is 'full'??
                targets = []
                releases = []

                snapshot = snapshot_get_all_branches(storage, status.snapshot)
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
