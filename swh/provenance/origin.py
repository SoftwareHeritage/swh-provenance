from typing import Optional

from swh.model.model import ObjectType, Origin, TargetType

from .archive import ArchiveInterface
from .model import OriginEntry, RevisionEntry

################################################################################
################################################################################


class FileOriginIterator:
    """Iterator over origins present in the given CSV file."""

    def __init__(
        self, filename: str, archive: ArchiveInterface, limit: Optional[int] = None
    ):
        self.file = open(filename)
        self.limit = limit
        self.archive = archive

    def __iter__(self):
        yield from iterate_statuses(
            [Origin(url.strip()) for url in self.file], self.archive, self.limit
        )


class ArchiveOriginIterator:
    """Iterator over origins present in the given storage."""

    def __init__(self, archive: ArchiveInterface, limit: Optional[int] = None):
        self.limit = limit
        self.archive = archive

    def __iter__(self):
        yield from iterate_statuses(
            self.archive.iter_origins(), self.archive, self.limit
        )


def iterate_statuses(origins, archive: ArchiveInterface, limit: Optional[int] = None):
    idx = 0
    for origin in origins:
        for visit in archive.iter_origin_visits(origin.url):
            for status in archive.iter_origin_visit_statuses(origin.url, visit.visit):
                snapshot = archive.snapshot_get_all_branches(status.snapshot)
                if snapshot is None:
                    continue
                # TODO: may filter only those whose status is 'full'??
                targets_set = set()
                releases_set = set()
                if snapshot is not None:
                    for branch in snapshot.branches:
                        if snapshot.branches[branch].target_type == TargetType.REVISION:
                            targets_set.add(snapshot.branches[branch].target)
                        elif (
                            snapshot.branches[branch].target_type == TargetType.RELEASE
                        ):
                            releases_set.add(snapshot.branches[branch].target)

                # This is done to keep the query in release_get small, hence avoiding
                # a timeout.
                batchsize = 100
                while releases_set:
                    releases = [
                        releases_set.pop() for i in range(batchsize) if releases_set
                    ]
                    for release in archive.release_get(releases):
                        if release is not None:
                            if release.target_type == ObjectType.REVISION:
                                targets_set.add(release.target)

                # This is done to keep the query in revision_get small, hence avoiding
                # a timeout.
                revisions = set()
                while targets_set:
                    targets = [
                        targets_set.pop() for i in range(batchsize) if targets_set
                    ]
                    for revision in archive.revision_get(targets):
                        if revision is not None:
                            revisions.add(RevisionEntry(revision.id))
                            # target_set |= set(revision.parents)

                yield OriginEntry(status.origin, list(revisions))

                idx += 1
                if idx == limit:
                    return
