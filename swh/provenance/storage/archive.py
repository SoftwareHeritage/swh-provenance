from typing import Any, Dict, Iterable, Set

from swh.model.model import ObjectType, Revision, Sha1, TargetType
from swh.storage.interface import StorageInterface


class ArchiveStorage:
    def __init__(self, storage: StorageInterface):
        self.storage = storage

    def directory_ls(self, id: Sha1) -> Iterable[Dict[str, Any]]:
        # TODO: filter unused fields
        yield from self.storage.directory_ls(id)

    def revision_get(self, ids: Iterable[Sha1]) -> Iterable[Revision]:
        # TODO: filter unused fields
        yield from (
            rev for rev in self.storage.revision_get(list(ids)) if rev is not None
        )

    def snapshot_get_heads(self, id: Sha1) -> Iterable[Sha1]:
        from swh.core.utils import grouper
        from swh.storage.algos.snapshot import snapshot_get_all_branches

        snapshot = snapshot_get_all_branches(self.storage, id)
        assert snapshot is not None

        targets_set = set()
        releases_set = set()
        if snapshot is not None:
            for branch in snapshot.branches:
                if snapshot.branches[branch].target_type == TargetType.REVISION:
                    targets_set.add(snapshot.branches[branch].target)
                elif snapshot.branches[branch].target_type == TargetType.RELEASE:
                    releases_set.add(snapshot.branches[branch].target)

        batchsize = 100
        for releases in grouper(releases_set, batchsize):
            targets_set.update(
                release.target
                for release in self.storage.release_get(list(releases))
                if release is not None and release.target_type == ObjectType.REVISION
            )

        revisions: Set[Sha1] = set()
        for targets in grouper(targets_set, batchsize):
            revisions.update(
                revision.id
                for revision in self.storage.revision_get(list(targets))
                if revision is not None
            )

        yield from revisions
