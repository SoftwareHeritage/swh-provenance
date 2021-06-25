from typing import Any, Dict, Iterable, Set

from swh.model.model import ObjectType, Sha1Git, TargetType
from swh.storage.interface import StorageInterface


class ArchiveStorage:
    def __init__(self, storage: StorageInterface):
        self.storage = storage

    def directory_ls(self, id: Sha1Git) -> Iterable[Dict[str, Any]]:
        # TODO: add file size filtering
        for entry in self.storage.directory_ls(id):
            yield {
                "name": entry["name"],
                "target": entry["target"],
                "type": entry["type"],
            }

    def revision_get_parents(self, id: Sha1Git) -> Iterable[Sha1Git]:
        rev = self.storage.revision_get([id])[0]
        if rev is not None:
            yield from rev.parents

    def snapshot_get_heads(self, id: Sha1Git) -> Iterable[Sha1Git]:
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

        revisions: Set[Sha1Git] = set()
        for targets in grouper(targets_set, batchsize):
            revisions.update(
                revision.id
                for revision in self.storage.revision_get(list(targets))
                if revision is not None
            )

        yield from revisions
