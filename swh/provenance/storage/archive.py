# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
from typing import Any, Dict, Iterable, Set, Tuple

from swh.core.statsd import statsd
from swh.model.model import ObjectType, Sha1Git, TargetType
from swh.storage.interface import StorageInterface

ARCHIVE_DURATION_METRIC = "swh_provenance_archive_api_duration_seconds"


class ArchiveStorage:
    def __init__(self, storage: StorageInterface) -> None:
        self.storage = storage

    @statsd.timed(metric=ARCHIVE_DURATION_METRIC, tags={"method": "directory_ls"})
    def directory_ls(self, id: Sha1Git) -> Iterable[Dict[str, Any]]:
        # TODO: add file size filtering
        for entry in self.storage.directory_ls(id):
            yield {
                "name": entry["name"],
                "target": entry["target"],
                "type": entry["type"],
            }

    @statsd.timed(
        metric=ARCHIVE_DURATION_METRIC, tags={"method": "revision_get_parents"}
    )
    def revision_get_parents(self, id: Sha1Git) -> Iterable[Sha1Git]:
        rev = self.storage.revision_get([id])[0]
        if rev is not None:
            yield from rev.parents

    @statsd.timed(metric=ARCHIVE_DURATION_METRIC, tags={"method": "snapshot_get_heads"})
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

        revisions: Set[Tuple[datetime, Sha1Git]] = set()
        for targets in grouper(targets_set, batchsize):
            revisions.update(
                (revision.date.to_datetime(), revision.id)
                for revision in self.storage.revision_get(list(targets))
                if revision is not None and revision.date is not None
            )

        yield from (head for _, head in sorted(revisions))
