# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, Set, Tuple

from swh.core.statsd import statsd
from swh.model.model import ObjectType, Sha1Git, TargetType
from swh.storage.interface import StorageInterface

ARCHIVE_DURATION_METRIC = "swh_provenance_archive_api_duration_seconds"


class ArchiveStorage:
    def __init__(self, storage: StorageInterface) -> None:
        self.storage = storage

    @statsd.timed(metric=ARCHIVE_DURATION_METRIC, tags={"method": "directory_ls"})
    def directory_ls(self, id: Sha1Git, minsize: int = 0) -> Iterable[Dict[str, Any]]:
        for entry in self.storage.directory_ls(id):
            if entry["type"] == "dir" or (
                entry["type"] == "file" and entry["length"] >= minsize
            ):
                yield {
                    "name": entry["name"],
                    "target": entry["target"],
                    "type": entry["type"],
                }

    @statsd.timed(
        metric=ARCHIVE_DURATION_METRIC,
        tags={"method": "revision_get_some_outbound_edges"},
    )
    def revision_get_some_outbound_edges(
        self, revision_id: Sha1Git
    ) -> Iterable[Tuple[Sha1Git, Sha1Git]]:
        rev = self.storage.revision_get([revision_id])[0]
        if rev is not None:
            for parent_id in rev.parents:
                yield (revision_id, parent_id)

    @statsd.timed(
        metric=ARCHIVE_DURATION_METRIC,
        tags={"method": "revisions_get"},
    )
    def revisions_get(
        self, revision_ids: Iterable[Sha1Git]
    ) -> Iterator[Tuple[Sha1Git, Sha1Git, Dict[str, Any]]]:
        revs = self.storage.revision_get(list(revision_ids))
        for rev in revs:
            if rev is not None and rev.date is not None:
                yield (rev.id, rev.directory, rev.date.to_dict())

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

        yield from (head for _, head in revisions)
