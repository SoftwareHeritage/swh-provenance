# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import Any, Dict, Iterable, Iterator, List, Tuple

from swh.core.statsd import statsd
from swh.model.model import Directory, Sha1Git
from swh.provenance.archive import ArchiveInterface
from swh.storage.interface import StorageInterface

ARCHIVE_DURATION_METRIC = "swh_provenance_archive_multiplexed_duration_seconds"
ARCHIVE_OPS_METRIC = "swh_provenance_archive_multiplexed_per_backend_count"

LOGGER = logging.getLogger(__name__)

EMPTY_DIR_ID = Directory(entries=()).id


class ArchiveMultiplexed:
    storage: StorageInterface

    def __init__(self, archives: List[Tuple[str, ArchiveInterface]]) -> None:
        self.archives = archives

    @statsd.timed(metric=ARCHIVE_DURATION_METRIC, tags={"method": "directory_ls"})
    def directory_ls(self, id: Sha1Git, minsize: int = 0) -> Iterable[Dict[str, Any]]:
        if id == EMPTY_DIR_ID:
            return []

        for backend, archive in self.archives:
            try:
                entries = list(archive.directory_ls(id, minsize=minsize))
            except NotImplementedError:
                continue

            if entries:
                statsd.increment(
                    ARCHIVE_OPS_METRIC,
                    tags={"method": "directory_ls", "backend": backend},
                )
                return entries

        statsd.increment(
            ARCHIVE_OPS_METRIC,
            tags={"method": "directory_ls", "backend": "empty_or_not_found"},
        )
        LOGGER.debug("directory empty (only rev entries) or not found: %s", id.hex())

        return []

    @statsd.timed(
        metric=ARCHIVE_DURATION_METRIC,
        tags={"method": "revision_get_some_outbound_edges"},
    )
    def revision_get_some_outbound_edges(
        self, revision_id: Sha1Git
    ) -> Iterable[Tuple[Sha1Git, Sha1Git]]:
        # TODO: what if the revision doesn't exist in the archive?
        for backend, archive in self.archives:
            try:
                edges = list(archive.revision_get_some_outbound_edges(revision_id))
                if edges:
                    statsd.increment(
                        ARCHIVE_OPS_METRIC,
                        tags={
                            "method": "revision_get_some_outbound_edges",
                            "backend": backend,
                        },
                    )
                    return edges
                LOGGER.debug(
                    "No outbound edges found for revision %s via %s",
                    revision_id.hex(),
                    archive.__class__,
                )
            except Exception as e:
                LOGGER.warn(
                    "Error retrieving outbound edges of revision %s via %s: %s",
                    revision_id.hex(),
                    archive.__class__,
                    e,
                )
        statsd.increment(
            ARCHIVE_OPS_METRIC,
            tags={
                "method": "revision_get_some_outbound_edges",
                "backend": "no_parents_or_not_found",
            },
        )

        return []

    @statsd.timed(
        metric=ARCHIVE_DURATION_METRIC,
        tags={"method": "revisions_get"},
    )
    def revisions_get(
        self, revision_ids: Iterable[Sha1Git]
    ) -> Iterator[Tuple[Sha1Git, Sha1Git, Dict[str, Any]]]:
        revision_ids = list(revision_ids)  # this will be iterated several times
        for backend, archive in self.archives:
            try:
                revs = list(archive.revisions_get(revision_ids))
                if revs:
                    statsd.increment(
                        ARCHIVE_OPS_METRIC,
                        tags={
                            "method": "revisions_get",
                            "backend": backend,
                        },
                    )
                    yield from revs
                LOGGER.debug(
                    "No revs found via %s",
                    archive.__class__,
                )
            except Exception as e:
                LOGGER.warn(
                    "Error retrieving revisions via %s: %s",
                    archive.__class__,
                    e,
                )
        statsd.increment(
            ARCHIVE_OPS_METRIC,
            tags={
                "method": "revisions_get",
                "backend": "not_found",
            },
        )

    @statsd.timed(metric=ARCHIVE_DURATION_METRIC, tags={"method": "snapshot_get_heads"})
    def snapshot_get_heads(self, id: Sha1Git) -> Iterable[Sha1Git]:
        for backend, archive in self.archives:

            try:
                heads = list(archive.snapshot_get_heads(id))

                if heads:
                    statsd.increment(
                        ARCHIVE_OPS_METRIC,
                        tags={"method": "snapshot_get_heads", "backend": backend},
                    )
                    return heads
                LOGGER.debug(
                    "No heads found for snapshot %s via %s", str(id), archive.__class__
                )
            except Exception as e:
                LOGGER.warn(
                    "Error retrieving heads of snapshots %s via %s: %s",
                    id.hex(),
                    archive.__class__,
                    e,
                )

        statsd.increment(
            ARCHIVE_OPS_METRIC,
            tags={"method": "snapshot_get_heads", "backend": "no_heads_or_not_found"},
        )
        return []
