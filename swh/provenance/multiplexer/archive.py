# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import Any, Dict, Iterable, List

from swh.core.statsd import statsd
from swh.model.model import Sha1Git
from swh.provenance.archive import ArchiveInterface
from swh.storage.interface import StorageInterface

ARCHIVE_DURATION_METRIC = "swh_provenance_archive_multiplexed_duration_seconds"

LOGGER = logging.getLogger(__name__)


class ArchiveMultiplexed:
    storage: StorageInterface

    def __init__(self, archives: List[ArchiveInterface]) -> None:
        self.archives = archives

    @statsd.timed(metric=ARCHIVE_DURATION_METRIC, tags={"method": "directory_ls"})
    def directory_ls(self, id: Sha1Git, minsize: int = 0) -> Iterable[Dict[str, Any]]:
        directories = None
        for archive in self.archives:
            try:
                directories = list(archive.directory_ls(id))
            except NotImplementedError:
                pass

            if directories:
                return directories
            LOGGER.debug(
                "No parents found for revision %s via %s", id.hex(), archive.__class__
            )

        return []

    @statsd.timed(
        metric=ARCHIVE_DURATION_METRIC, tags={"method": "revision_get_parents"}
    )
    def revision_get_parents(self, id: Sha1Git) -> Iterable[Sha1Git]:

        for archive in self.archives:
            try:
                parents = list(archive.revision_get_parents(id))
                if parents:
                    return parents
                LOGGER.debug(
                    "No parents found for revision %s via %s",
                    id.hex(),
                    archive.__class__,
                )
            except Exception as e:
                LOGGER.warn(
                    "Error retrieving parents of revision %s via %s: %s",
                    id.hex(),
                    archive.__class__,
                    e,
                )

        return []

    @statsd.timed(metric=ARCHIVE_DURATION_METRIC, tags={"method": "snapshot_get_heads"})
    def snapshot_get_heads(self, id: Sha1Git) -> Iterable[Sha1Git]:
        for archive in self.archives:

            try:
                heads = list(archive.snapshot_get_heads(id))

                if heads:
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

        return []
