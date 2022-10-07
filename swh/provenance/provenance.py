# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
import hashlib
import logging
import os
from types import TracebackType
from typing import Dict, Generator, Iterable, Optional, Set, Tuple, Type

from typing_extensions import Literal, TypedDict

from swh.core.statsd import statsd
from swh.model.model import Sha1Git

from .interface import ProvenanceInterface
from .model import DirectoryEntry, FileEntry, OriginEntry, RevisionEntry
from .storage.interface import (
    DirectoryData,
    ProvenanceResult,
    ProvenanceStorageInterface,
    RelationData,
    RelationType,
    RevisionData,
)
from .util import path_normalize

LOGGER = logging.getLogger(__name__)

BACKEND_DURATION_METRIC = "swh_provenance_backend_duration_seconds"
BACKEND_OPERATIONS_METRIC = "swh_provenance_backend_operations_total"


class DatetimeCache(TypedDict):
    data: Dict[Sha1Git, Optional[datetime]]  # None means unknown
    added: Set[Sha1Git]


class OriginCache(TypedDict):
    data: Dict[Sha1Git, str]
    added: Set[Sha1Git]


class RevisionCache(TypedDict):
    data: Dict[Sha1Git, Sha1Git]
    added: Set[Sha1Git]


class ProvenanceCache(TypedDict):
    content: DatetimeCache
    directory: DatetimeCache
    directory_flatten: Dict[Sha1Git, Optional[bool]]  # None means unknown
    revision: DatetimeCache
    # below are insertion caches only
    content_in_revision: Set[Tuple[Sha1Git, Sha1Git, bytes]]
    content_in_directory: Set[Tuple[Sha1Git, Sha1Git, bytes]]
    directory_in_revision: Set[Tuple[Sha1Git, Sha1Git, bytes]]
    # these two are for the origin layer
    origin: OriginCache
    revision_origin: RevisionCache
    revision_before_revision: Dict[Sha1Git, Set[Sha1Git]]
    revision_in_origin: Set[Tuple[Sha1Git, Sha1Git]]


def new_cache() -> ProvenanceCache:
    return ProvenanceCache(
        content=DatetimeCache(data={}, added=set()),
        directory=DatetimeCache(data={}, added=set()),
        directory_flatten={},
        revision=DatetimeCache(data={}, added=set()),
        content_in_revision=set(),
        content_in_directory=set(),
        directory_in_revision=set(),
        origin=OriginCache(data={}, added=set()),
        revision_origin=RevisionCache(data={}, added=set()),
        revision_before_revision={},
        revision_in_origin=set(),
    )


class Provenance:
    MAX_CACHE_ELEMENTS = 40000

    def __init__(self, storage: ProvenanceStorageInterface) -> None:
        self.storage = storage
        self.cache = new_cache()

    def __enter__(self) -> ProvenanceInterface:
        self.open()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def _flush_limit_reached(self) -> bool:
        return sum(self._get_cache_stats().values()) > self.MAX_CACHE_ELEMENTS

    def _get_cache_stats(self) -> Dict[str, int]:
        return {
            k: len(v["data"])
            if (isinstance(v, dict) and v.get("data") is not None)
            else len(v)  # type: ignore
            for (k, v) in self.cache.items()
        }

    def clear_caches(self) -> None:
        self.cache = new_cache()

    def close(self) -> None:
        self.storage.close()

    @statsd.timed(metric=BACKEND_DURATION_METRIC, tags={"method": "flush"})
    def flush(self) -> None:
        self.flush_revision_content_layer()
        self.flush_origin_revision_layer()
        self.clear_caches()

    def flush_if_necessary(self) -> bool:
        """Flush if the number of cached information reached a limit."""
        LOGGER.debug("Cache stats: %s", self._get_cache_stats())
        if self._flush_limit_reached():
            self.flush()
            return True
        else:
            return False

    @statsd.timed(
        metric=BACKEND_DURATION_METRIC, tags={"method": "flush_origin_revision"}
    )
    def flush_origin_revision_layer(self) -> None:
        # Origins and revisions should be inserted first so that internal ids'
        # resolution works properly.
        urls = {
            sha1: url
            for sha1, url in self.cache["origin"]["data"].items()
            if sha1 in self.cache["origin"]["added"]
        }
        if urls:
            while not self.storage.origin_add(urls):
                statsd.increment(
                    metric=BACKEND_OPERATIONS_METRIC,
                    tags={"method": "flush_origin_revision_retry_origin"},
                )
                LOGGER.warning(
                    "Unable to write origins urls to the storage. Retrying..."
                )

        rev_orgs = {
            # Destinations in this relation should match origins in the next one
            **{
                src: RevisionData(date=None, origin=None)
                for src in self.cache["revision_before_revision"]
            },
            **{
                # This relation comes second so that non-None origins take precedence
                src: RevisionData(date=None, origin=org)
                for src, org in self.cache["revision_in_origin"]
            },
        }
        if rev_orgs:
            while not self.storage.revision_add(rev_orgs):
                statsd.increment(
                    metric=BACKEND_OPERATIONS_METRIC,
                    tags={"method": "flush_origin_revision_retry_revision"},
                )
                LOGGER.warning(
                    "Unable to write revision entities to the storage. Retrying..."
                )

        # Second, flat models for revisions' histories (ie. revision-before-revision).
        if self.cache["revision_before_revision"]:
            rev_before_rev = {
                src: {RelationData(dst=dst, path=None) for dst in dsts}
                for src, dsts in self.cache["revision_before_revision"].items()
            }
            while not self.storage.relation_add(
                RelationType.REV_BEFORE_REV, rev_before_rev
            ):
                statsd.increment(
                    metric=BACKEND_OPERATIONS_METRIC,
                    tags={
                        "method": "flush_origin_revision_retry_revision_before_revision"
                    },
                )
                LOGGER.warning(
                    "Unable to write %s rows to the storage. Retrying...",
                    RelationType.REV_BEFORE_REV,
                )

        # Heads (ie. revision-in-origin entries) should be inserted once flat models for
        # their histories were already added. This is to guarantee consistent results if
        # something needs to be reprocessed due to a failure: already inserted heads
        # won't get reprocessed in such a case.
        if self.cache["revision_in_origin"]:
            rev_in_org: Dict[Sha1Git, Set[RelationData]] = {}
            for src, dst in self.cache["revision_in_origin"]:
                rev_in_org.setdefault(src, set()).add(RelationData(dst=dst, path=None))
            while not self.storage.relation_add(RelationType.REV_IN_ORG, rev_in_org):
                statsd.increment(
                    metric=BACKEND_OPERATIONS_METRIC,
                    tags={"method": "flush_origin_revision_retry_revision_in_origin"},
                )
                LOGGER.warning(
                    "Unable to write %s rows to the storage. Retrying...",
                    RelationType.REV_IN_ORG,
                )

    @statsd.timed(
        metric=BACKEND_DURATION_METRIC, tags={"method": "flush_revision_content"}
    )
    def flush_revision_content_layer(self) -> None:
        # Register in the storage all entities, to ensure the coming relations can
        # properly resolve any internal reference if needed. Content and directory
        # entries may safely be registered with their associated dates. In contrast,
        # revision entries should be registered without date, as it is used to
        # acknowledge that the flushing was successful. Also, directories are
        # registered with their flatten flag not set.
        cnt_dates = {
            sha1: date
            for sha1, date in self.cache["content"]["data"].items()
            if sha1 in self.cache["content"]["added"] and date is not None
        }
        if cnt_dates:
            while not self.storage.content_add(cnt_dates):
                statsd.increment(
                    metric=BACKEND_OPERATIONS_METRIC,
                    tags={"method": "flush_revision_content_retry_content_date"},
                )
                LOGGER.warning(
                    "Unable to write content dates to the storage. Retrying..."
                )

        dir_dates = {
            sha1: DirectoryData(date=date, flat=False)
            for sha1, date in self.cache["directory"]["data"].items()
            if sha1 in self.cache["directory"]["added"] and date is not None
        }
        if dir_dates:
            while not self.storage.directory_add(dir_dates):
                statsd.increment(
                    metric=BACKEND_OPERATIONS_METRIC,
                    tags={"method": "flush_revision_content_retry_directory_date"},
                )
                LOGGER.warning(
                    "Unable to write directory dates to the storage. Retrying..."
                )

        revs = {
            sha1: RevisionData(date=None, origin=None)
            for sha1, date in self.cache["revision"]["data"].items()
            if sha1 in self.cache["revision"]["added"] and date is not None
        }
        if revs:
            while not self.storage.revision_add(revs):
                statsd.increment(
                    metric=BACKEND_OPERATIONS_METRIC,
                    tags={"method": "flush_revision_content_retry_revision_none"},
                )
                LOGGER.warning(
                    "Unable to write revision entities to the storage. Retrying..."
                )

        paths = {
            hashlib.sha1(path).digest(): path
            for _, _, path in self.cache["content_in_revision"]
            | self.cache["content_in_directory"]
            | self.cache["directory_in_revision"]
        }
        if paths:
            while not self.storage.location_add(paths):
                statsd.increment(
                    metric=BACKEND_OPERATIONS_METRIC,
                    tags={"method": "flush_revision_content_retry_location"},
                )
                LOGGER.warning(
                    "Unable to write locations entities to the storage. Retrying..."
                )

        # For this layer, relations need to be inserted first so that, in case of
        # failure, reprocessing the input does not generated an inconsistent database.
        if self.cache["content_in_revision"]:
            cnt_in_rev: Dict[Sha1Git, Set[RelationData]] = {}
            for src, dst, path in self.cache["content_in_revision"]:
                cnt_in_rev.setdefault(src, set()).add(RelationData(dst=dst, path=path))
            while not self.storage.relation_add(
                RelationType.CNT_EARLY_IN_REV, cnt_in_rev
            ):
                statsd.increment(
                    metric=BACKEND_OPERATIONS_METRIC,
                    tags={"method": "flush_revision_content_retry_content_in_revision"},
                )
                LOGGER.warning(
                    "Unable to write %s rows to the storage. Retrying...",
                    RelationType.CNT_EARLY_IN_REV,
                )

        if self.cache["content_in_directory"]:
            cnt_in_dir: Dict[Sha1Git, Set[RelationData]] = {}
            for src, dst, path in self.cache["content_in_directory"]:
                cnt_in_dir.setdefault(src, set()).add(RelationData(dst=dst, path=path))
            while not self.storage.relation_add(RelationType.CNT_IN_DIR, cnt_in_dir):
                statsd.increment(
                    metric=BACKEND_OPERATIONS_METRIC,
                    tags={
                        "method": "flush_revision_content_retry_content_in_directory"
                    },
                )
                LOGGER.warning(
                    "Unable to write %s rows to the storage. Retrying...",
                    RelationType.CNT_IN_DIR,
                )

        if self.cache["directory_in_revision"]:
            dir_in_rev: Dict[Sha1Git, Set[RelationData]] = {}
            for src, dst, path in self.cache["directory_in_revision"]:
                dir_in_rev.setdefault(src, set()).add(RelationData(dst=dst, path=path))
            while not self.storage.relation_add(RelationType.DIR_IN_REV, dir_in_rev):
                statsd.increment(
                    metric=BACKEND_OPERATIONS_METRIC,
                    tags={
                        "method": "flush_revision_content_retry_directory_in_revision"
                    },
                )
                LOGGER.warning(
                    "Unable to write %s rows to the storage. Retrying...",
                    RelationType.DIR_IN_REV,
                )

        # After relations, flatten flags for directories can be safely set (if
        # applicable) acknowledging those directories that have already be flattened.
        # Similarly, dates for the revisions are set to acknowledge that these revisions
        # won't need to be reprocessed in case of failure.
        dir_acks = {
            sha1: DirectoryData(
                date=date, flat=self.cache["directory_flatten"].get(sha1) or False
            )
            for sha1, date in self.cache["directory"]["data"].items()
            if self.cache["directory_flatten"].get(sha1) and date is not None
        }
        if dir_acks:
            while not self.storage.directory_add(dir_acks):
                statsd.increment(
                    metric=BACKEND_OPERATIONS_METRIC,
                    tags={"method": "flush_revision_content_retry_directory_ack"},
                )
                LOGGER.warning(
                    "Unable to write directory dates to the storage. Retrying..."
                )

        rev_dates = {
            sha1: RevisionData(date=date, origin=None)
            for sha1, date in self.cache["revision"]["data"].items()
            if sha1 in self.cache["revision"]["added"] and date is not None
        }
        if rev_dates:
            while not self.storage.revision_add(rev_dates):
                statsd.increment(
                    metric=BACKEND_OPERATIONS_METRIC,
                    tags={"method": "flush_revision_content_retry_revision_date"},
                )
                LOGGER.warning(
                    "Unable to write revision dates to the storage. Retrying..."
                )

    def content_add_to_directory(
        self, directory: DirectoryEntry, blob: FileEntry, prefix: bytes
    ) -> None:
        self.cache["content_in_directory"].add(
            (blob.id, directory.id, path_normalize(os.path.join(prefix, blob.name)))
        )

    def content_add_to_revision(
        self, revision: RevisionEntry, blob: FileEntry, prefix: bytes
    ) -> None:
        self.cache["content_in_revision"].add(
            (blob.id, revision.id, path_normalize(os.path.join(prefix, blob.name)))
        )

    def content_find_first(self, id: Sha1Git) -> Optional[ProvenanceResult]:
        return self.storage.content_find_first(id)

    def content_find_all(
        self, id: Sha1Git, limit: Optional[int] = None
    ) -> Generator[ProvenanceResult, None, None]:
        yield from self.storage.content_find_all(id, limit=limit)

    def content_get_early_date(self, blob: FileEntry) -> Optional[datetime]:
        return self.get_dates("content", [blob.id]).get(blob.id)

    def content_get_early_dates(
        self, blobs: Iterable[FileEntry]
    ) -> Dict[Sha1Git, datetime]:
        return self.get_dates("content", [blob.id for blob in blobs])

    def content_set_early_date(self, blob: FileEntry, date: datetime) -> None:
        self.cache["content"]["data"][blob.id] = date
        self.cache["content"]["added"].add(blob.id)

    def directory_add_to_revision(
        self, revision: RevisionEntry, directory: DirectoryEntry, path: bytes
    ) -> None:
        self.cache["directory_in_revision"].add(
            (directory.id, revision.id, path_normalize(path))
        )

    def directory_already_flattenned(self, directory: DirectoryEntry) -> Optional[bool]:
        cache = self.cache["directory_flatten"]
        if directory.id not in cache:
            cache.setdefault(directory.id, None)
            ret = self.storage.directory_get([directory.id])
            if directory.id in ret:
                dir = ret[directory.id]
                cache[directory.id] = dir.flat
                # date is kept to ensure we have it available when flushing
                self.cache["directory"]["data"][directory.id] = dir.date
        return cache.get(directory.id)

    def directory_flag_as_flattenned(self, directory: DirectoryEntry) -> None:
        self.cache["directory_flatten"][directory.id] = True

    def directory_get_date_in_isochrone_frontier(
        self, directory: DirectoryEntry
    ) -> Optional[datetime]:
        return self.get_dates("directory", [directory.id]).get(directory.id)

    def directory_get_dates_in_isochrone_frontier(
        self, dirs: Iterable[DirectoryEntry]
    ) -> Dict[Sha1Git, datetime]:
        return self.get_dates("directory", [directory.id for directory in dirs])

    def directory_set_date_in_isochrone_frontier(
        self, directory: DirectoryEntry, date: datetime
    ) -> None:
        self.cache["directory"]["data"][directory.id] = date
        self.cache["directory"]["added"].add(directory.id)

    def get_dates(
        self,
        entity: Literal["content", "directory", "revision"],
        ids: Iterable[Sha1Git],
    ) -> Dict[Sha1Git, datetime]:
        cache = self.cache[entity]
        missing_ids = set(id for id in ids if id not in cache)
        if missing_ids:
            if entity == "content":
                cache["data"].update(self.storage.content_get(missing_ids))
            elif entity == "directory":
                cache["data"].update(
                    {
                        id: dir.date
                        for id, dir in self.storage.directory_get(missing_ids).items()
                    }
                )
            elif entity == "revision":
                cache["data"].update(
                    {
                        id: rev.date
                        for id, rev in self.storage.revision_get(missing_ids).items()
                    }
                )
        dates: Dict[Sha1Git, datetime] = {}
        for sha1 in ids:
            date = cache["data"].setdefault(sha1, None)
            if date is not None:
                dates[sha1] = date
        return dates

    def open(self) -> None:
        self.storage.open()

    def origin_add(self, origin: OriginEntry) -> None:
        self.cache["origin"]["data"][origin.id] = origin.url
        self.cache["origin"]["added"].add(origin.id)

    def revision_add(self, revision: RevisionEntry) -> None:
        self.cache["revision"]["data"][revision.id] = revision.date
        self.cache["revision"]["added"].add(revision.id)

    def revision_add_before_revision(
        self, head_id: Sha1Git, revision_id: Sha1Git
    ) -> None:
        self.cache["revision_before_revision"].setdefault(revision_id, set()).add(
            head_id
        )

    def revision_add_to_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ) -> None:
        self.cache["revision_in_origin"].add((revision.id, origin.id))

    def revision_is_head(self, revision: RevisionEntry) -> bool:
        return bool(self.storage.relation_get(RelationType.REV_IN_ORG, [revision.id]))

    def revision_get_date(self, revision: RevisionEntry) -> Optional[datetime]:
        return self.get_dates("revision", [revision.id]).get(revision.id)

    def revision_get_preferred_origin(self, revision_id: Sha1Git) -> Optional[Sha1Git]:
        cache = self.cache["revision_origin"]["data"]
        if revision_id not in cache:
            ret = self.storage.revision_get([revision_id])
            if revision_id in ret:
                origin = ret[revision_id].origin
                if origin is not None:
                    cache[revision_id] = origin
        return cache.get(revision_id)

    def revision_set_preferred_origin(
        self, origin: OriginEntry, revision_id: Sha1Git
    ) -> None:
        self.cache["revision_origin"]["data"][revision_id] = origin.id
        self.cache["revision_origin"]["added"].add(revision_id)
