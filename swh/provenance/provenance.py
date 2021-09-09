# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
import logging
import os
from typing import Dict, Generator, Iterable, Optional, Set, Tuple

from typing_extensions import Literal, TypedDict

from swh.model.model import Sha1Git

from .interface import (
    ProvenanceResult,
    ProvenanceStorageInterface,
    RelationData,
    RelationType,
    RevisionData,
)
from .model import DirectoryEntry, FileEntry, OriginEntry, RevisionEntry

LOGGER = logging.getLogger(__name__)


class DatetimeCache(TypedDict):
    data: Dict[Sha1Git, Optional[datetime]]
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
    def __init__(self, storage: ProvenanceStorageInterface) -> None:
        self.storage = storage
        self.cache = new_cache()

    def clear_caches(self) -> None:
        self.cache = new_cache()

    def flush(self) -> None:
        # Revision-content layer insertions ############################################

        # After relations, dates for the entities can be safely set, acknowledging that
        # these entities won't need to be reprocessed in case of failure.
        cnts = {
            src
            for src, _, _ in self.cache["content_in_revision"]
            | self.cache["content_in_directory"]
        }
        if cnts:
            while not self.storage.content_add(cnts):
                LOGGER.warning(
                    "Unable to write content entities to the storage. Retrying..."
                )

        dirs = {dst for _, dst, _ in self.cache["content_in_directory"]}
        if dirs:
            while not self.storage.directory_add(dirs):
                LOGGER.warning(
                    "Unable to write directory entities to the storage. Retrying..."
                )

        revs = {
            dst
            for _, dst, _ in self.cache["content_in_revision"]
            | self.cache["directory_in_revision"]
        }
        if revs:
            while not self.storage.revision_add(revs):
                LOGGER.warning(
                    "Unable to write revision entities to the storage. Retrying..."
                )

        paths = {
            path
            for _, _, path in self.cache["content_in_revision"]
            | self.cache["content_in_directory"]
            | self.cache["directory_in_revision"]
        }
        if paths:
            while not self.storage.location_add(paths):
                LOGGER.warning(
                    "Unable to write locations entities to the storage. Retrying..."
                )

        # For this layer, relations need to be inserted first so that, in case of
        # failure, reprocessing the input does not generated an inconsistent database.
        if self.cache["content_in_revision"]:
            while not self.storage.relation_add(
                RelationType.CNT_EARLY_IN_REV,
                (
                    RelationData(src=src, dst=dst, path=path)
                    for src, dst, path in self.cache["content_in_revision"]
                ),
            ):
                LOGGER.warning(
                    "Unable to write %s rows to the storage. Retrying...",
                    RelationType.CNT_EARLY_IN_REV,
                )

        if self.cache["content_in_directory"]:
            while not self.storage.relation_add(
                RelationType.CNT_IN_DIR,
                (
                    RelationData(src=src, dst=dst, path=path)
                    for src, dst, path in self.cache["content_in_directory"]
                ),
            ):
                LOGGER.warning(
                    "Unable to write %s rows to the storage. Retrying...",
                    RelationType.CNT_IN_DIR,
                )

        if self.cache["directory_in_revision"]:
            while not self.storage.relation_add(
                RelationType.DIR_IN_REV,
                (
                    RelationData(src=src, dst=dst, path=path)
                    for src, dst, path in self.cache["directory_in_revision"]
                ),
            ):
                LOGGER.warning(
                    "Unable to write %s rows to the storage. Retrying...",
                    RelationType.DIR_IN_REV,
                )

        # After relations, dates for the entities can be safely set, acknowledging that
        # these entities won't need to be reprocessed in case of failure.
        cnt_dates = {
            sha1: date
            for sha1, date in self.cache["content"]["data"].items()
            if sha1 in self.cache["content"]["added"] and date is not None
        }
        if cnt_dates:
            while not self.storage.content_add(cnt_dates):
                LOGGER.warning(
                    "Unable to write content dates to the storage. Retrying..."
                )

        dir_dates = {
            sha1: date
            for sha1, date in self.cache["directory"]["data"].items()
            if sha1 in self.cache["directory"]["added"] and date is not None
        }
        if dir_dates:
            while not self.storage.directory_add(dir_dates):
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
                LOGGER.warning(
                    "Unable to write revision dates to the storage. Retrying..."
                )

        # Origin-revision layer insertions #############################################

        # Origins and revisions should be inserted first so that internal ids'
        # resolution works properly.
        urls = {
            sha1: url
            for sha1, url in self.cache["origin"]["data"].items()
            if sha1 in self.cache["origin"]["added"]
        }
        if urls:
            while not self.storage.origin_add(urls):
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
                LOGGER.warning(
                    "Unable to write revision entities to the storage. Retrying..."
                )

        # Second, flat models for revisions' histories (ie. revision-before-revision).
        data: Iterable[RelationData] = sum(
            [
                [
                    RelationData(src=prev, dst=next, path=None)
                    for next in self.cache["revision_before_revision"][prev]
                ]
                for prev in self.cache["revision_before_revision"]
            ],
            [],
        )
        if data:
            while not self.storage.relation_add(RelationType.REV_BEFORE_REV, data):
                LOGGER.warning(
                    "Unable to write %s rows to the storage. Retrying...",
                    RelationType.REV_BEFORE_REV,
                )

        # Heads (ie. revision-in-origin entries) should be inserted once flat models for
        # their histories were already added. This is to guarantee consistent results if
        # something needs to be reprocessed due to a failure: already inserted heads
        # won't get reprocessed in such a case.
        data = (
            RelationData(src=rev, dst=org, path=None)
            for rev, org in self.cache["revision_in_origin"]
        )
        if data:
            while not self.storage.relation_add(RelationType.REV_IN_ORG, data):
                LOGGER.warning(
                    "Unable to write %s rows to the storage. Retrying...",
                    RelationType.REV_IN_ORG,
                )

        # clear local cache ############################################################
        self.clear_caches()

    def content_add_to_directory(
        self, directory: DirectoryEntry, blob: FileEntry, prefix: bytes
    ) -> None:
        self.cache["content_in_directory"].add(
            (blob.id, directory.id, normalize(os.path.join(prefix, blob.name)))
        )

    def content_add_to_revision(
        self, revision: RevisionEntry, blob: FileEntry, prefix: bytes
    ) -> None:
        self.cache["content_in_revision"].add(
            (blob.id, revision.id, normalize(os.path.join(prefix, blob.name)))
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
            (directory.id, revision.id, normalize(path))
        )

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
            if entity == "revision":
                updated = {
                    id: rev.date
                    for id, rev in self.storage.revision_get(missing_ids).items()
                }
            else:
                updated = getattr(self.storage, f"{entity}_get")(missing_ids)
            cache["data"].update(updated)
        dates: Dict[Sha1Git, datetime] = {}
        for sha1 in ids:
            date = cache["data"].setdefault(sha1, None)
            if date is not None:
                dates[sha1] = date
        return dates

    def origin_add(self, origin: OriginEntry) -> None:
        self.cache["origin"]["data"][origin.id] = origin.url
        self.cache["origin"]["added"].add(origin.id)

    def revision_add(self, revision: RevisionEntry) -> None:
        self.cache["revision"]["data"][revision.id] = revision.date
        self.cache["revision"]["added"].add(revision.id)

    def revision_add_before_revision(
        self, head: RevisionEntry, revision: RevisionEntry
    ) -> None:
        self.cache["revision_before_revision"].setdefault(revision.id, set()).add(
            head.id
        )

    def revision_add_to_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ) -> None:
        self.cache["revision_in_origin"].add((revision.id, origin.id))

    def revision_get_date(self, revision: RevisionEntry) -> Optional[datetime]:
        return self.get_dates("revision", [revision.id]).get(revision.id)

    def revision_get_preferred_origin(
        self, revision: RevisionEntry
    ) -> Optional[Sha1Git]:
        cache = self.cache["revision_origin"]["data"]
        if revision.id not in cache:
            ret = self.storage.revision_get([revision.id])
            if revision.id in ret:
                origin = ret[revision.id].origin
                if origin is not None:
                    cache[revision.id] = origin
        return cache.get(revision.id)

    def revision_in_history(self, revision: RevisionEntry) -> bool:
        return revision.id in self.cache["revision_before_revision"] or bool(
            self.storage.relation_get(RelationType.REV_BEFORE_REV, [revision.id])
        )

    def revision_set_preferred_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ) -> None:
        self.cache["revision_origin"]["data"][revision.id] = origin.id
        self.cache["revision_origin"]["added"].add(revision.id)

    def revision_visited(self, revision: RevisionEntry) -> bool:
        return revision.id in dict(self.cache["revision_in_origin"]) or bool(
            self.storage.relation_get(RelationType.REV_IN_ORG, [revision.id])
        )


def normalize(path: bytes) -> bytes:
    return path[2:] if path.startswith(bytes("." + os.path.sep, "utf-8")) else path
