from datetime import datetime
import logging
import os
from typing import Dict, Generator, Iterable, Optional, Set, Tuple

from typing_extensions import Literal, TypedDict

from swh.model.model import Sha1Git

from .model import DirectoryEntry, FileEntry, OriginEntry, RevisionEntry
from .provenance import (
    ProvenanceResult,
    ProvenanceStorageInterface,
    RelationData,
    RelationType,
)


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


# TODO: maybe move this to a separate file
class ProvenanceBackend:
    def __init__(self, storage: ProvenanceStorageInterface) -> None:
        self.storage = storage
        self.cache = new_cache()

    def clear_caches(self) -> None:
        self.cache = new_cache()

    def flush(self) -> None:
        # Revision-content layer insertions ############################################

        # For this layer, relations need to be inserted first so that, in case of
        # failure, reprocessing the input does not generated an inconsistent database.
        while not self.storage.relation_add(
            RelationType.CNT_EARLY_IN_REV,
            (
                RelationData(src=src, dst=dst, path=path)
                for src, dst, path in self.cache["content_in_revision"]
            ),
        ):
            logging.warning(
                f"Unable to write {RelationType.CNT_EARLY_IN_REV} rows to the storage. "
                f"Data: {self.cache['content_in_revision']}. Retrying..."
            )

        while not self.storage.relation_add(
            RelationType.CNT_IN_DIR,
            (
                RelationData(src=src, dst=dst, path=path)
                for src, dst, path in self.cache["content_in_directory"]
            ),
        ):
            logging.warning(
                f"Unable to write {RelationType.CNT_IN_DIR} rows to the storage. "
                f"Data: {self.cache['content_in_directory']}. Retrying..."
            )

        while not self.storage.relation_add(
            RelationType.DIR_IN_REV,
            (
                RelationData(src=src, dst=dst, path=path)
                for src, dst, path in self.cache["directory_in_revision"]
            ),
        ):
            logging.warning(
                f"Unable to write {RelationType.DIR_IN_REV} rows to the storage. "
                f"Data: {self.cache['directory_in_revision']}. Retrying..."
            )

        # After relations, dates for the entities can be safely set, acknowledging that
        # these entities won't need to be reprocessed in case of failure.
        dates = {
            sha1: date
            for sha1, date in self.cache["content"]["data"].items()
            if sha1 in self.cache["content"]["added"] and date is not None
        }
        while not self.storage.content_set_date(dates):
            logging.warning(
                f"Unable to write content dates to the storage. "
                f"Data: {dates}. Retrying..."
            )

        dates = {
            sha1: date
            for sha1, date in self.cache["directory"]["data"].items()
            if sha1 in self.cache["directory"]["added"] and date is not None
        }
        while not self.storage.directory_set_date(dates):
            logging.warning(
                f"Unable to write directory dates to the storage. "
                f"Data: {dates}. Retrying..."
            )

        dates = {
            sha1: date
            for sha1, date in self.cache["revision"]["data"].items()
            if sha1 in self.cache["revision"]["added"] and date is not None
        }
        while not self.storage.revision_set_date(dates):
            logging.warning(
                f"Unable to write revision dates to the storage. "
                f"Data: {dates}. Retrying..."
            )

        # Origin-revision layer insertions #############################################

        # Origins urls should be inserted first so that internal ids' resolution works
        # properly.
        urls = {
            sha1: date
            for sha1, date in self.cache["origin"]["data"].items()
            if sha1 in self.cache["origin"]["added"]
        }
        while not self.storage.origin_set_url(urls):
            logging.warning(
                f"Unable to write origins urls to the storage. "
                f"Data: {urls}. Retrying..."
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
        while not self.storage.relation_add(RelationType.REV_BEFORE_REV, data):
            logging.warning(
                f"Unable to write {RelationType.REV_BEFORE_REV} rows to the storage. "
                f"Data: {data}. Retrying..."
            )

        # Heads (ie. revision-in-origin entries) should be inserted once flat models for
        # their histories were already added. This is to guarantee consistent results if
        # something needs to be reprocessed due to a failure: already inserted heads
        # won't get reprocessed in such a case.
        data = (
            RelationData(src=rev, dst=org, path=None)
            for rev, org in self.cache["revision_in_origin"]
        )
        while not self.storage.relation_add(RelationType.REV_IN_ORG, data):
            logging.warning(
                f"Unable to write {RelationType.REV_IN_ORG} rows to the storage. "
                f"Data: {data}. Retrying..."
            )

        # Finally, preferred origins for the visited revisions are set (this step can be
        # reordered if required).
        origins = {
            sha1: self.cache["revision_origin"]["data"][sha1]
            for sha1 in self.cache["revision_origin"]["added"]
        }
        while not self.storage.revision_set_origin(origins):
            logging.warning(
                f"Unable to write preferred origins to the storage. "
                f"Data: {origins}. Retrying..."
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
                    if rev.date is not None
                }
            else:
                updated = getattr(self.storage, f"{entity}_get")(missing_ids)
            cache["data"].update(updated)
        dates: Dict[Sha1Git, datetime] = {}
        for sha1 in ids:
            date = cache["data"].get(sha1)
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
