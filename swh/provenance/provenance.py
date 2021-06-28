from datetime import datetime
import logging
import os
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple

import psycopg2
from typing_extensions import Literal, Protocol, TypedDict, runtime_checkable

from swh.model.model import Sha1Git

from .model import DirectoryEntry, FileEntry, OriginEntry, RevisionEntry


class ProvenanceResult:
    def __init__(
        self,
        content: Sha1Git,
        revision: Sha1Git,
        date: datetime,
        origin: Optional[str],
        path: bytes,
    ) -> None:
        self.content = content
        self.revision = revision
        self.date = date
        self.origin = origin
        self.path = path


@runtime_checkable
class ProvenanceInterface(Protocol):
    raise_on_commit: bool = False

    def flush(self) -> None:
        """Flush internal cache to the underlying `storage`."""
        ...

    def content_add_to_directory(
        self, directory: DirectoryEntry, blob: FileEntry, prefix: bytes
    ) -> None:
        """Associate `blob` with `directory` in the provenance model. `prefix` is the
        relative path from `directory` to `blob` (excluding `blob`'s name).
        """
        ...

    def content_add_to_revision(
        self, revision: RevisionEntry, blob: FileEntry, prefix: bytes
    ) -> None:
        """Associate `blob` with `revision` in the provenance model. `prefix` is the
        absolute path from `revision`'s root directory to `blob` (excluding `blob`'s
        name).
        """
        ...

    def content_find_first(self, id: Sha1Git) -> Optional[ProvenanceResult]:
        """Retrieve the first occurrence of the blob identified by `id`."""
        ...

    def content_find_all(
        self, id: Sha1Git, limit: Optional[int] = None
    ) -> Generator[ProvenanceResult, None, None]:
        """Retrieve all the occurrences of the blob identified by `id`."""
        ...

    def content_get_early_date(self, blob: FileEntry) -> Optional[datetime]:
        """Retrieve the earliest known date of `blob`."""
        ...

    def content_get_early_dates(
        self, blobs: Iterable[FileEntry]
    ) -> Dict[Sha1Git, datetime]:
        """Retrieve the earliest known date for each blob in `blobs`. If some blob has
        no associated date, it is not present in the resulting dictionary.
        """
        ...

    def content_set_early_date(self, blob: FileEntry, date: datetime) -> None:
        """Associate `date` to `blob` as it's earliest known date."""
        ...

    def directory_add_to_revision(
        self, revision: RevisionEntry, directory: DirectoryEntry, path: bytes
    ) -> None:
        """Associate `directory` with `revision` in the provenance model. `path` is the
        absolute path from `revision`'s root directory to `directory` (including
        `directory`'s name).
        """
        ...

    def directory_get_date_in_isochrone_frontier(
        self, directory: DirectoryEntry
    ) -> Optional[datetime]:
        """Retrieve the earliest known date of `directory` as an isochrone frontier in
        the provenance model.
        """
        ...

    def directory_get_dates_in_isochrone_frontier(
        self, dirs: Iterable[DirectoryEntry]
    ) -> Dict[Sha1Git, datetime]:
        """Retrieve the earliest known date for each directory in `dirs` as isochrone
        frontiers provenance model. If some directory has no associated date, it is not
        present in the resulting dictionary.
        """
        ...

    def directory_set_date_in_isochrone_frontier(
        self, directory: DirectoryEntry, date: datetime
    ) -> None:
        """Associate `date` to `directory` as it's earliest known date as an isochrone
        frontier in the provenance model.
        """
        ...

    def origin_add(self, origin: OriginEntry) -> None:
        """Add `origin` to the provenance model."""
        ...

    def revision_add(self, revision: RevisionEntry) -> None:
        """Add `revision` to the provenance model. This implies storing `revision`'s
        date in the model, thus `revision.date` must be a valid date.
        """
        ...

    def revision_add_before_revision(
        self, head: RevisionEntry, revision: RevisionEntry
    ) -> None:
        """Associate `revision` to `head` as an ancestor of the latter."""
        ...

    def revision_add_to_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ) -> None:
        """Associate `revision` to `origin` as a head revision of the latter (ie. the
        target of an snapshot for `origin` in the archive)."""
        ...

    def revision_get_date(self, revision: RevisionEntry) -> Optional[datetime]:
        """Retrieve the date associated to `revision`."""
        ...

    def revision_get_preferred_origin(
        self, revision: RevisionEntry
    ) -> Optional[Sha1Git]:
        """Retrieve the preferred origin associated to `revision`."""
        ...

    def revision_in_history(self, revision: RevisionEntry) -> bool:
        """Check if `revision` is known to be an ancestor of some head revision in the
        provenance model.
        """
        ...

    def revision_set_preferred_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ) -> None:
        """Associate `origin` as the preferred origin for `revision`."""
        ...

    def revision_visited(self, revision: RevisionEntry) -> bool:
        """Check if `revision` is known to be a head revision for some origin in the
        provenance model.
        """
        ...


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
    raise_on_commit: bool = False

    def __init__(self, conn: psycopg2.extensions.connection):
        from .postgresql.provenancedb_base import ProvenanceDBBase

        # TODO: this class should not know what the actual used DB is.
        self.storage: ProvenanceDBBase
        flavor = ProvenanceDBBase(conn).flavor
        if flavor == "with-path":
            from .postgresql.provenancedb_with_path import ProvenanceWithPathDB

            self.storage = ProvenanceWithPathDB(conn)
        else:
            from .postgresql.provenancedb_without_path import ProvenanceWithoutPathDB

            self.storage = ProvenanceWithoutPathDB(conn)
        self.cache: ProvenanceCache = new_cache()

    def clear_caches(self) -> None:
        self.cache = new_cache()

    def flush(self) -> None:
        # TODO: for now we just forward the cache. This should be improved!
        while not self.storage.commit(self.cache, raise_on_commit=self.raise_on_commit):
            logging.warning(
                f"Unable to commit cached information {self.cache}. Retrying..."
            )
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
        self, entity: Literal["content", "revision", "directory"], ids: List[Sha1Git]
    ) -> Dict[Sha1Git, datetime]:
        cache = self.cache[entity]
        missing_ids = set(id for id in ids if id not in cache)
        if missing_ids:
            cache["data"].update(self.storage.get_dates(entity, list(missing_ids)))
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
        cache = self.cache["revision_origin"]
        if revision.id not in cache:
            origin = self.storage.revision_get_preferred_origin(revision.id)
            if origin is not None:
                cache["data"][revision.id] = origin
        return cache["data"].get(revision.id)

    def revision_in_history(self, revision: RevisionEntry) -> bool:
        return revision.id in self.cache[
            "revision_before_revision"
        ] or self.storage.revision_in_history(revision.id)

    def revision_set_preferred_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ) -> None:
        self.cache["revision_origin"]["data"][revision.id] = origin.id
        self.cache["revision_origin"]["added"].add(revision.id)

    def revision_visited(self, revision: RevisionEntry) -> bool:
        return revision.id in dict(
            self.cache["revision_in_origin"]
        ) or self.storage.revision_visited(revision.id)


def normalize(path: bytes) -> bytes:
    return path[2:] if path.startswith(bytes("." + os.path.sep, "utf-8")) else path
