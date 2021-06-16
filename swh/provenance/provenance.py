from datetime import datetime
import logging
import os
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple

import psycopg2
from typing_extensions import Literal, Protocol, TypedDict, runtime_checkable

from .model import DirectoryEntry, FileEntry, OriginEntry, RevisionEntry


# XXX: this protocol doesn't make much sense now that flavours have been delegated to
# another class, lower in the callstack.
@runtime_checkable
class ProvenanceInterface(Protocol):
    raise_on_commit: bool = False

    def commit(self):
        """Commit currently ongoing transactions in the backend DB"""
        ...

    def content_add_to_directory(
        self, directory: DirectoryEntry, blob: FileEntry, prefix: bytes
    ) -> None:
        ...

    def content_add_to_revision(
        self, revision: RevisionEntry, blob: FileEntry, prefix: bytes
    ) -> None:
        ...

    def content_find_first(
        self, blob: bytes
    ) -> Optional[Tuple[bytes, bytes, datetime, bytes]]:
        ...

    def content_find_all(
        self, blob: bytes, limit: Optional[int] = None
    ) -> Generator[Tuple[bytes, bytes, datetime, bytes], None, None]:
        ...

    def content_get_early_date(self, blob: FileEntry) -> Optional[datetime]:
        ...

    def content_get_early_dates(
        self, blobs: Iterable[FileEntry]
    ) -> Dict[bytes, datetime]:
        ...

    def content_set_early_date(self, blob: FileEntry, date: datetime) -> None:
        ...

    def directory_add_to_revision(
        self, revision: RevisionEntry, directory: DirectoryEntry, path: bytes
    ) -> None:
        ...

    def directory_get_date_in_isochrone_frontier(
        self, directory: DirectoryEntry
    ) -> Optional[datetime]:
        ...

    def directory_get_dates_in_isochrone_frontier(
        self, dirs: Iterable[DirectoryEntry]
    ) -> Dict[bytes, datetime]:
        ...

    def directory_set_date_in_isochrone_frontier(
        self, directory: DirectoryEntry, date: datetime
    ) -> None:
        ...

    def origin_get_id(self, origin: OriginEntry) -> int:
        ...

    def revision_add(self, revision: RevisionEntry) -> None:
        ...

    def revision_add_before_revision(
        self, relative: RevisionEntry, revision: RevisionEntry
    ) -> None:
        ...

    def revision_add_to_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ) -> None:
        ...

    def revision_get_early_date(self, revision: RevisionEntry) -> Optional[datetime]:
        ...

    def revision_get_preferred_origin(self, revision: RevisionEntry) -> Optional[int]:
        ...

    def revision_in_history(self, revision: RevisionEntry) -> bool:
        ...

    def revision_set_preferred_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ) -> None:
        ...

    def revision_visited(self, revision: RevisionEntry) -> bool:
        ...


class DatetimeCache(TypedDict):
    data: Dict[bytes, datetime]
    added: Set[bytes]


class OriginCache(TypedDict):
    data: Dict[bytes, int]  # TODO: we should switch to use Url instead
    added: Set[bytes]


class ProvenanceCache(TypedDict):
    content: DatetimeCache
    directory: DatetimeCache
    revision: DatetimeCache
    # below are insertion caches only
    content_in_revision: Set[Tuple[bytes, bytes, bytes]]
    content_in_directory: Set[Tuple[bytes, bytes, bytes]]
    directory_in_revision: Set[Tuple[bytes, bytes, bytes]]
    # these two are for the origin layer
    revision_before_revision: Dict[bytes, Set[bytes]]
    revision_in_origin: Set[Tuple[bytes, int]]
    revision_preferred_origin: OriginCache


def new_cache():
    return ProvenanceCache(
        content=DatetimeCache(data={}, added=set()),
        directory=DatetimeCache(data={}, added=set()),
        revision=DatetimeCache(data={}, added=set()),
        content_in_revision=set(),
        content_in_directory=set(),
        directory_in_revision=set(),
        revision_before_revision={},
        revision_in_origin=set(),
        revision_preferred_origin=OriginCache(data={}, added=set()),
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

    def clear_caches(self):
        self.cache = new_cache()

    def commit(self):
        # TODO: for now we just forward the cache. This should be improved!
        while not self.storage.commit(self.cache, raise_on_commit=self.raise_on_commit):
            logging.warning(
                f"Unable to commit cached information {self.write_cache}. Retrying..."
            )
        self.clear_caches()

    def content_add_to_directory(
        self, directory: DirectoryEntry, blob: FileEntry, prefix: bytes
    ):
        self.cache["content_in_directory"].add(
            (blob.id, directory.id, normalize(os.path.join(prefix, blob.name)))
        )

    def content_add_to_revision(
        self, revision: RevisionEntry, blob: FileEntry, prefix: bytes
    ):
        self.cache["content_in_revision"].add(
            (blob.id, revision.id, normalize(os.path.join(prefix, blob.name)))
        )

    def content_find_first(
        self, blob: bytes
    ) -> Optional[Tuple[bytes, bytes, datetime, bytes]]:
        return self.storage.content_find_first(blob)

    def content_find_all(
        self, blob: bytes, limit: Optional[int] = None
    ) -> Generator[Tuple[bytes, bytes, datetime, bytes], None, None]:
        yield from self.storage.content_find_all(blob, limit=limit)

    def content_get_early_date(self, blob: FileEntry) -> Optional[datetime]:
        return self.get_dates("content", [blob.id]).get(blob.id, None)

    def content_get_early_dates(
        self, blobs: Iterable[FileEntry]
    ) -> Dict[bytes, datetime]:
        return self.get_dates("content", [blob.id for blob in blobs])

    def content_set_early_date(self, blob: FileEntry, date: datetime):
        self.cache["content"]["data"][blob.id] = date
        self.cache["content"]["added"].add(blob.id)

    def directory_add_to_revision(
        self, revision: RevisionEntry, directory: DirectoryEntry, path: bytes
    ):
        self.cache["directory_in_revision"].add(
            (directory.id, revision.id, normalize(path))
        )

    def directory_get_date_in_isochrone_frontier(
        self, directory: DirectoryEntry
    ) -> Optional[datetime]:
        return self.get_dates("directory", [directory.id]).get(directory.id, None)

    def directory_get_dates_in_isochrone_frontier(
        self, dirs: Iterable[DirectoryEntry]
    ) -> Dict[bytes, datetime]:
        return self.get_dates("directory", [directory.id for directory in dirs])

    def directory_set_date_in_isochrone_frontier(
        self, directory: DirectoryEntry, date: datetime
    ):
        self.cache["directory"]["data"][directory.id] = date
        self.cache["directory"]["added"].add(directory.id)

    def get_dates(
        self, entity: Literal["content", "revision", "directory"], ids: List[bytes]
    ) -> Dict[bytes, datetime]:
        cache = self.cache[entity]
        missing_ids = set(id for id in ids if id not in cache)
        if missing_ids:
            cache["data"].update(self.storage.get_dates(entity, list(missing_ids)))
        return {sha1: cache["data"][sha1] for sha1 in ids if sha1 in cache["data"]}

    def origin_get_id(self, origin: OriginEntry) -> int:
        if origin.id is None:
            return self.storage.origin_get_id(origin.url)
        else:
            return origin.id

    def revision_add(self, revision: RevisionEntry):
        # Add current revision to the compact DB
        assert revision.date is not None
        self.cache["revision"]["data"][revision.id] = revision.date
        self.cache["revision"]["added"].add(revision.id)

    def revision_add_before_revision(
        self, relative: RevisionEntry, revision: RevisionEntry
    ):
        self.cache["revision_before_revision"].setdefault(revision.id, set()).add(
            relative.id
        )

    def revision_add_to_origin(self, origin: OriginEntry, revision: RevisionEntry):
        assert origin.id is not None
        self.cache["revision_in_origin"].add((revision.id, origin.id))

    def revision_get_early_date(self, revision: RevisionEntry) -> Optional[datetime]:
        return self.get_dates("revision", [revision.id]).get(revision.id, None)

    def revision_get_preferred_origin(self, revision: RevisionEntry) -> Optional[int]:
        if revision.id not in self.cache["revision_preferred_origin"]["data"]:
            origin = self.storage.revision_get_preferred_origin(revision.id)
            if origin is not None:
                self.cache["revision_preferred_origin"]["data"][revision.id] = origin
        return self.cache["revision_preferred_origin"]["data"].get(revision.id)

    def revision_in_history(self, revision: RevisionEntry) -> bool:
        return revision.id in self.cache[
            "revision_before_revision"
        ] or self.storage.revision_in_history(revision.id)

    def revision_set_preferred_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ):
        assert origin.id is not None
        self.cache["revision_preferred_origin"]["data"][revision.id] = origin.id
        self.cache["revision_preferred_origin"]["added"].add(revision.id)

    def revision_visited(self, revision: RevisionEntry) -> bool:
        return revision.id in dict(
            self.cache["revision_in_origin"]
        ) or self.storage.revision_visited(revision.id)


def normalize(path: bytes) -> bytes:
    return path[2:] if path.startswith(bytes("." + os.path.sep, "utf-8")) else path
