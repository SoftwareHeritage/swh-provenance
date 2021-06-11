from datetime import datetime
import logging
import os
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple

import psycopg2
from typing_extensions import Protocol, runtime_checkable

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

    def revision_get_preferred_origin(self, revision: RevisionEntry) -> int:
        ...

    def revision_in_history(self, revision: RevisionEntry) -> bool:
        ...

    def revision_set_preferred_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ) -> None:
        ...

    def revision_visited(self, revision: RevisionEntry) -> bool:
        ...


# TODO: maybe move this to a separate file
class ProvenanceBackend:
    raise_on_commit: bool = False

    def __init__(self, conn: psycopg2.extensions.connection, with_path: bool = True):
        from .postgresql.provenancedb_base import ProvenanceDBBase

        # TODO: this class should not know what the actual used DB is.
        self.storage: ProvenanceDBBase
        if with_path:
            from .postgresql.provenancedb_with_path import ProvenanceWithPathDB

            self.storage = ProvenanceWithPathDB(conn)
        else:
            from .postgresql.provenancedb_without_path import ProvenanceWithoutPathDB

            self.storage = ProvenanceWithoutPathDB(conn)

        self.write_cache: Dict[str, Any] = {}
        self.read_cache: Dict[str, Any] = {}
        self.clear_caches()

    def clear_caches(self):
        self.write_cache = {
            "content": dict(),
            "content_early_in_rev": set(),
            "content_in_dir": set(),
            "directory": dict(),
            "directory_in_rev": set(),
            "revision": dict(),
            "revision_before_rev": list(),
            "revision_in_org": list(),
        }
        self.read_cache = {"content": dict(), "directory": dict(), "revision": dict()}

    def commit(self):
        # TODO: for now we just forward the write_cache. This should be improved!
        while not self.storage.commit(
            self.write_cache, raise_on_commit=self.raise_on_commit
        ):
            logging.warning(
                f"Unable to commit cached information {self.write_cache}. Retrying..."
            )
        self.clear_caches()

    def content_add_to_directory(
        self, directory: DirectoryEntry, blob: FileEntry, prefix: bytes
    ):
        self.write_cache["content_in_dir"].add(
            (blob.id, directory.id, normalize(os.path.join(prefix, blob.name)))
        )

    def content_add_to_revision(
        self, revision: RevisionEntry, blob: FileEntry, prefix: bytes
    ):
        self.write_cache["content_early_in_rev"].add(
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
        self.write_cache["content"][blob.id] = date
        # update read cache as well
        self.read_cache["content"][blob.id] = date

    def directory_add_to_revision(
        self, revision: RevisionEntry, directory: DirectoryEntry, path: bytes
    ):
        self.write_cache["directory_in_rev"].add(
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
        self.write_cache["directory"][directory.id] = date
        # update read cache as well
        self.read_cache["directory"][directory.id] = date

    def get_dates(self, entity: str, ids: List[bytes]) -> Dict[bytes, datetime]:
        dates = {}
        pending = []
        for sha1 in ids:
            # Check whether the date has been queried before
            date = self.read_cache[entity].get(sha1, None)
            if date is not None:
                dates[sha1] = date
            else:
                pending.append(sha1)
        dates.update(self.storage.get_dates(entity, pending))
        return dates

    def origin_get_id(self, origin: OriginEntry) -> int:
        if origin.id is None:
            return self.storage.origin_get_id(origin.url)
        else:
            return origin.id

    def revision_add(self, revision: RevisionEntry):
        # Add current revision to the compact DB
        self.write_cache["revision"][revision.id] = revision.date
        # update read cache as well
        self.read_cache["revision"][revision.id] = revision.date

    def revision_add_before_revision(
        self, relative: RevisionEntry, revision: RevisionEntry
    ):
        self.write_cache["revision_before_rev"].append((revision.id, relative.id))

    def revision_add_to_origin(self, origin: OriginEntry, revision: RevisionEntry):
        self.write_cache["revision_in_org"].append((revision.id, origin.id))

    def revision_get_early_date(self, revision: RevisionEntry) -> Optional[datetime]:
        return self.get_dates("revision", [revision.id]).get(revision.id, None)

    def revision_get_preferred_origin(self, revision: RevisionEntry) -> int:
        # TODO: adapt this method to consider cached values
        return self.storage.revision_get_preferred_origin(revision.id)

    def revision_in_history(self, revision: RevisionEntry) -> bool:
        # TODO: adapt this method to consider cached values
        return self.storage.revision_in_history(revision.id)

    def revision_set_preferred_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ):
        assert origin.id is not None
        # TODO: adapt this method to consider cached values
        self.storage.revision_set_preferred_origin(origin.id, revision.id)

    def revision_visited(self, revision: RevisionEntry) -> bool:
        # TODO: adapt this method to consider cached values
        return self.storage.revision_visited(revision.id)


def normalize(path: bytes) -> bytes:
    return path[2:] if path.startswith(bytes("." + os.path.sep, "utf-8")) else path
