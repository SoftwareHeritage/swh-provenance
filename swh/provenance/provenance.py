from datetime import datetime
import enum
from typing import Dict, Generator, Iterable, Optional, Set, Tuple

from typing_extensions import Protocol, runtime_checkable

from swh.model.model import Sha1Git

from .model import DirectoryEntry, FileEntry, OriginEntry, RevisionEntry


class RelationType(enum.Enum):
    CNT_EARLY_IN_REV = "content_in_revision"
    CNT_IN_DIR = "content_in_directory"
    DIR_IN_REV = "directory_in_revision"
    REV_IN_ORG = "revision_in_origin"
    REV_BEFORE_REV = "revision_before_revision"


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
class ProvenanceStorageInterface(Protocol):
    raise_on_commit: bool = False

    def content_find_first(self, id: Sha1Git) -> Optional[ProvenanceResult]:
        """Retrieve the first occurrence of the blob identified by `id`."""
        ...

    def content_find_all(
        self, id: Sha1Git, limit: Optional[int] = None
    ) -> Generator[ProvenanceResult, None, None]:
        """Retrieve all the occurrences of the blob identified by `id`."""
        ...

    def content_set_date(self, dates: Dict[Sha1Git, datetime]) -> bool:
        """Associate dates to blobs identified by sha1 ids, as paired in `dates`. Return
        a boolean stating whether the information was successfully stored.
        """
        ...

    def content_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, datetime]:
        """Retrieve the associated date for each blob sha1 in `ids`. If some blob has
        no associated date, it is not present in the resulting dictionary.
        """
        ...

    def directory_set_date(self, dates: Dict[Sha1Git, datetime]) -> bool:
        """Associate dates to directories identified by sha1 ids, as paired in
        `dates`. Return a boolean stating whether the information was successfully
        stored.
        """
        ...

    def directory_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, datetime]:
        """Retrieve the associated date for each directory sha1 in `ids`. If some
        directory has no associated date, it is not present in the resulting dictionary.
        """
        ...

    def origin_set_url(self, urls: Dict[Sha1Git, str]) -> bool:
        """Associate urls to origins identified by sha1 ids, as paired in `urls`. Return
        a boolean stating whether the information was successfully stored.
        """
        ...

    def origin_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, str]:
        """Retrieve the associated url for each origin sha1 in `ids`. If some origin has
        no associated date, it is not present in the resulting dictionary.
        """
        ...

    def revision_set_date(self, dates: Dict[Sha1Git, datetime]) -> bool:
        """Associate dates to revisions identified by sha1 ids, as paired in `dates`.
        Return a boolean stating whether the information was successfully stored.
        """
        ...

    def revision_set_origin(self, origins: Dict[Sha1Git, Sha1Git]) -> bool:
        """Associate origins to revisions identified by sha1 ids, as paired in
        `origins` (revision ids are keys and origin ids, values). Return a boolean
        stating whether the information was successfully stored.
        """
        ...

    def revision_get(
        self, ids: Iterable[Sha1Git]
    ) -> Dict[Sha1Git, Tuple[Optional[datetime], Optional[Sha1Git]]]:
        """Retrieve the associated date and origin for each revision sha1 in `ids`. If
        some revision has no associated date nor origin, it is not present in the
        resulting dictionary.
        """
        ...

    def relation_add(
        self,
        relation: RelationType,
        data: Iterable[Tuple[Sha1Git, Sha1Git, Optional[bytes]]],
    ) -> bool:
        """Add entries in the selected `relation`. Each tuple in `data` is of the from
        (`src`, `dst`, `path`), where `src` and `dst` are the sha1 ids of the entities
        being related, and `path` is optional depending on the selected `relation`.
        """
        ...

    def relation_get(
        self, relation: RelationType, ids: Iterable[Sha1Git], reverse: bool = False
    ) -> Set[Tuple[Sha1Git, Sha1Git, Optional[bytes]]]:
        """Retrieve all tuples in the selected `relation` whose source entities are
        identified by some sha1 id in `ids`. If `reverse` is set, destination entities
        are matched instead.
        """
        ...


@runtime_checkable
class ProvenanceInterface(Protocol):
    storage: ProvenanceStorageInterface

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
