# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import enum
from types import TracebackType
from typing import Dict, Generator, Iterable, Optional, Set, Type, Union

from typing_extensions import Protocol, runtime_checkable

from swh.core.api import remote_api_endpoint
from swh.model.model import Sha1Git

from .model import DirectoryEntry, FileEntry, OriginEntry, RevisionEntry


class EntityType(enum.Enum):
    CONTENT = "content"
    DIRECTORY = "directory"
    REVISION = "revision"
    ORIGIN = "origin"


class RelationType(enum.Enum):
    CNT_EARLY_IN_REV = "content_in_revision"
    CNT_IN_DIR = "content_in_directory"
    DIR_IN_REV = "directory_in_revision"
    REV_IN_ORG = "revision_in_origin"
    REV_BEFORE_REV = "revision_before_revision"


@dataclass(eq=True, frozen=True)
class ProvenanceResult:
    content: Sha1Git
    revision: Sha1Git
    date: datetime
    origin: Optional[str]
    path: bytes


@dataclass(eq=True, frozen=True)
class RevisionData:
    """Object representing the data associated to a revision in the provenance model,
    where `date` is the optional date of the revision (specifying it acknowledges that
    the revision was already processed by the revision-content algorithm); and `origin`
    identifies the preferred origin for the revision, if any.
    """

    date: Optional[datetime]
    origin: Optional[Sha1Git]


@dataclass(eq=True, frozen=True)
class RelationData:
    """Object representing a relation entry in the provenance model, where `src` and
    `dst` are the sha1 ids of the entities being related, and `path` is optional
    depending on the relation being represented.
    """

    dst: Sha1Git
    path: Optional[bytes]


@runtime_checkable
class ProvenanceStorageInterface(Protocol):
    def __enter__(self) -> ProvenanceStorageInterface:
        ...

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        ...

    @remote_api_endpoint("close")
    def close(self) -> None:
        """Close connection to the storage and release resources."""
        ...

    @remote_api_endpoint("content_add")
    def content_add(
        self, cnts: Union[Iterable[Sha1Git], Dict[Sha1Git, Optional[datetime]]]
    ) -> bool:
        """Add blobs identified by sha1 ids, with an optional associated date (as paired
        in `cnts`) to the provenance storage. Return a boolean stating whether the
        information was successfully stored.
        """
        ...

    @remote_api_endpoint("content_find_first")
    def content_find_first(self, id: Sha1Git) -> Optional[ProvenanceResult]:
        """Retrieve the first occurrence of the blob identified by `id`."""
        ...

    @remote_api_endpoint("content_find_all")
    def content_find_all(
        self, id: Sha1Git, limit: Optional[int] = None
    ) -> Generator[ProvenanceResult, None, None]:
        """Retrieve all the occurrences of the blob identified by `id`."""
        ...

    @remote_api_endpoint("content_get")
    def content_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, datetime]:
        """Retrieve the associated date for each blob sha1 in `ids`. If some blob has
        no associated date, it is not present in the resulting dictionary.
        """
        ...

    @remote_api_endpoint("directory_add")
    def directory_add(
        self, dirs: Union[Iterable[Sha1Git], Dict[Sha1Git, Optional[datetime]]]
    ) -> bool:
        """Add directories identified by sha1 ids, with an optional associated date (as
        paired in `dirs`) to the provenance storage. Return a boolean stating if the
        information was successfully stored.
        """
        ...

    @remote_api_endpoint("directory_get")
    def directory_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, datetime]:
        """Retrieve the associated date for each directory sha1 in `ids`. If some
        directory has no associated date, it is not present in the resulting dictionary.
        """
        ...

    @remote_api_endpoint("entity_get_all")
    def entity_get_all(self, entity: EntityType) -> Set[Sha1Git]:
        """Retrieve all sha1 ids for entities of type `entity` present in the provenance
        model. This method is used only in tests.
        """
        ...

    @remote_api_endpoint("location_add")
    def location_add(self, paths: Iterable[bytes]) -> bool:
        """Register the given `paths` in the storage."""
        ...

    @remote_api_endpoint("location_get_all")
    def location_get_all(self) -> Set[bytes]:
        """Retrieve all paths present in the provenance model.
        This method is used only in tests."""
        ...

    @remote_api_endpoint("open")
    def open(self) -> None:
        """Open connection to the storage and allocate necessary resources."""
        ...

    @remote_api_endpoint("origin_add")
    def origin_add(self, orgs: Dict[Sha1Git, str]) -> bool:
        """Add origins identified by sha1 ids, with their corresponding url (as paired
        in `orgs`) to the provenance storage. Return a boolean stating if the
        information was successfully stored.
        """
        ...

    @remote_api_endpoint("origin_get")
    def origin_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, str]:
        """Retrieve the associated url for each origin sha1 in `ids`."""
        ...

    @remote_api_endpoint("revision_add")
    def revision_add(
        self, revs: Union[Iterable[Sha1Git], Dict[Sha1Git, RevisionData]]
    ) -> bool:
        """Add revisions identified by sha1 ids, with optional associated date or origin
        (as paired in `revs`) to the provenance storage. Return a boolean stating if the
        information was successfully stored.
        """
        ...

    @remote_api_endpoint("revision_get")
    def revision_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, RevisionData]:
        """Retrieve the associated date and origin for each revision sha1 in `ids`. If
        some revision has no associated date nor origin, it is not present in the
        resulting dictionary.
        """
        ...

    @remote_api_endpoint("relation_add")
    def relation_add(
        self, relation: RelationType, data: Dict[Sha1Git, Set[RelationData]]
    ) -> bool:
        """Add entries in the selected `relation`. This method assumes all entities
        being related are already registered in the storage. See `content_add`,
        `directory_add`, `origin_add`, and `revision_add`.
        """
        ...

    @remote_api_endpoint("relation_get")
    def relation_get(
        self, relation: RelationType, ids: Iterable[Sha1Git], reverse: bool = False
    ) -> Dict[Sha1Git, Set[RelationData]]:
        """Retrieve all entries in the selected `relation` whose source entities are
        identified by some sha1 id in `ids`. If `reverse` is set, destination entities
        are matched instead.
        """
        ...

    @remote_api_endpoint("relation_get_all")
    def relation_get_all(
        self, relation: RelationType
    ) -> Dict[Sha1Git, Set[RelationData]]:
        """Retrieve all entries in the selected `relation` that are present in the
        provenance model. This method is used only in tests.
        """
        ...

    @remote_api_endpoint("with_path")
    def with_path(self) -> bool:
        ...


@runtime_checkable
class ProvenanceInterface(Protocol):
    storage: ProvenanceStorageInterface

    def __enter__(self) -> ProvenanceInterface:
        ...

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        ...

    def close(self) -> None:
        """Close connection to the underlying `storage` and release resources."""
        ...

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

    def open(self) -> None:
        """Open connection to the underlying `storage` and allocate necessary
        resources.
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
