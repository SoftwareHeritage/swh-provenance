# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import enum
from types import TracebackType
from typing import Dict, Generator, Iterable, List, Optional, Set, Type

from typing_extensions import Protocol, runtime_checkable

from swh.core.api import remote_api_endpoint
from swh.model.model import Sha1Git


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
class DirectoryData:
    """Object representing the data associated to a directory in the provenance model,
    where `date` is the date of the directory in the isochrone frontier, and `flat` is a
    flag acknowledging that a flat model for the elements outside the frontier has
    already been created.
    """

    date: datetime
    flat: bool


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
    def content_add(self, cnts: Dict[Sha1Git, datetime]) -> bool:
        """Add blobs identified by sha1 ids, with an associated date (as paired in
        `cnts`) to the provenance storage. Return a boolean stating whether the
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
        """Retrieve the associated date for each blob sha1 in `ids`."""
        ...

    @remote_api_endpoint("directory_add")
    def directory_add(self, dirs: Dict[Sha1Git, DirectoryData]) -> bool:
        """Add directories identified by sha1 ids, with associated date and (optional)
        flatten flag (as paired in `dirs`) to the provenance storage. If the flatten
        flag is set to None, the previous value present in the storage is preserved.
        Return a boolean stating if the information was successfully stored.
        """
        ...

    @remote_api_endpoint("directory_get")
    def directory_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, DirectoryData]:
        """Retrieve the associated date and (optional) flatten flag for each directory
        sha1 in `ids`. If some directories has no associated date, it is not present in
        the resulting dictionary.
        """
        ...

    @remote_api_endpoint("directory_iter_not_flattenned")
    def directory_iter_not_flattenned(
        self, limit: int, start_id: Sha1Git
    ) -> List[Sha1Git]:
        """Retrieve the unflattenned directories after ``start_id`` up to ``limit`` entries."""
        ...

    @remote_api_endpoint("entity_get_all")
    def entity_get_all(self, entity: EntityType) -> Set[Sha1Git]:
        """Retrieve all sha1 ids for entities of type `entity` present in the provenance
        model. This method is used only in tests.
        """
        ...

    @remote_api_endpoint("location_add")
    def location_add(self, paths: Dict[Sha1Git, bytes]) -> bool:
        """Register the given `paths` in the storage."""
        ...

    @remote_api_endpoint("location_get_all")
    def location_get_all(self) -> Dict[Sha1Git, bytes]:
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
    def revision_add(self, revs: Dict[Sha1Git, RevisionData]) -> bool:
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
