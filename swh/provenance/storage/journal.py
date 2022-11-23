# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from datetime import datetime
import hashlib
from types import TracebackType
from typing import Dict, Generator, Iterable, List, Optional, Set, Type

from swh.model.model import Sha1Git
from swh.provenance.storage.interface import (
    DirectoryData,
    EntityType,
    ProvenanceResult,
    ProvenanceStorageInterface,
    RelationData,
    RelationType,
    RevisionData,
)


class JournalMessage:
    def __init__(self, id, value, add_id=True):
        self.id = id
        self.value = value
        self.add_id = add_id

    def anonymize(self):
        return None

    def unique_key(self):
        return self.id

    def to_dict(self):
        if self.add_id:
            return {
                "id": self.id,
                "value": self.value,
            }
        else:
            return self.value


class ProvenanceStorageJournal:
    def __init__(self, storage, journal):
        self.storage = storage
        self.journal = journal

    def __enter__(self) -> ProvenanceStorageInterface:
        self.storage.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        return self.storage.__exit__(exc_type, exc_val, exc_tb)

    def open(self) -> None:
        self.storage.open()

    def close(self) -> None:
        self.storage.close()

    def content_add(self, cnts: Dict[Sha1Git, datetime]) -> bool:
        self.journal.write_additions(
            "content", [JournalMessage(key, value) for (key, value) in cnts.items()]
        )
        return self.storage.content_add(cnts)

    def content_find_first(self, id: Sha1Git) -> Optional[ProvenanceResult]:
        return self.storage.content_find_first(id)

    def content_find_all(
        self, id: Sha1Git, limit: Optional[int] = None
    ) -> Generator[ProvenanceResult, None, None]:
        return self.storage.content_find_all(id, limit)

    def content_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, datetime]:
        return self.storage.content_get(ids)

    def directory_add(self, dirs: Dict[Sha1Git, DirectoryData]) -> bool:
        self.journal.write_additions(
            "directory",
            [
                JournalMessage(key, value.date)
                for (key, value) in dirs.items()
                if value.date is not None
            ],
        )
        return self.storage.directory_add(dirs)

    def directory_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, DirectoryData]:
        return self.storage.directory_get(ids)

    def directory_iter_not_flattened(
        self, limit: int, start_id: Sha1Git
    ) -> List[Sha1Git]:
        return self.storage.directory_iter_not_flattened(limit, start_id)

    def entity_get_all(self, entity: EntityType) -> Set[Sha1Git]:
        return self.storage.entity_get_all(entity)

    def location_add(self, paths: Dict[Sha1Git, bytes]) -> bool:
        return self.storage.location_add(paths)

    def location_get_all(self) -> Dict[Sha1Git, bytes]:
        return self.storage.location_get_all()

    def origin_add(self, orgs: Dict[Sha1Git, str]) -> bool:
        self.journal.write_additions(
            "origin", [JournalMessage(key, value) for (key, value) in orgs.items()]
        )
        return self.storage.origin_add(orgs)

    def origin_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, str]:
        return self.storage.origin_get(ids)

    def revision_add(self, revs: Dict[Sha1Git, RevisionData]) -> bool:
        self.journal.write_additions(
            "revision",
            [
                JournalMessage(key, value.date)
                for (key, value) in revs.items()
                if value.date is not None
            ],
        )
        return self.storage.revision_add(revs)

    def revision_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, RevisionData]:
        return self.storage.revision_get(ids)

    def relation_add(
        self, relation: RelationType, data: Dict[Sha1Git, Set[RelationData]]
    ) -> bool:
        messages = []
        for src, relations in data.items():
            for reldata in relations:
                key = hashlib.sha1(src + reldata.dst + (reldata.path or b"")).digest()
                messages.append(
                    JournalMessage(
                        key,
                        {"src": src, "dst": reldata.dst, "path": reldata.path},
                        add_id=False,
                    )
                )

        self.journal.write_additions(relation.value, messages)
        return self.storage.relation_add(relation, data)

    def relation_get(
        self, relation: RelationType, ids: Iterable[Sha1Git], reverse: bool = False
    ) -> Dict[Sha1Git, Set[RelationData]]:
        return self.storage.relation_get(relation, ids, reverse)

    def relation_get_all(
        self, relation: RelationType
    ) -> Dict[Sha1Git, Set[RelationData]]:
        return self.storage.relation_get_all(relation)
