# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Iterator, List, Optional

from swh.model.model import Origin, Sha1Git

from .archive import ArchiveInterface


class OriginEntry:
    def __init__(self, url: str, snapshot: Sha1Git) -> None:
        self.url = url
        self.id = Origin(url=self.url).id
        self.snapshot = snapshot
        self._revisions: Optional[List[RevisionEntry]] = None

    def retrieve_revisions(self, archive: ArchiveInterface) -> None:
        if self._revisions is None:
            self._revisions = [
                RevisionEntry(rev) for rev in archive.snapshot_get_heads(self.snapshot)
            ]

    @property
    def revisions(self) -> Iterator[RevisionEntry]:
        if self._revisions is None:
            raise RuntimeError(
                "Revisions of this node has not yet been retrieved. "
                "Please call retrieve_revisions() before using this property."
            )
        return (x for x in self._revisions)

    def __str__(self) -> str:
        return f"<MOrigin[{self.id.hex()}] url={self.url}, snap={self.snapshot.hex()}>"


class RevisionEntry:
    def __init__(
        self,
        id: Sha1Git,
        date: Optional[datetime] = None,
        root: Optional[Sha1Git] = None,
        parents: Optional[Iterable[Sha1Git]] = None,
    ) -> None:
        self.id = id
        self.date = date
        assert self.date is None or self.date.tzinfo is not None
        self.root = root
        self._parents_ids = parents
        self._parents_entries: Optional[List[RevisionEntry]] = None

    def retrieve_parents(self, archive: ArchiveInterface) -> None:
        if self._parents_entries is None:
            if self._parents_ids is None:
                self._parents_ids = archive.revision_get_parents(self.id)
            self._parents_entries = [RevisionEntry(id) for id in self._parents_ids]

    @property
    def parents(self) -> Iterator[RevisionEntry]:
        if self._parents_entries is None:
            raise RuntimeError(
                "Parents of this node has not yet been retrieved. "
                "Please call retrieve_parents() before using this property."
            )
        return (x for x in self._parents_entries)

    def __str__(self) -> str:
        return f"<MRevision[{self.id.hex()}]>"

    def __eq__(self, other) -> bool:
        return isinstance(other, RevisionEntry) and self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)


class DirectoryEntry:
    def __init__(self, id: Sha1Git, name: bytes = b"") -> None:
        self.id = id
        self.name = name
        self._files: Optional[List[FileEntry]] = None
        self._dirs: Optional[List[DirectoryEntry]] = None

    def retrieve_children(self, archive: ArchiveInterface) -> None:
        if self._files is None and self._dirs is None:
            self._files = []
            self._dirs = []
            for child in archive.directory_ls(self.id):
                if child["type"] == "dir":
                    self._dirs.append(
                        DirectoryEntry(child["target"], name=child["name"])
                    )
                elif child["type"] == "file":
                    self._files.append(FileEntry(child["target"], child["name"]))

    @property
    def files(self) -> Iterator[FileEntry]:
        if self._files is None:
            raise RuntimeError(
                "Children of this node has not yet been retrieved. "
                "Please call retrieve_children() before using this property."
            )
        return (x for x in self._files)

    @property
    def dirs(self) -> Iterator[DirectoryEntry]:
        if self._dirs is None:
            raise RuntimeError(
                "Children of this node has not yet been retrieved. "
                "Please call retrieve_children() before using this property."
            )
        return (x for x in self._dirs)

    def __str__(self) -> str:
        return f"<MDirectory[{self.id.hex()}] {self.name!r}>"

    def __eq__(self, other) -> bool:
        return isinstance(other, DirectoryEntry) and (self.id, self.name) == (
            other.id,
            other.name,
        )

    def __hash__(self) -> int:
        return hash((self.id, self.name))


class FileEntry:
    def __init__(self, id: Sha1Git, name: bytes) -> None:
        self.id = id
        self.name = name

    def __str__(self) -> str:
        return f"<MFile[{self.id.hex()}] {self.name!r}>"

    def __eq__(self, other) -> bool:
        return isinstance(other, FileEntry) and (self.id, self.name) == (
            other.id,
            other.name,
        )

    def __hash__(self) -> int:
        return hash((self.id, self.name))
