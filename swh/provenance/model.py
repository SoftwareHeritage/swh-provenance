# Copyright (C) 2021-2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterator, List, Optional

from swh.model.model import Origin, Sha1Git

from .archive import ArchiveInterface

UTC = timezone.utc


class OriginEntry:

    revisions_count: int

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
            self._revisions_count = len(self._revisions)

    @property
    def revision_count(self) -> int:
        if self._revisions_count is None:
            raise ValueError("retrieve_revisions was not called")
        return self._revisions_count

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
    ) -> None:
        self.id = id
        assert date is None or date.tzinfo is not None
        self.date = date.astimezone(UTC) if date is not None else None
        self.root = root

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

    def retrieve_children(self, archive: ArchiveInterface, minsize: int = 0) -> None:
        if self._files is None and self._dirs is None:
            self._files = []
            self._dirs = []
            for child in archive.directory_ls(self.id, minsize=minsize):
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
