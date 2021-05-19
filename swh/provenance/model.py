# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
from typing import Iterable, Iterator, List, Optional

from .archive import ArchiveInterface


class OriginEntry:
    def __init__(self, url, revisions: Iterable["RevisionEntry"], id=None):
        self.id = id
        self.url = url
        self.revisions = revisions


class RevisionEntry:
    def __init__(
        self,
        id: bytes,
        date: Optional[datetime] = None,
        root: Optional[bytes] = None,
        parents: Optional[Iterable[bytes]] = None,
    ):
        self.id = id
        self.date = date
        assert self.date is None or self.date.tzinfo is not None
        self.root = root
        self._parents = parents
        self._nodes: List[RevisionEntry] = []

    def parents(self, archive: ArchiveInterface):
        if self._parents is None:
            revision = archive.revision_get([self.id])
            if revision:
                self._parents = revision[0].parents
        if self._parents and not self._nodes:
            self._nodes = [
                RevisionEntry(
                    id=rev.id,
                    root=rev.directory,
                    date=rev.date,
                    parents=rev.parents,
                )
                for rev in archive.revision_get(self._parents)
                if rev
            ]
        yield from self._nodes

    def __str__(self):
        return f"<MRevision[{self.id.hex()}] {self.date.isoformat()}>"


class DirectoryEntry:
    def __init__(self, id: bytes, name: bytes = b""):
        self.id = id
        self.name = name
        self._files: Optional[List[FileEntry]] = None
        self._dirs: Optional[List[DirectoryEntry]] = None

    def retrieve_children(self, archive: ArchiveInterface):
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
    def files(self) -> Iterator["FileEntry"]:
        if self._files is None:
            raise RuntimeError(
                "Children of this node has not yet been retrieved. "
                "Please call retrieve_children() before using this property."
            )
        return (x for x in self._files)

    @property
    def dirs(self) -> Iterator["DirectoryEntry"]:
        if self._dirs is None:
            raise RuntimeError(
                "Children of this node has not yet been retrieved. "
                "Please call retrieve_children() before using this property."
            )
        return (x for x in self._dirs)

    def __str__(self):
        return f"<MDirectory[{self.id.hex()}] {self.name}>"


class FileEntry:
    def __init__(self, id: bytes, name: bytes):
        self.id = id
        self.name = name

    def __str__(self):
        return f"<MFile[{self.id.hex()}] {self.name}>"
