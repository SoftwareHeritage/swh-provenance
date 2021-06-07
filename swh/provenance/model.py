# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
from typing import Iterable, Iterator, List, Optional, Set

from swh.core.utils import grouper
from swh.model.model import ObjectType, TargetType

from .archive import ArchiveInterface


class OriginEntry:
    def __init__(
        self, url: str, date: datetime, snapshot: bytes, id: Optional[int] = None
    ):
        self.url = url
        self.date = date
        self.snapshot = snapshot
        self.id = id
        self._revisions: Optional[List[RevisionEntry]] = None

    def retrieve_revisions(self, archive: ArchiveInterface):
        if self._revisions is None:
            snapshot = archive.snapshot_get_all_branches(self.snapshot)
            assert snapshot is not None
            targets_set = set()
            releases_set = set()
            if snapshot is not None:
                for branch in snapshot.branches:
                    if snapshot.branches[branch].target_type == TargetType.REVISION:
                        targets_set.add(snapshot.branches[branch].target)
                    elif snapshot.branches[branch].target_type == TargetType.RELEASE:
                        releases_set.add(snapshot.branches[branch].target)

            batchsize = 100
            for releases in grouper(releases_set, batchsize):
                targets_set.update(
                    release.target
                    for release in archive.revision_get(releases)
                    if release is not None
                    and release.target_type == ObjectType.REVISION
                )

            revisions: Set[RevisionEntry] = set()
            for targets in grouper(targets_set, batchsize):
                revisions.update(
                    RevisionEntry(revision.id)
                    for revision in archive.revision_get(targets)
                    if revision is not None
                )

            self._revisions = list(revisions)

    @property
    def revisions(self) -> Iterator["RevisionEntry"]:
        if self._revisions is None:
            raise RuntimeError(
                "Revisions of this node has not yet been retrieved. "
                "Please call retrieve_revisions() before using this property."
            )
        return (x for x in self._revisions)


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
                self._parents = list(revision)[0].parents
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

    def __eq__(self, other):
        return isinstance(other, DirectoryEntry) and (self.id, self.name) == (
            other.id,
            other.name,
        )

    def __hash__(self):
        return hash((self.id, self.name))


class FileEntry:
    def __init__(self, id: bytes, name: bytes):
        self.id = id
        self.name = name

    def __str__(self):
        return f"<MFile[{self.id.hex()}] {self.name}>"

    def __eq__(self, other):
        return isinstance(other, FileEntry) and (self.id, self.name) == (
            other.id,
            other.name,
        )

    def __hash__(self):
        return hash((self.id, self.name))
