# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any, Dict, Optional, Set

from swh.model.hashutil import hash_to_hex
from swh.model.model import Sha1Git

from .archive import ArchiveInterface
from .interface import ProvenanceInterface
from .model import DirectoryEntry, RevisionEntry

UTCMIN = datetime.min.replace(tzinfo=timezone.utc)


class HistoryNode:
    def __init__(
        self, entry: RevisionEntry, is_head: bool = False, in_history: bool = False
    ) -> None:
        self.entry = entry
        # A revision is `is_head` if it is directly pointed by an origin (ie. a head
        # revision for some snapshot)
        self.is_head = is_head
        # A revision is `in_history` if it appears in the history graph of an already
        # processed revision in the provenance database
        self.in_history = in_history
        # XXX: the current simplified version of the origin-revision layer algorithm
        # does not use this previous two flags at all. They are kept for now but might
        # be removed in the future (hence, RevisionEntry might be used instead of
        # HistoryNode).

    def __str__(self) -> str:
        return f"<{self.entry}: is_head={self.is_head}, in_history={self.in_history}>"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "rev": hash_to_hex(self.entry.id),
            "is_head": self.is_head,
            "in_history": self.in_history,
        }


class HistoryGraph:
    def __init__(
        self,
        archive: ArchiveInterface,
        provenance: ProvenanceInterface,
        revision: RevisionEntry,
    ) -> None:
        self._head = HistoryNode(
            revision,
            is_head=provenance.revision_visited(revision),
            in_history=provenance.revision_in_history(revision),
        )
        self._graph: Dict[HistoryNode, Set[HistoryNode]] = {}

        stack = [self._head]
        while stack:
            current = stack.pop()

            if current not in self._graph:
                self._graph[current] = set()
                current.entry.retrieve_parents(archive)
                for parent in current.entry.parents:
                    node = HistoryNode(
                        parent,
                        is_head=provenance.revision_visited(parent),
                        in_history=provenance.revision_in_history(parent),
                    )
                    self._graph[current].add(node)
                    stack.append(node)

    @property
    def head(self) -> HistoryNode:
        return self._head

    @property
    def parents(self) -> Dict[HistoryNode, Set[HistoryNode]]:
        return self._graph

    def __str__(self) -> str:
        return f"<HistoryGraph: head={self._head}, graph={self._graph}"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "head": self.head.as_dict(),
            "graph": {
                hash_to_hex(node.entry.id): sorted(
                    [parent.as_dict() for parent in parents],
                    key=lambda d: d["rev"],
                )
                for node, parents in self._graph.items()
            },
        }


class IsochroneNode:
    def __init__(
        self,
        entry: DirectoryEntry,
        dbdate: Optional[datetime] = None,
        depth: int = 0,
        prefix: bytes = b"",
    ) -> None:
        self.entry = entry
        self.depth = depth

        # dbdate is the maxdate for this node that comes from the DB
        self._dbdate: Optional[datetime] = dbdate

        # maxdate is set by the maxdate computation algorithm
        self.maxdate: Optional[datetime] = None

        # known is True if this node is already known in the db; either because
        # the current directory actually exists in the database, or because all
        # the content of the current directory is known (subdirectories and files)
        self.known = self.dbdate is not None
        self.invalid = False
        self.path = os.path.join(prefix, self.entry.name) if prefix else self.entry.name
        self.children: Set[IsochroneNode] = set()

    @property
    def dbdate(self) -> Optional[datetime]:
        # use a property to make this attribute (mostly) read-only
        return self._dbdate

    def invalidate(self) -> None:
        self._dbdate = None
        self.maxdate = None
        self.known = False
        self.invalid = True

    def add_directory(
        self, child: DirectoryEntry, date: Optional[datetime] = None
    ) -> IsochroneNode:
        # we should not be processing this node (ie add subdirectories or files) if it's
        # actually known by the provenance DB
        assert self.dbdate is None
        node = IsochroneNode(child, dbdate=date, depth=self.depth + 1, prefix=self.path)
        self.children.add(node)
        return node

    def __str__(self) -> str:
        return (
            f"<{self.entry}: depth={self.depth}, "
            f"dbdate={self.dbdate}, maxdate={self.maxdate}, "
            f"known={self.known}, invalid={self.invalid}, path={self.path!r}, "
            f"children=[{', '.join(str(child) for child in self.children)}]>"
        )

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, IsochroneNode) and self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        # only immutable attributes are considered to compute hash
        return hash((self.entry, self.depth, self.path))


def build_isochrone_graph(
    archive: ArchiveInterface,
    provenance: ProvenanceInterface,
    revision: RevisionEntry,
    directory: DirectoryEntry,
) -> IsochroneNode:
    assert revision.date is not None
    assert revision.root == directory.id

    # this function process a revision in 2 steps:
    #
    # 1. build the tree structure of IsochroneNode objects (one INode per
    #    directory under the root directory of the revision but not following
    #    known subdirectories), and gather the dates from the DB for already
    #    known objects; for files, just keep all the dates in a global 'fdates'
    #    dict; note that in this step, we will only recurse the directories
    #    that are not already known.
    #
    # 2. compute the maxdate for each node of the tree that was not found in the DB.

    # Build the nodes structure
    root_date = provenance.directory_get_date_in_isochrone_frontier(directory)
    root = IsochroneNode(directory, dbdate=root_date)
    stack = [root]
    fdates: Dict[Sha1Git, datetime] = {}  # map {file_id: date}
    while stack:
        current = stack.pop()
        if current.dbdate is None or current.dbdate > revision.date:
            # If current directory has an associated date in the isochrone frontier that
            # is greater or equal to the current revision's one, it should be ignored as
            # the revision is being processed out of order.
            if current.dbdate is not None and current.dbdate > revision.date:
                current.invalidate()

            # Pre-query all known dates for directories in the current directory
            # for the provenance object to have them cached and (potentially) improve
            # performance.
            current.entry.retrieve_children(archive)
            ddates = provenance.directory_get_dates_in_isochrone_frontier(
                current.entry.dirs
            )
            for dir in current.entry.dirs:
                # Recursively analyse subdirectory nodes
                node = current.add_directory(dir, date=ddates.get(dir.id, None))
                stack.append(node)

            fdates.update(provenance.content_get_early_dates(current.entry.files))

    # Precalculate max known date for each node in the graph (only directory nodes are
    # pushed to the stack).
    stack = [root]

    while stack:
        current = stack.pop()
        # Current directory node is known if it already has an assigned date (ie. it was
        # already seen as an isochrone frontier).
        if current.known:
            assert current.maxdate is None
            current.maxdate = current.dbdate
        else:
            if any(x.maxdate is None for x in current.children):
                # at least one child of current has no maxdate yet
                # Current node needs to be analysed again after its children.
                stack.append(current)
                for child in current.children:
                    if child.maxdate is None:
                        # if child.maxdate is None, it must be processed
                        stack.append(child)
            else:
                # all the files and directories under current have a maxdate,
                # we can infer the maxdate for current directory
                assert current.maxdate is None
                # if all content is already known, update current directory info.
                current.maxdate = max(
                    [UTCMIN]
                    + [
                        child.maxdate
                        for child in current.children
                        if child.maxdate is not None  # unnecessary, but needed for mypy
                    ]
                    + [
                        fdates.get(file.id, revision.date)
                        for file in current.entry.files
                    ]
                )
                if current.maxdate <= revision.date:
                    current.known = (
                        # true if all subdirectories are known
                        all(child.known for child in current.children)
                        # true if all files are in fdates, i.e. if all files were known
                        # *before building this isochrone graph node*
                        # Note: the 'all()' is lazy: will stop iterating as soon as
                        # possible
                        and all((file.id in fdates) for file in current.entry.files)
                    )
                else:
                    # at least one content is being processed out-of-order, then current
                    # node should be treated as unknown
                    current.maxdate = revision.date
                    current.known = False
    return root
