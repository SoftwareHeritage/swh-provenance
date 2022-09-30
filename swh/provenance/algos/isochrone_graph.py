# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any, Dict, Optional, Set

from swh.core.statsd import statsd
from swh.model.model import Sha1Git
from swh.provenance.archive import ArchiveInterface
from swh.provenance.interface import ProvenanceInterface
from swh.provenance.model import DirectoryEntry, RevisionEntry

GRAPH_DURATION_METRIC = "swh_provenance_graph_duration_seconds"
GRAPH_OPERATIONS_METRIC = "swh_provenance_graph_operations_total"

UTCMIN = datetime.min.replace(tzinfo=timezone.utc)


class DirectoryTooLarge(ValueError):
    pass


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

        self.invalid = False
        self.path = os.path.join(prefix, self.entry.name) if prefix else self.entry.name
        self.children: Set[IsochroneNode] = set()

    @property
    def dbdate(self) -> Optional[datetime]:
        # use a property to make this attribute (mostly) read-only
        return self._dbdate

    def invalidate(self) -> None:
        statsd.increment(
            metric=GRAPH_OPERATIONS_METRIC, tags={"method": "invalidate_frontier"}
        )
        self._dbdate = None
        self.maxdate = None
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
            f"<{self.entry}: depth={self.depth}, dbdate={self.dbdate}, "
            f"maxdate={self.maxdate}, invalid={self.invalid}, path={self.path!r}, "
            f"children=[{', '.join(str(child) for child in self.children)}]>"
        )

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, IsochroneNode) and self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        # only immutable attributes are considered to compute hash
        return hash((self.entry, self.depth, self.path))


@statsd.timed(metric=GRAPH_DURATION_METRIC, tags={"method": "build_isochrone_graph"})
def build_isochrone_graph(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    revision: RevisionEntry,
    directory: DirectoryEntry,
    minsize: int = 0,
    max_directory_size: int = 0,
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
    counter = 0
    while stack:
        counter += 1
        if max_directory_size and counter > max_directory_size:
            raise DirectoryTooLarge(
                f"Max directory size exceeded ({counter}): {directory.id.hex()}"
            )
        current = stack.pop()
        if current.dbdate is None or current.dbdate >= revision.date:
            # If current directory has an associated date in the isochrone frontier that
            # is greater or equal to the current revision's one, it should be ignored as
            # the revision is being processed out of order.
            if current.dbdate is not None and current.dbdate >= revision.date:
                current.invalidate()

            # Pre-query all known dates for directories in the current directory
            # for the provenance object to have them cached and (potentially) improve
            # performance.
            current.entry.retrieve_children(archive, minsize=minsize)
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
        if current.dbdate is not None:
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
                        if child.maxdate is not None  # for mypy
                    ]
                    + [
                        fdates.get(file.id, revision.date)
                        for file in current.entry.files
                    ]
                )
    return root
