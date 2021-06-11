from collections import Counter
from datetime import datetime, timezone
import logging
import os
from typing import Dict, List, Optional

from swh.model.hashutil import hash_to_hex

from .archive import ArchiveInterface
from .model import DirectoryEntry, RevisionEntry
from .provenance import ProvenanceInterface

UTCMIN = datetime.min.replace(tzinfo=timezone.utc)


class IsochroneNode:
    def __init__(
        self,
        entry: DirectoryEntry,
        dbdate: Optional[datetime] = None,
        depth: int = 0,
        prefix: bytes = b"",
    ):
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
        self.children: List[IsochroneNode] = []

    @property
    def dbdate(self):
        # use a property to make this attribute (mostly) read-only
        return self._dbdate

    def invalidate(self):
        self._dbdate = None
        self.maxdate = None
        self.known = False
        self.invalid = True

    def add_directory(
        self, child: DirectoryEntry, date: Optional[datetime] = None
    ) -> "IsochroneNode":
        # we should not be processing this node (ie add subdirectories or
        # files) if it's actually known by the provenance DB
        assert self.dbdate is None
        node = IsochroneNode(child, dbdate=date, depth=self.depth + 1, prefix=self.path)
        self.children.append(node)
        return node

    def __str__(self):
        return (
            f"<{self.entry}: dbdate={self.dbdate}, maxdate={self.maxdate}, "
            f"known={self.known}, invalid={self.invalid}, path={self.path}, "
            f"children=[{', '.join(str(child) for child in self.children)}]>"
        )

    def __eq__(self, other):
        return (
            isinstance(other, IsochroneNode)
            and (
                self.entry,
                self.depth,
                self._dbdate,
                self.maxdate,
                self.known,
                self.invalid,
                self.path,
            )
            == (
                other.entry,
                other.depth,
                other._dbdate,
                other.maxdate,
                other.known,
                other.invalid,
                other.path,
            )
            and Counter(self.children) == Counter(other.children)
        )

    def __hash__(self):
        return hash(
            (
                self.entry,
                self.depth,
                self._dbdate,
                self.maxdate,
                self.known,
                self.invalid,
                self.path,
            )
        )


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
    logging.debug(
        f"Recursively creating graph for revision {hash_to_hex(revision.id)}..."
    )
    fdates: Dict[bytes, datetime] = {}  # map {file_id: date}
    while stack:
        current = stack.pop()
        if current.dbdate is None or current.dbdate > revision.date:
            # If current directory has an associated date in the isochrone frontier that
            # is greater or equal to the current revision's one, it should be ignored as
            # the revision is being processed out of order.
            if current.dbdate is not None and current.dbdate > revision.date:
                logging.debug(
                    f"Invalidating frontier on {hash_to_hex(current.entry.id)}"
                    f" (date {current.dbdate})"
                    f" when processing revision {hash_to_hex(revision.id)}"
                    f" (date {revision.date})"
                )
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

    logging.debug(
        f"Isochrone graph for revision {hash_to_hex(revision.id)} successfully created!"
    )
    # Precalculate max known date for each node in the graph (only directory nodes are
    # pushed to the stack).
    logging.debug(f"Computing maxdates for revision {hash_to_hex(revision.id)}...")
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
    logging.debug(
        f"Maxdates for revision {hash_to_hex(revision.id)} successfully computed!"
    )
    return root
