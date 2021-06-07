from datetime import datetime, timezone
import logging
import os
import time
from typing import Dict, Generator, Iterable, List, Optional, Tuple

from typing_extensions import Protocol, runtime_checkable

from swh.model.hashutil import hash_to_hex

from .archive import ArchiveInterface
from .model import DirectoryEntry, FileEntry, OriginEntry, RevisionEntry

UTCMIN = datetime.min.replace(tzinfo=timezone.utc)


@runtime_checkable
class ProvenanceInterface(Protocol):
    raise_on_commit: bool = False

    def commit(self):
        """Commit currently ongoing transactions in the backend DB"""
        ...

    def content_add_to_directory(
        self, directory: DirectoryEntry, blob: FileEntry, prefix: bytes
    ) -> None:
        ...

    def content_add_to_revision(
        self, revision: RevisionEntry, blob: FileEntry, prefix: bytes
    ) -> None:
        ...

    def content_find_first(
        self, blobid: bytes
    ) -> Optional[Tuple[bytes, bytes, datetime, bytes]]:
        ...

    def content_find_all(
        self, blobid: bytes, limit: Optional[int] = None
    ) -> Generator[Tuple[bytes, bytes, datetime, bytes], None, None]:
        ...

    def content_get_early_date(self, blob: FileEntry) -> Optional[datetime]:
        ...

    def content_get_early_dates(
        self, blobs: Iterable[FileEntry]
    ) -> Dict[bytes, datetime]:
        ...

    def content_set_early_date(self, blob: FileEntry, date: datetime) -> None:
        ...

    def directory_add_to_revision(
        self, revision: RevisionEntry, directory: DirectoryEntry, path: bytes
    ) -> None:
        ...

    def directory_get_date_in_isochrone_frontier(
        self, directory: DirectoryEntry
    ) -> Optional[datetime]:
        ...

    def directory_get_dates_in_isochrone_frontier(
        self, dirs: Iterable[DirectoryEntry]
    ) -> Dict[bytes, datetime]:
        ...

    def directory_invalidate_in_isochrone_frontier(
        self, directory: DirectoryEntry
    ) -> None:
        ...

    def directory_set_date_in_isochrone_frontier(
        self, directory: DirectoryEntry, date: datetime
    ) -> None:
        ...

    def origin_get_id(self, origin: OriginEntry) -> int:
        ...

    def revision_add(self, revision: RevisionEntry) -> None:
        ...

    def revision_add_before_revision(
        self, relative: RevisionEntry, revision: RevisionEntry
    ) -> None:
        ...

    def revision_add_to_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ) -> None:
        ...

    def revision_get_early_date(self, revision: RevisionEntry) -> Optional[datetime]:
        ...

    def revision_get_preferred_origin(self, revision: RevisionEntry) -> int:
        ...

    def revision_in_history(self, revision: RevisionEntry) -> bool:
        ...

    def revision_set_preferred_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ) -> None:
        ...

    def revision_visited(self, revision: RevisionEntry) -> bool:
        ...


def flatten_directory(
    archive: ArchiveInterface,
    provenance: ProvenanceInterface,
    directory: DirectoryEntry,
) -> None:
    """Recursively retrieve all the files of 'directory' and insert them in the
    'provenance' database in the 'content_to_directory' table.
    """
    stack = [(directory, b"")]
    while stack:
        current, prefix = stack.pop()
        current.retrieve_children(archive)
        for f_child in current.files:
            # Add content to the directory with the computed prefix.
            provenance.content_add_to_directory(directory, f_child, prefix)
        for d_child in current.dirs:
            # Recursively walk the child directory.
            stack.append((d_child, os.path.join(prefix, d_child.name)))


def origin_add(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    origins: List[OriginEntry],
) -> None:
    start = time.time()
    for origin in origins:
        origin.retrieve_revisions(archive)
        for revision in origin.revisions:
            origin_add_revision(provenance, archive, origin, revision)
    done = time.time()
    provenance.commit()
    stop = time.time()
    logging.debug(
        "Origins "
        ";".join(
            [origin.url + ":" + hash_to_hex(origin.snapshot) for origin in origins]
        )
        + f" were processed in {stop - start} secs (commit took {stop - done} secs)!"
    )


def origin_add_revision(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    origin: OriginEntry,
    revision: RevisionEntry,
) -> None:
    stack: List[Tuple[Optional[RevisionEntry], RevisionEntry]] = [(None, revision)]

    while stack:
        relative, current = stack.pop()

        # Check if current revision has no preferred origin and update if necessary.
        preferred = provenance.revision_get_preferred_origin(current)

        if preferred is None:
            provenance.revision_set_preferred_origin(origin, current)
        ########################################################################

        if relative is None:
            # This revision is pointed directly by the origin.
            visited = provenance.revision_visited(current)
            provenance.revision_add_to_origin(origin, current)

            if not visited:
                stack.append((current, current))

        else:
            # This revision is a parent of another one in the history of the
            # relative revision.
            for parent in current.parents(archive):
                visited = provenance.revision_visited(parent)

                if not visited:
                    # The parent revision has never been seen before pointing
                    # directly to an origin.
                    known = provenance.revision_in_history(parent)

                    if known:
                        # The parent revision is already known in some other
                        # revision's history. We should point it directly to
                        # the origin and (eventually) walk its history.
                        stack.append((None, parent))
                    else:
                        # The parent revision was never seen before. We should
                        # walk its history and associate it with the same
                        # relative revision.
                        provenance.revision_add_before_revision(relative, parent)
                        stack.append((relative, parent))
                else:
                    # The parent revision already points to an origin, so its
                    # history was properly processed before. We just need to
                    # make sure it points to the current origin as well.
                    provenance.revision_add_to_origin(origin, parent)


def revision_add(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    revisions: List[RevisionEntry],
    trackall: bool = True,
    lower: bool = True,
    mindepth: int = 1,
) -> None:
    start = time.time()
    for revision in revisions:
        assert revision.date is not None
        assert revision.root is not None
        # Processed content starting from the revision's root directory.
        date = provenance.revision_get_early_date(revision)
        if date is None or revision.date < date:
            logging.debug(
                f"Processing revisions {hash_to_hex(revision.id)}"
                f" (known date {date} / revision date {revision.date})..."
            )
            graph = build_isochrone_graph(
                archive,
                provenance,
                revision,
                DirectoryEntry(revision.root),
            )
            # TODO: add file size filtering
            revision_process_content(
                archive,
                provenance,
                revision,
                graph,
                trackall=trackall,
                lower=lower,
                mindepth=mindepth,
            )
    done = time.time()
    # TODO: improve this! Maybe using a max attempt counter?
    # Ideally Provenance class should guarantee that a commit never fails.
    while not provenance.commit():
        logging.warning(
            "Could not commit revisions "
            + ";".join([hash_to_hex(revision.id) for revision in revisions])
            + ". Retrying..."
        )
    stop = time.time()
    logging.debug(
        f"Revisions {';'.join([hash_to_hex(revision.id) for revision in revisions])} "
        f" were processed in {stop - start} secs (commit took {stop - done} secs)!"
    )
    # logging.critical(
    #     ";".join([hash_to_hex(revision.id) for revision in revisions])
    #     + f",{stop - start},{stop - done}"
    # )


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
        self.known: bool = self.dbdate is not None
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
            f"<{self.entry}: "
            f"known={self.known}, maxdate={self.maxdate}, "
            f"dbdate={self.dbdate}, path={self.path}, "
            f"children=[{', '.join(str(child) for child in self.children)}]>"
        )

    def __eq__(self, other):
        sameDbDate = (
            self._dbdate is None and other._dbdate is None
        ) or self._dbdate == other._dbdate
        sameMaxdate = (
            self.maxdate is None and other.maxdate is None
        ) or self.maxdate == other.maxdate
        return (
            isinstance(other, IsochroneNode)
            and (self.entry, self.depth, self.known, self.path)
            == (other.entry, other.depth, other.known, other.path)
            and sameDbDate
            and sameMaxdate
            and set(self.children) == set(other.children)
        )

    def __hash__(self):
        return hash(
            (self.entry, self.depth, self._dbdate, self.maxdate, self.known, self.path)
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
                provenance.directory_invalidate_in_isochrone_frontier(current.entry)
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
                # If all content is already known, update current directory info.
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
                current.known = (
                    # true if all subdirectories are known
                    all(child.known for child in current.children)
                    # true if all files are in fdates, i.e. if all files were known
                    # *before building this isochrone graph node*
                    # Note: the 'all()' is lazy: will stop iterating as soon as possible
                    and all((file.id in fdates) for file in current.entry.files)
                )
    logging.debug(
        f"Maxdates for revision {hash_to_hex(revision.id)} successfully computed!"
    )
    return root


def revision_process_content(
    archive: ArchiveInterface,
    provenance: ProvenanceInterface,
    revision: RevisionEntry,
    graph: IsochroneNode,
    trackall: bool = True,
    lower: bool = True,
    mindepth: int = 1,
):
    assert revision.date is not None
    provenance.revision_add(revision)

    stack = [graph]
    while stack:
        current = stack.pop()
        if current.dbdate is not None:
            assert current.dbdate <= revision.date
            if trackall:
                # Current directory is an outer isochrone frontier for a previously
                # processed revision. It should be reused as is.
                provenance.directory_add_to_revision(
                    revision, current.entry, current.path
                )
        else:
            # Current directory is not an outer isochrone frontier for any previous
            # revision. It might be eligible for this one.
            if is_new_frontier(
                current,
                revision=revision,
                trackall=trackall,
                lower=lower,
                mindepth=mindepth,
            ):
                assert current.maxdate is not None
                # Outer frontier should be moved to current position in the isochrone
                # graph. This is the first time this directory is found in the isochrone
                # frontier.
                provenance.directory_set_date_in_isochrone_frontier(
                    current.entry, current.maxdate
                )
                if trackall:
                    provenance.directory_add_to_revision(
                        revision, current.entry, current.path
                    )
                    flatten_directory(archive, provenance, current.entry)
            else:
                # No point moving the frontier here. Either there are no files or they
                # are being seen for the first time here. Add all blobs to current
                # revision updating date if necessary, and recursively analyse
                # subdirectories as candidates to the outer frontier.
                for blob in current.entry.files:
                    date = provenance.content_get_early_date(blob)
                    if date is None or revision.date < date:
                        provenance.content_set_early_date(blob, revision.date)
                    provenance.content_add_to_revision(revision, blob, current.path)
                for child in current.children:
                    stack.append(child)


def is_new_frontier(
    node: IsochroneNode,
    revision: RevisionEntry,
    trackall: bool = True,
    lower: bool = True,
    mindepth: int = 1,
) -> bool:
    assert node.maxdate is not None  # for mypy
    assert revision.date is not None  # idem
    if trackall:
        # The only real condition for a directory to be a frontier is that its
        # content is already known and its maxdate is less (or equal) than
        # current revision's date. Checking mindepth is meant to skip root
        # directories (or any arbitrary depth) to improve the result. The
        # option lower tries to maximize the reusage rate of previously defined
        # frontiers by keeping them low in the directory tree.
        return (
            node.known
            and node.maxdate <= revision.date  # all content is earlier than revision
            and node.depth
            >= mindepth  # current node is deeper than the min allowed depth
            and (has_blobs(node) if lower else True)  # there is at least one blob in it
        )
    else:
        # If we are only tracking first occurrences, we want to ensure that all first
        # occurrences end up in the content_early_in_rev relation. Thus, we force for
        # every blob outside a frontier to have an extrictly earlier date.
        return (
            node.maxdate < revision.date  # all content is earlier than revision
            and node.depth >= mindepth  # deeper than the min allowed depth
            and (has_blobs(node) if lower else True)  # there is at least one blob
        )


def has_blobs(node: IsochroneNode) -> bool:
    # We may want to look for files in different ways to decide whether to define a
    # frontier or not:
    # 1. Only files in current node:
    return any(node.entry.files)
    # 2. Files anywhere in the isochrone graph
    # stack = [node]
    # while stack:
    #     current = stack.pop()
    #     if any(
    #         map(lambda child: isinstance(child.entry, FileEntry), current.children)):
    #         return True
    #     else:
    #         # All children are directory entries.
    #         stack.extend(current.children)
    # return False
    # 3. Files in the intermediate directories between current node and any previously
    #    defined frontier:
    # TODO: complete this case!
    # return any(
    #     map(lambda child: isinstance(child.entry, FileEntry), node.children)
    # ) or all(
    #     map(
    #         lambda child: (
    #             not (isinstance(child.entry, DirectoryEntry) and child.date is None)
    #         )
    #         or has_blobs(child),
    #         node.children,
    #     )
    # )
