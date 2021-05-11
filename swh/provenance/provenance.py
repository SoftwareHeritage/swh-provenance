from datetime import datetime, timezone
import logging
import os
import time
from typing import Dict, Generator, List, Optional, Tuple

from typing_extensions import Protocol, runtime_checkable

from swh.model.hashutil import hash_to_hex

from .archive import ArchiveInterface
from .model import DirectoryEntry, FileEntry, OriginEntry, RevisionEntry

UTCMIN = datetime.min.replace(tzinfo=timezone.utc)


@runtime_checkable
class ProvenanceInterface(Protocol):
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

    def content_get_early_dates(self, blobs: List[FileEntry]) -> Dict[bytes, datetime]:
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
        self, dirs: List[DirectoryEntry]
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
    stack = [(directory, b"")]
    while stack:
        current, prefix = stack.pop()
        for child in current.ls(archive):
            if isinstance(child, FileEntry):
                # Add content to the directory with the computed prefix.
                provenance.content_add_to_directory(directory, child, prefix)
            elif isinstance(child, DirectoryEntry):
                # Recursively walk the child directory.
                stack.append((child, os.path.join(prefix, child.name)))


def origin_add(
    archive: ArchiveInterface, provenance: ProvenanceInterface, origin: OriginEntry
) -> None:
    # TODO: refactor to iterate over origin visit statuses and commit only once
    # per status.
    origin.id = provenance.origin_get_id(origin)
    for revision in origin.revisions:
        origin_add_revision(archive, provenance, origin, revision)
        # Commit after each revision
        provenance.commit()  # TODO: verify this!


def origin_add_revision(
    archive: ArchiveInterface,
    provenance: ProvenanceInterface,
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
        date: Optional[datetime] = None,
        depth: int = 0,
        prefix: bytes = b"",
    ):
        self.entry = entry
        self.depth = depth
        self.date = date
        self.known = self.date is not None
        self.children: List[IsochroneNode] = []
        self.maxdate: Optional[datetime] = None
        self.path = os.path.join(prefix, self.entry.name) if prefix else self.entry.name

    def add_child(
        self, child: DirectoryEntry, date: Optional[datetime] = None
    ) -> "IsochroneNode":
        assert isinstance(self.entry, DirectoryEntry) and self.date is None
        node = IsochroneNode(
            child,
            date=date,
            depth=self.depth + 1,
            prefix=self.path,
        )
        self.children.append(node)
        return node


def build_isochrone_graph(
    archive: ArchiveInterface,
    provenance: ProvenanceInterface,
    revision: RevisionEntry,
    directory: DirectoryEntry,
) -> IsochroneNode:
    assert revision.date is not None
    assert revision.root == directory.id

    # Build the nodes structure
    root_date = provenance.directory_get_date_in_isochrone_frontier(directory)
    root = IsochroneNode(directory, date=root_date)
    stack = [root]
    logging.debug(
        f"Recursively creating graph for revision {hash_to_hex(revision.id)}..."
    )
    while stack:
        current = stack.pop()
        assert isinstance(current.entry, DirectoryEntry)
        if current.date is None or current.date > revision.date:
            # If current directory has an associated date in the isochrone frontier that
            # is greater or equal to the current revision's one, it should be ignored as
            # the revision is being processed out of order.
            if current.date is not None and current.date > revision.date:
                logging.debug(
                    f"Invalidating frontier on {hash_to_hex(current.entry.id)}"
                    f" (date {current.date})"
                    f" when processing revision {hash_to_hex(revision.id)}"
                    f" (date {revision.date})"
                )
                provenance.directory_invalidate_in_isochrone_frontier(current.entry)
                current.date = None
                current.known = False
            # Pre-query all known dates for content/directories in the current directory
            # for the provenance object to have them cached and (potentially) improve
            # performance.
            ddates = provenance.directory_get_dates_in_isochrone_frontier(
                [
                    child
                    for child in current.entry.ls(archive)
                    if isinstance(child, DirectoryEntry)
                ]
            )
            fdates = provenance.content_get_early_dates(
                [
                    child
                    for child in current.entry.ls(archive)
                    if isinstance(child, FileEntry)
                ]
            )
            for child in current.entry.ls(archive):
                # Recursively analyse directory nodes.
                if isinstance(child, DirectoryEntry):
                    node = current.add_child(child, date=ddates.get(child.id))
                    stack.append(node)
                else:
                    # WARNING: there is a type checking issue here!
                    current.add_child(child, date=fdates.get(child.id))
    logging.debug(
        f"Isochrone graph for revision {hash_to_hex(revision.id)} successfully created!"
    )
    # Precalculate max known date for each node in the graph (only directory nodes are
    # pushed to the stack).
    stack = [root]
    logging.debug(f"Computing maxdates for revision {hash_to_hex(revision.id)}...")
    while stack:
        current = stack.pop()
        # Current directory node is known if it already has an assigned date (ie. it was
        # already seen as an isochrone frontier).
        if not current.known:
            if any(map(lambda child: child.maxdate is None, current.children)):
                # Current node needs to be analysed again after its children.
                stack.append(current)
                for child in current.children:
                    if isinstance(child.entry, FileEntry):
                        # A file node is known if it already has an assigned date (ie.
                        # is was processed before)
                        if child.known:
                            assert child.date is not None
                            # Just use its known date.
                            child.maxdate = child.date
                        else:
                            # Use current revision date.
                            child.maxdate = revision.date
                    else:
                        stack.append(child)
            else:
                maxdates = [
                    child.maxdate
                    for child in current.children
                    if child.maxdate is not None  # mostly to please mypy
                ]
                current.maxdate = max(maxdates) if maxdates else UTCMIN
                # If all content is already known, update current directory info.
                current.known = all(map(lambda child: child.known, current.children))
        else:
            # Directory node in the frontier, just use its known date.
            current.maxdate = current.date
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
        assert isinstance(current.entry, DirectoryEntry)
        if current.date is not None:
            assert current.date <= revision.date
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
                current, revision, trackall=trackall, lower=lower, mindepth=mindepth
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
                for child in current.children:
                    if isinstance(child.entry, FileEntry):
                        blob = child.entry
                        if child.date is None or revision.date < child.date:
                            provenance.content_set_early_date(blob, revision.date)
                        provenance.content_add_to_revision(revision, blob, current.path)
                    else:
                        stack.append(child)


def is_new_frontier(
    node: IsochroneNode,
    revision: RevisionEntry,
    trackall: bool = True,
    lower: bool = True,
    mindepth: int = 1,
) -> bool:
    assert node.maxdate is not None and revision.date is not None
    if trackall:
        # The only real condition for a directory to be a frontier is that its content
        # is already known and its maxdate is less (or equal) than current revision's
        # date. Checking mindepth is meant to skip root directories (or any arbitrary
        # depth) to improve the result. The option lower tries to maximize the reusage
        # rate of previously defined frontiers by keeping them low in the directory
        # tree.
        return (
            node.known  # all content in node was already seen before
            and node.maxdate <= revision.date  # all content is earlier than revision
            and node.depth >= mindepth  # deeper than the min allowed depth
            and (has_blobs(node) if lower else True)  # there is at least one blob
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
    return any(map(lambda child: isinstance(child.entry, FileEntry), node.children))
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
