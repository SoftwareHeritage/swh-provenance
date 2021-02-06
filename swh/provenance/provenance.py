import os

from .archive import ArchiveInterface
from .model import DirectoryEntry, FileEntry, TreeEntry
from .origin import OriginEntry
from .revision import RevisionEntry

from datetime import datetime
from typing import Dict, Generator, List, Optional, Tuple


# TODO: consider moving to path utils file together with normalize.
def is_child(path: bytes, prefix: bytes) -> bool:
    return path != prefix and os.path.dirname(path) == prefix


class ProvenanceInterface:
    def __init__(self, **kwargs):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def content_add_to_directory(
        self, directory: DirectoryEntry, blob: FileEntry, prefix: bytes
    ):
        raise NotImplementedError

    def content_add_to_revision(
        self, revision: RevisionEntry, blob: FileEntry, prefix: bytes
    ):
        raise NotImplementedError

    def content_find_first(
        self, blobid: bytes
    ) -> Optional[Tuple[bytes, bytes, datetime, bytes]]:
        raise NotImplementedError

    def content_find_all(
        self, blobid: bytes
    ) -> Generator[Tuple[bytes, bytes, datetime, bytes], None, None]:
        raise NotImplementedError

    def content_get_early_date(self, blob: FileEntry) -> Optional[datetime]:
        raise NotImplementedError

    def content_get_early_dates(self, blobs: List[FileEntry]) -> Dict[bytes, datetime]:
        raise NotImplementedError

    def content_set_early_date(self, blob: FileEntry, date: datetime):
        raise NotImplementedError

    def directory_add_to_revision(
        self, revision: RevisionEntry, directory: DirectoryEntry, path: bytes
    ):
        raise NotImplementedError

    def directory_get_date_in_isochrone_frontier(
        self, directory: DirectoryEntry
    ) -> Optional[datetime]:
        raise NotImplementedError

    def directory_get_dates_in_isochrone_frontier(
        self, dirs: List[DirectoryEntry]
    ) -> Dict[bytes, datetime]:
        raise NotImplementedError

    def directory_invalidate_in_isochrone_frontier(self, directory: DirectoryEntry):
        raise NotImplementedError

    def directory_set_date_in_isochrone_frontier(
        self, directory: DirectoryEntry, date: datetime
    ):
        raise NotImplementedError

    def origin_get_id(self, origin: OriginEntry) -> int:
        raise NotImplementedError

    def revision_add(self, revision: RevisionEntry):
        raise NotImplementedError

    def revision_add_before_revision(
        self, relative: RevisionEntry, revision: RevisionEntry
    ):
        raise NotImplementedError

    def revision_add_to_origin(self, origin: OriginEntry, revision: RevisionEntry):
        raise NotImplementedError

    def revision_get_early_date(self, revision: RevisionEntry) -> Optional[datetime]:
        raise NotImplementedError

    def revision_get_prefered_origin(self, revision: RevisionEntry) -> int:
        raise NotImplementedError

    def revision_in_history(self, revision: RevisionEntry) -> bool:
        raise NotImplementedError

    def revision_set_prefered_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ):
        raise NotImplementedError

    def revision_visited(self, revision: RevisionEntry) -> bool:
        raise NotImplementedError


def directory_process_content(
    provenance: ProvenanceInterface, directory: DirectoryEntry, relative: DirectoryEntry
):
    stack = [(directory, b"")]
    while stack:
        current, prefix = stack.pop()
        for child in iter(current):
            if isinstance(child, FileEntry):
                # Add content to the relative directory with the computed prefix.
                provenance.content_add_to_directory(relative, child, prefix)
            else:
                # Recursively walk the child directory.
                stack.append((child, os.path.join(prefix, child.name)))


def origin_add(provenance: ProvenanceInterface, origin: OriginEntry):
    # TODO: refactor to iterate over origin visit statuses and commit only once
    # per status.
    origin.id = provenance.origin_get_id(origin)
    for revision in origin.revisions:
        origin_add_revision(provenance, origin, revision)
        # Commit after each revision
        provenance.commit()  # TODO: verify this!


def origin_add_revision(
    provenance: ProvenanceInterface, origin: OriginEntry, revision: RevisionEntry
):
    stack: List[Tuple[Optional[RevisionEntry], RevisionEntry]] = [(None, revision)]

    while stack:
        relative, current = stack.pop()

        # Check if current revision has no prefered origin and update if necessary.
        prefered = provenance.revision_get_prefered_origin(current)

        if prefered is None:
            provenance.revision_set_prefered_origin(origin, current)
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
            for parent in iter(current):
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
    provenance: ProvenanceInterface, archive: ArchiveInterface, revision: RevisionEntry
):
    assert revision.date is not None
    assert revision.root is not None
    # Processed content starting from the revision's root directory.
    date = provenance.revision_get_early_date(revision)
    if date is None or revision.date < date:
        provenance.revision_add(revision)
        # TODO: add file size filtering
        revision_process_content(
            provenance, revision, DirectoryEntry(archive, revision.root, b"")
        )
    # TODO: improve this! Maybe using a max attempt counter?
    # Idealy Provenance class should guarante that a commit never fails.
    while not provenance.commit():
        continue


class IsochroneNode:
    def __init__(self, entry: TreeEntry, provenance: ProvenanceInterface):
        self.entry = entry
        self.provenance = provenance
        self.is_dir = isinstance(self.entry, DirectoryEntry)
        if self.is_dir:
            assert isinstance(self.entry, DirectoryEntry)
            self.date = self.provenance.directory_get_date_in_isochrone_frontier(
                self.entry
            )
            self.children: List[IsochroneNode] = []
        else:
            assert isinstance(self.entry, FileEntry)
            self.date = self.provenance.content_get_early_date(self.entry)
        self.maxdate: Optional[datetime] = None

    def add_child(self, child: TreeEntry) -> "IsochroneNode":
        assert self.is_dir and self.date is None
        node = IsochroneNode(child, self.provenance)
        self.children.append(node)
        return node


def build_isochrone_graph(
    provenance: ProvenanceInterface, revision: RevisionEntry, directory: DirectoryEntry
):
    assert revision.date is not None
    # Build the nodes structure
    root = IsochroneNode(directory, provenance)
    stack = [root]
    while stack:
        current = stack.pop()
        assert isinstance(current.entry, DirectoryEntry)
        if current.date is None or current.date >= revision.date:
            # If current directory has an associated date in the isochrone frontier that
            # is greater or equal to the current revision's one, it should be ignored as
            # the revision is being processed out of order.
            if current.date is not None and current.date >= revision.date:
                provenance.directory_invalidate_in_isochrone_frontier(current.entry)
                current.date = None
            # Pre-query all known dates for content/directories in the current directory
            # for the provenance object to have them cached and (potentially) improve
            # performance.
            provenance.content_get_early_dates(
                [child for child in current.entry if isinstance(child, FileEntry)]
            )
            provenance.directory_get_dates_in_isochrone_frontier(
                [child for child in current.entry if isinstance(child, DirectoryEntry)]
            )
            for child in current.entry:
                node = current.add_child(child)
                if node.is_dir:
                    # Recursively analyse directory nodes.
                    stack.append(node)
    # Precalculate max known date for each node in the graph.
    stack = [root]
    while stack:
        current = stack.pop()
        if current.date is None:
            if any(map(lambda child: child.maxdate is None, current.children)):
                # Current node needs to be analysed again after its children.
                stack.append(current)
                for child in current.children:
                    if isinstance(child.entry, FileEntry):
                        if child.date is not None:
                            # File node that has been seen before, just use its known
                            # date.
                            child.maxdate = child.date
                        else:
                            # File node that has never been seen before, use current
                            # revision date.
                            child.maxdate = revision.date
                    else:
                        # Recursively analyse directory nodes.
                        stack.append(child)
            else:
                maxdates = []
                for child in current.children:
                    assert child.maxdate is not None
                    maxdates.append(child.maxdate)
                current.maxdate = max(maxdates) if maxdates else revision.date
        else:
            # Directory node in the frontier, just use its known date.
            current.maxdate = current.date
    return root


def revision_process_content(
    provenance: ProvenanceInterface, revision: RevisionEntry, root: DirectoryEntry
):
    assert revision.date is not None
    stack = [(build_isochrone_graph(provenance, revision, root), root.name)]
    while stack:
        current, path = stack.pop()
        if current.date is not None:
            assert current.date < revision.date
            # Current directory is an outer isochrone frontier for a previously
            # processed revision. It should be reused as is.
            provenance.directory_add_to_revision(revision, current.entry, path)
        else:
            # Current directory is not an outer isochrone frontier for any previous
            # revision. It might be eligible for this one.
            if is_new_frontier(current, revision):
                assert current.maxdate is not None
                # Outer frontier should be moved to current position in the isochrone
                # graph. This is the first time this directory is found in the isochrone
                # frontier.
                provenance.directory_set_date_in_isochrone_frontier(
                    current.entry, current.maxdate
                )
                provenance.directory_add_to_revision(revision, current.entry, path)
                directory_process_content(
                    provenance,
                    directory=current.entry,
                    relative=current.entry,
                )
            else:
                # No point moving the frontier here. Either there are no files or they
                # are being seen for the first time here. Add all blobs to current
                # revision updating date if necessary, and recursively analyse
                # subdirectories as canditates to the outer frontier.
                for child in current.children:
                    if isinstance(child.entry, FileEntry):
                        blob = child.entry
                        if child.date is None or revision.date < child.date:
                            provenance.content_set_early_date(blob, revision.date)
                        provenance.content_add_to_revision(revision, blob, path)
                    else:
                        stack.append((child, os.path.join(path, child.entry.name)))


def is_new_frontier(node: IsochroneNode, revision: RevisionEntry) -> bool:
    assert node.maxdate is not None and revision.date is not None
    # Using the following condition should we should get an algorithm equivalent to old
    # version where frontiers are pushed up in the tree whenever possible.
    return node.maxdate < revision.date
    # return has_blobs(node) and node.maxdate < revision.date


def has_blobs(node: IsochroneNode) -> bool:
    stack = [node]
    while stack:
        current = stack.pop()
        if any(map(lambda child: isinstance(child.entry, FileEntry), current.children)):
            return True
        else:
            # All children are directory entries.
            stack.extend(current.children)
    return False
