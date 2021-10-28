# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone
import os
from typing import Generator, Iterable, Iterator, List, Optional, Tuple

from swh.core.statsd import statsd
from swh.model.model import Sha1Git

from .archive import ArchiveInterface
from .graph import IsochroneNode, build_isochrone_graph
from .interface import ProvenanceInterface
from .model import DirectoryEntry, RevisionEntry

REVISION_DURATION_METRIC = "swh_provenance_revision_content_layer_duration_seconds"


class CSVRevisionIterator:
    """Iterator over revisions typically present in the given CSV file.

    The input is an iterator that produces 3 elements per row:

      (id, date, root)

    where:
    - id: is the id (sha1_git) of the revision
    - date: is the author date
    - root: sha1 of the directory
    """

    def __init__(
        self,
        revisions: Iterable[Tuple[Sha1Git, datetime, Sha1Git]],
        limit: Optional[int] = None,
    ) -> None:
        self.revisions: Iterator[Tuple[Sha1Git, datetime, Sha1Git]]
        if limit is not None:
            from itertools import islice

            self.revisions = islice(revisions, limit)
        else:
            self.revisions = iter(revisions)

    def __iter__(self) -> Generator[RevisionEntry, None, None]:
        for id, date, root in self.revisions:
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            yield RevisionEntry(id, date=date, root=root)


@statsd.timed(metric=REVISION_DURATION_METRIC, tags={"method": "main"})
def revision_add(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    revisions: List[RevisionEntry],
    trackall: bool = True,
    lower: bool = True,
    mindepth: int = 1,
    minsize: int = 0,
    commit: bool = True,
) -> None:
    for revision in revisions:
        assert revision.date is not None
        assert revision.root is not None
        # Processed content starting from the revision's root directory.
        date = provenance.revision_get_date(revision)
        if date is None or revision.date < date:
            graph = build_isochrone_graph(
                archive,
                provenance,
                revision,
                DirectoryEntry(revision.root),
                minsize=minsize,
            )
            revision_process_content(
                archive,
                provenance,
                revision,
                graph,
                trackall=trackall,
                lower=lower,
                mindepth=mindepth,
                minsize=minsize,
            )
    if commit:
        provenance.flush()


@statsd.timed(metric=REVISION_DURATION_METRIC, tags={"method": "process_content"})
def revision_process_content(
    archive: ArchiveInterface,
    provenance: ProvenanceInterface,
    revision: RevisionEntry,
    graph: IsochroneNode,
    trackall: bool = True,
    lower: bool = True,
    mindepth: int = 1,
    minsize: int = 0,
) -> None:
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
            assert current.maxdate is not None
            # Current directory is not an outer isochrone frontier for any previous
            # revision. It might be eligible for this one.
            if is_new_frontier(
                current,
                revision=revision,
                trackall=trackall,
                lower=lower,
                mindepth=mindepth,
            ):
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
                    flatten_directory(
                        archive, provenance, current.entry, minsize=minsize
                    )
            else:
                # If current node is an invalidated frontier, update its date for future
                # revisions to get the proper value.
                if current.invalid:
                    provenance.directory_set_date_in_isochrone_frontier(
                        current.entry, current.maxdate
                    )
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


@statsd.timed(metric=REVISION_DURATION_METRIC, tags={"method": "flatten_directory"})
def flatten_directory(
    archive: ArchiveInterface,
    provenance: ProvenanceInterface,
    directory: DirectoryEntry,
    minsize: int = 0,
) -> None:
    """Recursively retrieve all the files of 'directory' and insert them in the
    'provenance' database in the 'content_to_directory' table.
    """
    stack = [(directory, b"")]
    while stack:
        current, prefix = stack.pop()
        current.retrieve_children(archive, minsize=minsize)
        for f_child in current.files:
            # Add content to the directory with the computed prefix.
            provenance.content_add_to_directory(directory, f_child, prefix)
        for d_child in current.dirs:
            # Recursively walk the child directory.
            stack.append((d_child, os.path.join(prefix, d_child.name)))


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
        # The only real condition for a directory to be a frontier is that its content
        # is already known and its maxdate is less (or equal) than current revision's
        # date. Checking mindepth is meant to skip root directories (or any arbitrary
        # depth) to improve the result. The option lower tries to maximize the reuse
        # rate of previously defined  frontiers by keeping them low in the directory
        # tree.
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
        # every blob outside a frontier to have an strictly earlier date.
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
