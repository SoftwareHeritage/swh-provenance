from datetime import datetime, timezone
from itertools import islice
import logging
import os
import time
from typing import Iterable, Iterator, List, Optional, Tuple

import iso8601

from swh.model.hashutil import hash_to_bytes
from swh.model.model import Sha1Git

from .archive import ArchiveInterface
from .graph import IsochroneNode, build_isochrone_graph
from .model import DirectoryEntry, RevisionEntry
from .provenance import ProvenanceInterface


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
    ):
        self.revisions: Iterator[Tuple[Sha1Git, datetime, Sha1Git]]
        if limit is not None:
            self.revisions = islice(revisions, limit)
        else:
            self.revisions = iter(revisions)

    def __iter__(self):
        return self

    def __next__(self):
        id, date, root = next(self.revisions)
        date = iso8601.parse_date(date)
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return RevisionEntry(
            hash_to_bytes(id),
            date=date,
            root=hash_to_bytes(root),
        )


def revision_add(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    revisions: List[RevisionEntry],
    trackall: bool = True,
    lower: bool = True,
    mindepth: int = 1,
    commit: bool = True,
) -> None:
    start = time.time()
    for revision in revisions:
        assert revision.date is not None
        assert revision.root is not None
        # Processed content starting from the revision's root directory.
        date = provenance.revision_get_early_date(revision)
        if date is None or revision.date < date:
            logging.debug(
                f"Processing revisions {revision.id.hex()}"
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
    if commit:
        provenance.commit()
    stop = time.time()
    logging.debug(
        f"Revisions {';'.join([revision.id.hex() for revision in revisions])} "
        f" were processed in {stop - start} secs (commit took {stop - done} secs)!"
    )
    # logging.critical(
    #     ";".join([revision.id.hex() for revision in revisions])
    #     + f",{stop - start},{stop - done}"
    # )


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
                    flatten_directory(archive, provenance, current.entry)
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
