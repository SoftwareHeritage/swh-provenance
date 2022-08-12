# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone
import logging
from typing import Generator, Iterable, Iterator, List, Optional, Tuple

from swh.core.statsd import statsd
from swh.model.model import Sha1Git

from .archive import ArchiveInterface
from .directory import directory_flatten
from .graph import IsochroneNode, build_isochrone_graph
from .interface import ProvenanceInterface
from .model import DirectoryEntry, RevisionEntry

REVISION_DURATION_METRIC = "swh_provenance_revision_content_layer_duration_seconds"

logger = logging.getLogger(__name__)

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


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
    flatten: bool = True,
    lower: bool = True,
    mindepth: int = 1,
    minsize: int = 0,
    commit: bool = True,
) -> None:
    revs_processed = 0
    batch_size = len(revisions)
    for batch_pos, revision in enumerate(
        sorted(revisions, key=lambda r: r.date or EPOCH)
    ):
        assert revision.date is not None
        assert revision.root is not None
        # Processed content starting from the revision's root directory.
        date = provenance.revision_get_date(revision)
        if date is None or revision.date < date:
            logger.debug(
                "Processing revision %s on %s (root %s)",
                revision.id.hex(),
                revision.date,
                revision.root.hex(),
            )
            logger.debug("provenance date: %s, building isochrone graph", date)
            graph = build_isochrone_graph(
                provenance,
                archive,
                revision,
                DirectoryEntry(revision.root),
                minsize=minsize,
            )
            logger.debug("isochrone graph built, processing content")
            revision_process_content(
                provenance,
                archive,
                revision,
                graph,
                trackall=trackall,
                flatten=flatten,
                lower=lower,
                mindepth=mindepth,
                minsize=minsize,
            )
            revs_processed += 1
            if commit:
                flushed = provenance.flush_if_necessary()
                if flushed:
                    logger.debug(
                        "flushed (rev %s/%s, processed %s)",
                        batch_pos + 1,
                        batch_size,
                        revs_processed,
                    )
    if commit:
        logger.debug("flushing batch")
        provenance.flush()


@statsd.timed(metric=REVISION_DURATION_METRIC, tags={"method": "process_content"})
def revision_process_content(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    revision: RevisionEntry,
    graph: IsochroneNode,
    trackall: bool = True,
    flatten: bool = True,
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
            assert current.dbdate < revision.date
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
                    if flatten:
                        directory_flatten(
                            provenance, archive, current.entry, minsize=minsize
                        )
            else:
                # If current node is an invalidated frontier, update its date for future
                # revisions to get the proper value.
                if current.invalid:
                    provenance.directory_set_date_in_isochrone_frontier(
                        current.entry, revision.date
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


def is_new_frontier(
    node: IsochroneNode,
    revision: RevisionEntry,
    lower: bool = True,
    mindepth: int = 1,
) -> bool:
    assert node.maxdate is not None  # for mypy
    assert revision.date is not None  # idem
    # We want to ensure that all first occurrences end up in the content_early_in_rev
    # relation. Thus, we force for every blob outside a frontier to have an strictly
    # earlier date.
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
    #     if any(current.entry.files):
    #         return True
    #     else:
    #         # All children are directory entries.
    #         stack.extend(current.children)
    # return False
    # 3. Files in the intermediate directories between current node and any previously
    #    defined frontier:
    # TODO: complete this case!
