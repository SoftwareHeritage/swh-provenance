# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from itertools import islice
import logging
import time
from typing import Generator, Iterable, Iterator, List, Optional, Tuple

from swh.model.model import Sha1Git

from .archive import ArchiveInterface
from .graph import HistoryGraph
from .interface import ProvenanceInterface
from .model import OriginEntry, RevisionEntry


class CSVOriginIterator:
    """Iterator over origin visit statuses typically present in the given CSV
    file.

    The input is an iterator that produces 2 elements per row:

      (url, snap)

    where:
    - url: is the origin url of the visit
    - snap: sha1_git of the snapshot pointed by the visit status
    """

    def __init__(
        self,
        statuses: Iterable[Tuple[str, Sha1Git]],
        limit: Optional[int] = None,
    ) -> None:
        self.statuses: Iterator[Tuple[str, Sha1Git]]
        if limit is not None:
            self.statuses = islice(statuses, limit)
        else:
            self.statuses = iter(statuses)

    def __iter__(self) -> Generator[OriginEntry, None, None]:
        return (OriginEntry(url, snapshot) for url, snapshot in self.statuses)


def origin_add(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    origins: List[OriginEntry],
) -> None:
    start = time.time()
    for origin in origins:
        provenance.origin_add(origin)
        origin.retrieve_revisions(archive)
        for revision in origin.revisions:
            graph = HistoryGraph(archive, provenance, revision)
            origin_add_revision(provenance, origin, graph)
    done = time.time()
    provenance.flush()
    stop = time.time()
    logging.debug(
        "Origins %s were processed in %s secs (commit took %s secs)!",
        ";".join(origin.id.hex() + ":" + origin.snapshot.hex() for origin in origins),
        stop - start,
        stop - done,
    )


def origin_add_revision(
    provenance: ProvenanceInterface,
    origin: OriginEntry,
    graph: HistoryGraph,
) -> None:
    # XXX: simplified version of the origin-revision algorithm. This is generating flat
    # models for the history of all head revisions. No previous result is reused now!
    # The previous implementation was missing some paths from origins to certain
    # revisions due to a wrong reuse logic.

    # head is treated separately since it should always be added to the given origin
    check_preferred_origin(provenance, origin, graph.head.entry)
    provenance.revision_add_to_origin(origin, graph.head.entry)
    visited = {graph.head}

    # head's history should be recursively iterated starting from its parents
    stack = list(graph.parents[graph.head])
    while stack:
        current = stack.pop()
        check_preferred_origin(provenance, origin, current.entry)

        # create a link between it and the head, and recursively walk its history
        provenance.revision_add_before_revision(graph.head.entry, current.entry)
        visited.add(current)
        for parent in graph.parents[current]:
            if parent not in visited:
                stack.append(parent)


def check_preferred_origin(
    provenance: ProvenanceInterface,
    origin: OriginEntry,
    revision: RevisionEntry,
) -> None:
    # if the revision has no preferred origin just set the given origin as the
    # preferred one. TODO: this should be improved in the future!
    preferred = provenance.revision_get_preferred_origin(revision)
    if preferred is None:
        provenance.revision_set_preferred_origin(origin, revision)
