# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from datetime import datetime

from itertools import islice
from typing import Generator, Iterable, Iterator, List, Optional, Tuple

from swh.core.statsd import statsd
from swh.model.model import Sha1Git

from .archive import ArchiveInterface
from .graph import HistoryGraph
from .interface import ProvenanceInterface
from .model import OriginEntry, RevisionEntry


ORIGIN_DURATION_METRIC = "swh_provenance_origin_revision_layer_duration_seconds"

LOG_FORMAT = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)

LOGGER = logging.getLogger(__name__)


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


@statsd.timed(metric=ORIGIN_DURATION_METRIC, tags={"method": "main"})
def origin_add(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    origins: List[OriginEntry],
    commit: bool = True,
) -> None:
    for origin in origins:
        proceed_origin(provenance, archive, origin)
    if commit:
        provenance.flush()


@statsd.timed(metric=ORIGIN_DURATION_METRIC, tags={"method": "proceed_origin"})
def proceed_origin(
    provenance: ProvenanceInterface, archive: ArchiveInterface, origin: OriginEntry
) -> None:
    LOGGER.info("Processing origin %s", origin.url)
    start = datetime.now()
    provenance.origin_add(origin)
    origin.retrieve_revisions(archive)
    for revision in origin.revisions:
        if not provenance.revision_is_head(revision):
            graph = HistoryGraph(archive, revision)
            origin_add_revision(provenance, origin, graph)
        # head is treated separately
        check_preferred_origin(provenance, origin, revision)
        provenance.revision_add_to_origin(origin, revision)
    end = datetime.now()
    LOGGER.info("Processed origin %s in %s", origin.url, (end - start))


@statsd.timed(metric=ORIGIN_DURATION_METRIC, tags={"method": "process_revision"})
def origin_add_revision(
    provenance: ProvenanceInterface,
    origin: OriginEntry,
    graph: HistoryGraph,
) -> None:
    visited = {graph.head}
    # head's history should be recursively iterated starting from its parents
    stack = list(graph.parents[graph.head])
    while stack:
        current = stack.pop()
        check_preferred_origin(provenance, origin, current)

        # create a link between it and the head, and recursively walk its history
        provenance.revision_add_before_revision(graph.head, current)
        visited.add(current)
        for parent in graph.parents[current]:
            if parent not in visited:
                stack.append(parent)


@statsd.timed(metric=ORIGIN_DURATION_METRIC, tags={"method": "check_preferred_origin"})
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
