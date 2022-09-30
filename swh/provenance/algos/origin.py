# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
from itertools import islice
import logging
from typing import Any, Dict, Generator, Iterable, Iterator, List, Optional, Set, Tuple

from swh.core.statsd import statsd
from swh.model.model import Sha1Git
from swh.provenance.archive import ArchiveInterface
from swh.provenance.interface import ProvenanceInterface
from swh.provenance.model import OriginEntry, RevisionEntry

ORIGIN_DURATION_METRIC = "swh_provenance_origin_duration_seconds"

LOG_FORMAT = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)

LOGGER = logging.getLogger(__name__)


class HistoryGraph:
    @statsd.timed(metric=ORIGIN_DURATION_METRIC, tags={"method": "HistoryGraph"})
    def __init__(
        self,
        archive: ArchiveInterface,
        revision: RevisionEntry,
    ) -> None:
        self.head_id = revision.id
        self._nodes: Set[Sha1Git] = set()
        # rev -> set(parents)
        self._edges: Dict[Sha1Git, Set[Sha1Git]] = {}

        stack = {self.head_id}
        while stack:
            current = stack.pop()

            if current not in self._nodes:
                self._nodes.add(current)
                self._edges.setdefault(current, set())
                for rev, parent in archive.revision_get_some_outbound_edges(current):
                    self._nodes.add(rev)
                    self._edges.setdefault(rev, set()).add(parent)
                    stack.add(parent)

            # don't process nodes for which we've already retrieved outbound edges
            stack -= self._nodes

    def parent_ids(self) -> Set[Sha1Git]:
        """Get all the known parent ids in the current graph"""
        return self._nodes - {self.head_id}

    def __str__(self) -> str:
        return f"<HistoryGraph: head={self.head_id.hex()}, edges={self._edges}"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "head": self.head_id.hex(),
            "graph": {
                node.hex(): sorted(parent.hex() for parent in parents)
                for node, parents in self._edges.items()
            },
        }


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
        process_origin(provenance, archive, origin)
    if commit:
        start = datetime.now()
        LOGGER.debug("Flushing cache")
        provenance.flush()
        LOGGER.info("Cache flushed in %s", (datetime.now() - start))


@statsd.timed(metric=ORIGIN_DURATION_METRIC, tags={"method": "process_origin"})
def process_origin(
    provenance: ProvenanceInterface, archive: ArchiveInterface, origin: OriginEntry
) -> None:
    LOGGER.info("Processing origin=%s", origin)
    start = datetime.now()

    LOGGER.debug("Add origin")
    provenance.origin_add(origin)

    LOGGER.debug("Retrieving head revisions")
    origin.retrieve_revisions(archive)
    LOGGER.info("%d heads founds", origin.revision_count)

    for idx, revision in enumerate(origin.revisions):
        LOGGER.info(
            "checking revision %s (%d/%d)", revision, idx + 1, origin.revision_count
        )

        if not provenance.revision_is_head(revision):
            LOGGER.debug("revision %s not in heads", revision)

            graph = HistoryGraph(archive, revision)
            LOGGER.debug("History graph built")

            origin_add_revision(provenance, origin, graph)
            LOGGER.debug("Revision added")

        # head is treated separately
        LOGGER.debug("Checking preferred origin")
        check_preferred_origin(provenance, origin, revision.id)

        LOGGER.debug("Adding revision to origin")
        provenance.revision_add_to_origin(origin, revision)

        cache_flush_start = datetime.now()
        if provenance.flush_if_necessary():
            LOGGER.info(
                "Intermediate cache flush in %s", (datetime.now() - cache_flush_start)
            )

    end = datetime.now()
    LOGGER.info("Processed origin %s in %s", origin.url, (end - start))


@statsd.timed(metric=ORIGIN_DURATION_METRIC, tags={"method": "process_revision"})
def origin_add_revision(
    provenance: ProvenanceInterface,
    origin: OriginEntry,
    graph: HistoryGraph,
) -> None:
    for parent_id in graph.parent_ids():
        check_preferred_origin(provenance, origin, parent_id)

        # create a link between it and the head, and recursively walk its history
        provenance.revision_add_before_revision(
            head_id=graph.head_id, revision_id=parent_id
        )


@statsd.timed(metric=ORIGIN_DURATION_METRIC, tags={"method": "check_preferred_origin"})
def check_preferred_origin(
    provenance: ProvenanceInterface,
    origin: OriginEntry,
    revision_id: Sha1Git,
) -> None:
    # if the revision has no preferred origin just set the given origin as the
    # preferred one. TODO: this should be improved in the future!
    preferred = provenance.revision_get_preferred_origin(revision_id)
    if preferred is None:
        provenance.revision_set_preferred_origin(origin, revision_id)
