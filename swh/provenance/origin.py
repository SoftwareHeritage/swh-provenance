from datetime import datetime, timezone
from itertools import islice
import logging
import time
from typing import Iterable, Iterator, List, Optional, Tuple

import iso8601

from .archive import ArchiveInterface
from .graph import HistoryNode, build_history_graph
from .model import OriginEntry, RevisionEntry
from .provenance import ProvenanceInterface


class CSVOriginIterator:
    """Iterator over origin visit statuses typically present in the given CSV
    file.

    The input is an iterator that produces 3 elements per row:

      (url, date, snap)

    where:
    - url: is the origin url of the visit
    - date: is the date of the visit
    - snap: sha1_git of the snapshot pointed by the visit status
    """

    def __init__(
        self,
        statuses: Iterable[Tuple[str, datetime, bytes]],
        limit: Optional[int] = None,
    ):
        self.statuses: Iterator[Tuple[str, datetime, bytes]]
        if limit is not None:
            self.statuses = islice(statuses, limit)
        else:
            self.statuses = iter(statuses)

    def __iter__(self):
        for url, date, snap in self.statuses:
            date = iso8601.parse_date(date, default_timezone=timezone.utc)
            yield OriginEntry(url, date, snap)


def origin_add(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    origins: List[OriginEntry],
):
    start = time.time()
    for origin in origins:
        origin.retrieve_revisions(archive)
        for revision in origin.revisions:
            graph = build_history_graph(archive, provenance, revision)
            origin_add_revision(provenance, origin, graph)
    done = time.time()
    provenance.commit()
    stop = time.time()
    logging.debug(
        "Origins "
        ";".join([origin.url + ":" + origin.snapshot.hex() for origin in origins])
        + f" were processed in {stop - start} secs (commit took {stop - done} secs)!"
    )


def origin_add_revision(
    provenance: ProvenanceInterface,
    origin: OriginEntry,
    graph: HistoryNode,
):
    origin.id = provenance.origin_get_id(origin)

    # head is treated separately since it should always be added to the given origin
    head = graph.entry
    check_preferred_origin(provenance, origin, head)
    provenance.revision_add_to_origin(origin, head)

    # head's history should be recursively iterated starting from its parents
    stack = list(graph.parents)
    while stack:
        current = stack.pop()
        check_preferred_origin(provenance, origin, current.entry)

        if current.visited:
            # if current revision was already visited just add it to the current origin
            # and stop recursion (its history has already been flattened)
            provenance.revision_add_to_origin(origin, current.entry)
        else:
            # if current revision was not visited before create a link between it and
            # the head, and recursively walk its history
            provenance.revision_add_before_revision(head, current.entry)
            for parent in current.parents:
                stack.append(parent)


def check_preferred_origin(
    provenance: ProvenanceInterface,
    origin: OriginEntry,
    revision: RevisionEntry,
):
    # if the revision has no preferred origin just set the given origin as the
    # preferred one. TODO: this should be improved in the future!
    preferred = provenance.revision_get_preferred_origin(revision)
    if preferred is None:
        provenance.revision_set_preferred_origin(origin, revision)
