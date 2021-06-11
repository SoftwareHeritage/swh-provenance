from datetime import datetime, timezone
from itertools import islice
import logging
import time
from typing import Iterable, Iterator, List, Optional, Tuple

import iso8601

from swh.model.hashutil import hash_to_hex

from .archive import ArchiveInterface
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
    origin.id = provenance.origin_get_id(origin)

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
