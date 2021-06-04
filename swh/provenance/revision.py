from datetime import datetime, timezone
from itertools import islice
from typing import Iterable, Iterator, Optional, Tuple

import iso8601

from swh.model.hashutil import hash_to_bytes
from swh.provenance.model import RevisionEntry

########################################################################################
########################################################################################


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
        revisions: Iterable[Tuple[bytes, datetime, bytes]],
        limit: Optional[int] = None,
    ):
        self.revisions: Iterator[Tuple[bytes, datetime, bytes]]
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
