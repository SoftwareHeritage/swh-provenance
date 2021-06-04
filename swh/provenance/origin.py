from datetime import datetime, timezone
from itertools import islice
from typing import Iterable, Iterator, Optional, Tuple

import iso8601

from .model import OriginEntry

################################################################################
################################################################################


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
