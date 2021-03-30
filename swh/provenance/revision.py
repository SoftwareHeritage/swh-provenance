from datetime import datetime, timezone
from itertools import islice
import threading
from typing import Iterable, Iterator, Optional, Tuple

import iso8601

from swh.model.hashutil import hash_to_bytes
from swh.provenance.archive import ArchiveInterface
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
        archive: ArchiveInterface,
        limit: Optional[int] = None,
    ):
        self.revisions: Iterator[Tuple[bytes, datetime, bytes]]
        if limit is not None:
            self.revisions = islice(revisions, limit)
        else:
            self.revisions = iter(revisions)
        self.mutex = threading.Lock()
        self.archive = archive

    def __iter__(self):
        return self

    def __next__(self):
        with self.mutex:
            id, date, root = next(self.revisions)
            date = iso8601.parse_date(date)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            return RevisionEntry(
                hash_to_bytes(id), date=date, root=hash_to_bytes(root),
            )


# class ArchiveRevisionIterator(RevisionIterator):
#     """Iterator over revisions present in the given database."""
#
#     def __init__(self, conn, limit=None, chunksize=100):
#         self.cur = conn.cursor()
#         self.chunksize = chunksize
#         self.records = []
#         if limit is None:
#             self.cur.execute('''SELECT id, date, committer_date, directory
#                             FROM revision''')
#         else:
#             self.cur.execute('''SELECT id, date, committer_date, directory
#                             FROM revision
#                             LIMIT %s''', (limit,))
#         for row in self.cur.fetchmany(self.chunksize):
#             record = self.make_record(row)
#             if record is not None:
#                 self.records.append(record)
#         self.mutex = threading.Lock()
#
#     def __del__(self):
#         self.cur.close()
#
#     def next(self):
#         self.mutex.acquire()
#         if not self.records:
#             self.records.clear()
#             for row in self.cur.fetchmany(self.chunksize):
#                 record = self.make_record(row)
#                 if record is not None:
#                     self.records.append(record)
#
#         if self.records:
#             revision, *self.records = self.records
#             self.mutex.release()
#             return revision
#         else:
#             self.mutex.release()
#             return None
#
#     def make_record(self, row):
#         # Only revision with author or committer date are considered
#         if row[1] is not None:
#             # If the revision has author date, it takes precedence
#             return RevisionEntry(row[0], row[1], row[3])
#         elif row[2] is not None:
#             # If not, we use the committer date
#             return RevisionEntry(row[0], row[2], row[3])


########################################################################################
########################################################################################

# class RevisionWorker(threading.Thread):
#     def __init__(
#         self,
#         id: int,
#         conninfo: dict,
#         archive: ArchiveInterface,
#         revisions: RevisionIterator
#     ):
#         from .provenance import get_provenance
#
#         super().__init__()
#         self.archive = archive
#         self.id = id
#         self.provenance = get_provenance(conninfo)
#         self.revisions = revisions
#
#
#     def run(self):
#         from .provenance import revision_add
#
#
#         while True:
#             revision = self.revisions.next()
#             if revision is None: break
#
#             processed = False
#             while not processed:
#                 logging.info(
#                     f'Thread {(
#                         self.id
#                     )} - Processing revision {(
#                         hash_to_hex(revision.id)
#                     )} (timestamp: {revision.date})'
#                 )
#                 processed = revision_add(self.provenance, self.archive, revision)
#                 if not processed:
#                     logging.warning(
#                         f'Thread {(
#                              self.id
#                         )} - Failed to process revision {(
#                             hash_to_hex(revision.id)
#                         )} (timestamp: {revision.date})'
#                     )
