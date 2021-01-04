import threading

from .archive import ArchiveInterface

from datetime import datetime
from typing import Optional

from swh.model.hashutil import hash_to_bytes


class RevisionEntry:
    def __init__(
        self,
        archive: ArchiveInterface,
        id: bytes,
        date: Optional[datetime] = None,
        root: Optional[bytes] = None,
        parents: Optional[list] = None
    ):
        self.archive = archive
        self.id = id
        self.date = date
        self.parents = parents
        self.root = root

    def __iter__(self):
        if self.parents is None:
            self.parents = []
            for parent in self.archive.revision_get([self.id]):
                if parent is not None:
                    self.parents.append(
                        RevisionEntry(
                            self.archive,
                            parent.id,
                            parents=[
                                RevisionEntry(self.archive, id) for id in parent.parents
                            ]
                        )
                    )

        return iter(self.parents)


########################################################################################
########################################################################################


class RevisionIterator:
    """Iterator interface."""

    def __iter__(self):
        pass

    def __next__(self):
        pass


class FileRevisionIterator(RevisionIterator):
    """Iterator over revisions present in the given CSV file."""

    def __init__(
        self, filename: str, archive: ArchiveInterface, limit: Optional[int] = None
    ):
        self.file = open(filename)
        self.idx = 0
        self.limit = limit
        self.mutex = threading.Lock()
        self.archive = archive

    def next(self):
        self.mutex.acquire()
        line = self.file.readline().strip()
        if line and (self.limit is None or self.idx < self.limit):
            self.idx = self.idx + 1
            id, date, root = line.strip().split(",")
            self.mutex.release()

            return RevisionEntry(
                self.archive,
                hash_to_bytes(id),
                date=datetime.fromisoformat(date),
                root=hash_to_bytes(root)
            )
        else:
            self.mutex.release()
            return None


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
#         # Only revision with author or commiter date are considered
#         if row[1] is not None:
#             # If the revision has author date, it takes precedence
#             return RevisionEntry(row[0], row[1], row[3])
#         elif row[2] is not None:
#             # If not, we use the commiter date
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
