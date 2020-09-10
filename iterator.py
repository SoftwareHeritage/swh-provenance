import io
import psycopg2
import threading

from datetime import datetime
from swh.model.identifiers import identifier_to_bytes


class RevisionIterator:
    """Iterator interface."""

    def __iter__(self):
        pass

    def __next__(self):
        pass


class RevisionEntry:
    def __init__(self, swhid, timestamp, directory):
        self.swhid = swhid
        self.timestamp = timestamp
        self.directory = directory


class FileRevisionIterator(RevisionIterator):
    """Iterator over revisions present in the given CSV file."""

    def __init__(self, filename, limit=None):
        self.filename = filename
        self.limit = limit
        self.file = io.open(self.filename)
        self.idx = 0
        self.mutex = threading.Lock()

    def next(self):
        self.mutex.acquire()
        line = self.file.readline().strip()
        if line and (self.limit is None or self.idx < self.limit):
            self.idx = self.idx + 1
            swhid, timestamp, directory = line.strip().split(',')
            self.mutex.release()

            return RevisionEntry(
                identifier_to_bytes(swhid),
                datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S%z'),
                identifier_to_bytes(directory)
            )
        else:
            self.mutex.release()
            return None


class ArchiveRevisionIterator(RevisionIterator):
    """Iterator over revisions present in the given database."""

    def __init__(self, conn, limit=None, chunksize=100):
        self.cur = conn.cursor()
        self.chunksize = chunksize
        self.records = []
        if limit is None:
            self.cur.execute('''SELECT id, date, committer_date, directory
                            FROM revision''')
        else:
            self.cur.execute('''SELECT id, date, committer_date, directory
                            FROM revision
                            LIMIT %s''', (limit,))
        for row in self.cur.fetchmany(self.chunksize):
            record = self.make_record(row)
            if record is not None:
                self.records.append(record)
        self.mutex = threading.Lock()

    def __del__(self):
        self.cur.close()

    def next(self):
        self.mutex.acquire()
        if not self.records:
            self.records.clear()
            for row in self.cur.fetchmany(self.chunksize):
                record = self.make_record(row)
                if record is not None:
                    self.records.append(record)

        if self.records:
            revision, *self.records = self.records
            self.mutex.release()
            return revision
        else:
            self.mutex.release()
            return None

    def make_record(self, row):
        # Only revision with author or commiter date are considered
        if row[1] is not None:
            # If the revision has author date, it takes precedence
            return RevisionEntry(row[0], row[1], row[3])
        elif row[2] is not None:
            # If not, we use the commiter date
            return RevisionEntry(row[0], row[2], row[3])
