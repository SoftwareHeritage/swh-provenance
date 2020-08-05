import psycopg2

from swh.model.identifiers import identifier_to_str


class RevisionIterator:
    """Iterator over revisions present in the given database."""

    def __init__(self, conn, limit=None, chunksize=100):
        self.cur = conn.cursor()
        self.chunksize = chunksize
        self.limit = limit
        self.records = []

    def __del__(self):
        self.cur.close()

    def __iter__(self):
        self.records.clear()
        if self.limit is None:
            self.cur.execute('''SELECT id, date, committer_date, directory
                            FROM revision''')
        else:
            self.cur.execute('''SELECT id, date, committer_date, directory
                            FROM revision
                            LIMIT %s''', (self.limit,))
        for row in self.cur.fetchmany(self.chunksize):
            record = self.make_record(row)
            if record is not None:
                self.records.append(record)
        return self

    def __next__(self):
        if not self.records:
            self.records.clear()
            for row in self.cur.fetchmany(self.chunksize):
                record = self.make_record(row)
                if record is not None:
                    self.records.append(record)

        if self.records:
            revision, *self.records = self.records
            return revision
        else:
            raise StopIteration

    def make_record(self, row):
        # Only revision with author or commiter date are considered
        if row[1] is not None:
            # If the revision has author date, it takes precedence
            return RevisionEntry(row[0], row[1], row[3])
        elif row[2] is not None:
            # If not, we use the commiter date
            return RevisionEntry(row[0], row[2], row[3])


class RevisionEntry:
    def __init__(self, swhid, timestamp, directory):
        self.swhid = swhid
        self.timestamp = timestamp
        self.directory = directory
