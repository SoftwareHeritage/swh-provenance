# import psycopg2
from swh.model.identifiers import identifier_to_str


# def typecast_bytea(value, cur):
#     if value is not None:
#         data = psycopg2.BINARY(value, cur)
#         return data.tobytes()


class RevisionIterator:
    """Iterator over revisions present in the given database."""

    # def adapt_conn(self, conn):
    #     """Makes psycopg2 use 'bytes' to decode bytea instead of
    #     'memoryview', for this connection."""
    #     t_bytes = psycopg2.extensions.new_type((17,), "bytea", typecast_bytea)
    #     psycopg2.extensions.register_type(t_bytes, conn)

    #     t_bytes_array = psycopg2.extensions.new_array_type((1001,), "bytea[]", t_bytes)
    #     psycopg2.extensions.register_type(t_bytes_array, conn)

    def __init__(self, conn, limit=None, chunksize=100):
        # self.adapt_conn(conn)
        self.cur = conn.cursor()
        self.chunksize = chunksize
        self.limit = limit
        self.records = []
        self.aliases = ['id', 'date', 'dir']

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
            return dict(zip(self.aliases, (row[0], row[1], row[3])))
        elif row[2] is not None:
            # If not, we use the commiter date
            return dict(zip(self.aliases, (row[0], row[2], row[3])))
