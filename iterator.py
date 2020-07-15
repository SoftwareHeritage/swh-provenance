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
            self.cur.execute('''SELECT id, date, directory
                            FROM revision''')
            # self.cur.execute('''SELECT id, date, directory
            #                 FROM revision ORDER BY date''')
        else:
            self.cur.execute('''SELECT id, date, directory
                            FROM revision
                            LIMIT %s''', (self.limit,))
            # self.cur.execute('''SELECT id, date, directory
            #                 FROM revision ORDER BY date
            #                 LIMIT %s''', (self.limit,))
        for row in self.cur.fetchmany(self.chunksize):
            record = dict(zip(self.aliases, row))
            self.records.append(record)
        return self

    def __next__(self):
        if not self.records:
            self.records.clear()
            for row in self.cur.fetchmany(self.chunksize):
                record = dict(zip(self.aliases, row))
                self.records.append(record)
                # self.records.append((
                #     identifier_to_str(rev[0]),
                #     rev[1],
                #     identifier_to_str(rev[2])
                # ))

        if self.records:
            revision, *self.records = self.records
            return revision
        else:
            raise StopIteration
