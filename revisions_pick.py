#!/usr/bin/env python

import io
import sys

from swh.model.hashutil import hash_to_hex
from swh.provenance.postgresql.db_utils import connect


conninfo = {
    "host": "db.internal.softwareheritage.org",
    "dbname": "softwareheritage",
    "user": "guest"
}


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print('usage: listrevs <count> <outfile>')
        exit(-1)

    count = int(sys.argv[1])
    filename = sys.argv[2]

    print(f'Connection to database: {conninfo}...')
    conn = connect(conninfo)

    cursor = conn.cursor()
    cursor.execute('''SELECT COUNT(*) FROM revision''')
    total = cursor.fetchone()[0]

    probability = count / total * 100
    print(f"Requesting {count} revisions out of {total} (probability {probability}).")
    cursor.execute('''SELECT id, date, directory FROM revision TABLESAMPLE BERNOULLI(%s)''',
                      (probability,))
    revisions = [row for row in cursor.fetchall() if row[1] is not None]
    revisions.sort(key=lambda rev: rev[1])
    # assert len(revisions) >= count

    print(f"Filtering first {count} of {len(revisions)} obtained.")
    with io.open(filename, 'w') as outfile:
        for rev in revisions[:count]:
            outfile.write(f'{hash_to_hex(rev[0])},{rev[1]},{hash_to_hex(rev[2])}\n')
