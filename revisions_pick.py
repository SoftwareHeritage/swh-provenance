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
# conninfo = 'postgresql://guest@db.internal.softwareheritage.org/softwareheritage'


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print('usage: listrevs <count> <outfile>')
        exit(-1)

    count = int(sys.argv[1])
    filename = sys.argv[2]

    print(f'Connection to database: {conninfo}...')
    conn = connect(conninfo)

    low = b'\x0150352e5a43c5b9368990e1dfe0c1510f86de73'
    high = b'\xffecf10c8c0106a8d66718e29aa6604df441704e'
    limit = count

    revcur = conn.cursor()
    revcur.execute('''SELECT id FROM revision
                      WHERE id BETWEEN %s AND %s LIMIT %s''',
                      (low, high, limit))

    ids = []
    for revision in revcur.fetchall():
        ids.append(revision)

        parcur = conn.cursor()
        parcur.execute('''SELECT parent_id FROM revision_history
                          WHERE id=%s''',
                          (revision))
        ids.extend(parcur.fetchall())

    # Remove duplicates
    ids = list(dict().fromkeys(ids))
    print(f"Found {len(ids)} distinct revisions.")

    revcur.execute('''SELECT id, date, directory FROM revision
                      WHERE id IN %s AND date IS NOT NULL''',
                      (tuple(ids),))
    revisions = list(revcur.fetchall())
    revisions.sort(key=lambda rev: rev[1])
    assert len(revisions) >= count

    print(f"Filtering first {count}.")
    with io.open(filename, 'w') as outfile:
        for rev in revisions[:count]:
            outfile.write(f'{hash_to_hex(rev[0])},{rev[1]},{hash_to_hex(rev[2])}\n')
