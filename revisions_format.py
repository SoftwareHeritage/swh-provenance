#!/usr/bin/env python

import io
import json
import sys

from swh.model.hashutil import hash_to_hex, hash_to_bytes
from swh.provenance.postgresql.db_utils import connect


conninfo = {
    "host": "db.internal.softwareheritage.org",
    "dbname": "softwareheritage",
    "user": "guest",
}


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: revisions_format <infile> <outfile>")
        exit(-1)

    print(f"Connection to database: {conninfo}...")
    conn = connect(conninfo)

    infilename = sys.argv[1]
    outfilename = sys.argv[2]

    with io.open(infilename) as infile:
        with io.open(outfilename, "w") as outfile:
            ids = json.loads(infile.read())
            print(f"Formatting {len(ids)} revisions")
            for id in ids:
                cursor = conn.cursor()
                cursor.execute(
                    """SELECT id, date, directory
                         FROM revision
                         WHERE id=%s AND date IS NOT NULL""",
                    (hash_to_bytes(id),),
                )
                rev = cursor.fetchone()
                assert rev is not None
                outfile.write(f"{hash_to_hex(rev[0])},{rev[1]},{hash_to_hex(rev[2])}\n")
