#!/usr/bin/env python

import io
import sys

import psycopg2
from swh.core.db import BaseDb
from swh.model.hashutil import hash_to_bytes, hash_to_hex

conninfo = {
    "host": "db.internal.softwareheritage.org",
    "dbname": "softwareheritage",
    "user": "guest",
}


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: listrevs <outfile>")
        exit(-1)

    filename = sys.argv[1]

    print(f"Connection to database: {conninfo}...")
    conn: psycopg2.connection = BaseDb.connect(**conninfo).conn
    BaseDb.adapt_conn(conn)
    cursor = conn.cursor()

    revisions = set(
        [
            hash_to_bytes("1363496c1106606684d40447f5d1149b2c66a9f8"),
            hash_to_bytes("b91a781cbc1285d441aa682926d93d8c23678b0b"),
            hash_to_bytes("313315d9790c36e22bb5bb034e9c7d7f470cdf73"),
            hash_to_bytes("a3b54f0f5de1ad17889fd23aee7c230eefc300cd"),
            hash_to_bytes("74deb33d12bf275a3b3a9afc833f4760be90f031"),
        ]
    )
    pending = revisions

    while pending:
        cursor.execute(
            """SELECT parent_id FROM revision_history WHERE id IN %s""",
            (tuple(pending),),
        )
        parents = set(map(lambda row: row[0], cursor.fetchall()))
        pending = parents - revisions
        revisions = revisions | parents

    # print(f"Requesting {count} revisions out of {total} (probability {probability}).")
    cursor.execute(
        """SELECT id, date, directory FROM revision WHERE id IN %s""",
        (tuple(revisions),),
    )
    ordered = [row for row in cursor.fetchall() if row[1] is not None]
    ordered.sort(key=lambda rev: rev[1])

    print(f"Obtained {len(ordered)} revisions.")
    with io.open(filename, "w") as outfile:
        for rev in ordered:
            outfile.write(f"{hash_to_hex(rev[0])},{rev[1]},{hash_to_hex(rev[2])}\n")
