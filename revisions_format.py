#!/usr/bin/env python

import gzip
import sys
from typing import IO, Iterable

import psycopg2
from swh.core.db import BaseDb
from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.model.model import Sha1Git

conninfo = {
    "host": "db.internal.softwareheritage.org",
    "dbname": "softwareheritage",
    "user": "guest",
}


def write_output(
    cursor: psycopg2.cursor, ids: Iterable[Sha1Git], outfile: IO[bytes]
) -> None:
    cursor.execute(
        """SELECT id, date, directory
            FROM revision
            WHERE id IN %s
                AND date IS NOT NULL
            ORDER BY date""",
        (tuple(ids),),
    )
    for rev in cursor.fetchall():
        assert rev is not None, rev
        assert rev[1] is not None, rev
        outfile.write(f"{hash_to_hex(rev[0])},{rev[1]},{hash_to_hex(rev[2])}\n")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: revisions_format <infile> <outfile>")
        exit(-1)

    print(f"Connection to database: {conninfo}...")
    conn = BaseDb.connect(**conninfo).conn
    BaseDb.adapt_conn(conn)
    cursor = conn.cursor()

    infilename = sys.argv[1]
    outfilename = sys.argv[2]

    # with io.open(infilename) as infile:
    #     with io.open(outfilename, "w") as outfile:
    #         ids = json.loads(infile.read())
    #         print(f"Formatting {len(ids)} revisions")
    #         for id in ids:
    #             cursor.execute(
    #                 """SELECT id, date, directory
    #                      FROM revision
    #                      WHERE id=%s AND date IS NOT NULL""",
    #                 (hash_to_bytes(id),),
    #             )
    #             rev = cursor.fetchone()
    #             assert rev is not None
    #             outfile.write(f"{hash_to_hex(rev[0])},{rev[1]},{hash_to_hex(rev[2])}\n")

    with gzip.open(infilename, "rt") as infile:
        with gzip.open(outfilename, "wt") as outfile:
            ids = []
            for idx, line in enumerate(infile.readlines(), start=1):
                if line.strip():
                    ids.append(hash_to_bytes(line.split(",")[0]))
                    if idx % 100 == 0:
                        write_output(cursor, ids, outfile)
                        ids = []
            if ids:
                write_output(cursor, ids, outfile)
