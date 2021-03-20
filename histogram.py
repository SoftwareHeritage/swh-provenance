#!/usr/bin/env python

import io
import os

from collections import Counter

from swh.provenance import get_provenance


# TODO: take conninfo as command line arguments.
conninfo = {
    "cls": "local",
    "db": {"host": "/var/run/postgresql", "port": "5436", "dbname": "provenance"},
}


if __name__ == "__main__":
    # Get provenance object for both databases and query its lists of content.
    provenance = get_provenance(**conninfo)

    tables = ["directory_in_rev", "content_in_dir"]

    for table in tables:
        provenance.cursor.execute(f"""SELECT depths.depth, COUNT(depths.depth)
                                        FROM (SELECT (CHAR_LENGTH(ENCODE(location.path, 'escape')) - CHAR_LENGTH(REPLACE(ENCODE(location.path, 'escape'), '/', ''))) / CHAR_LENGTH('/') AS depth
                                                FROM {table}
                                                JOIN location
                                                  ON {table}.loc=location.id
                                             ) AS depths
                                        GROUP BY depths.depth
                                        ORDER BY depths.depth""")
        with io.open(conninfo["db"]["dbname"] + f"_{table}.csv", "w") as outfile:
            outfile.write(f"{table} depth,{table} count\n")
            for depth, count in provenance.cursor.fetchall():
                outfile.write(f"{depth},{count}\n")
