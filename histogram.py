#!/usr/bin/env python

import io

from swh.provenance import get_provenance
from swh.provenance.postgresql.provenance import ProvenanceStoragePostgreSql

# TODO: take conninfo as command line arguments.
conninfo = {
    "cls": "local",
    "db": {"host": "/var/run/postgresql", "port": "5436", "dbname": "provenance"},
}


if __name__ == "__main__":
    # Get provenance object.
    provenance = get_provenance(**conninfo)
    # TODO: use ProvenanceStorageInterface instead!
    assert isinstance(provenance.storage, ProvenanceStoragePostgreSql)

    tables = ["directory_in_rev", "content_in_dir"]

    for table in tables:
        with provenance.storage.transaction() as cursor:
            cursor.execute(
                f"""SELECT depths.depth, COUNT(depths.depth)
                    FROM (SELECT 
                            CASE location.path
                                WHEN '' THEN 0
                                WHEN '.' THEN 0
                                ELSE 1 + CHAR_LENGTH(ENCODE(location.path, 'escape')) - 
                                        CHAR_LENGTH(REPLACE(ENCODE(location.path, 'escape'), '/', ''))
                            END AS depth
                            FROM {table}
                            JOIN location
                            ON {table}.loc=location.id
                        ) AS depths
                    GROUP BY depths.depth
                    ORDER BY depths.depth"""
            )

            filename = "depths_" + conninfo["db"]["dbname"] + f"_{table}.csv"

            with io.open(filename, "w") as outfile:
                outfile.write(f"{table} depth,{table} count\n")
                for depth, count in cursor.fetchall():
                    outfile.write(f"{depth},{count}\n")
