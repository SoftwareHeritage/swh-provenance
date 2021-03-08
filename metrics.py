#!/usr/bin/env python

import io
import sys

from swh.model.hashutil import hash_to_hex
from swh.provenance import get_provenance


# TODO: take conninfo as command line arguments.
conninfo = {
    "cls": "ps",
    "db": {"host": "/var/run/postgresql", "port": "5436", "dbname": "upper2m8proc"},
}


if __name__ == "__main__":
    # Get provenance object for both databases and query its lists of content.
    provenance = get_provenance(**conninfo)

    tables = [
        "content",
        "content_early_in_rev",
        "content_in_dir",
        "directory",
        "directory_in_rev",
        "location",
        "revision"
    ]

    row_count = {}
    table_size = {}
    indexes_size = {}
    relation_size = {}
    for table in tables:
        provenance.cursor.execute(f"SELECT COUNT(*) FROM {table}")
        row_count[table] = provenance.cursor.fetchone()[0]

        provenance.cursor.execute(f"SELECT pg_table_size('{table}')")
        table_size[table] = provenance.cursor.fetchone()[0]

        provenance.cursor.execute(f"SELECT pg_indexes_size('{table}')")
        indexes_size[table] = provenance.cursor.fetchone()[0]

        # provenance.cursor.execute(f"SELECT pg_total_relation_size('{table}')")
        # relation_size[table] = provenance.cursor.fetchone()[0]
        relation_size[table] = table_size[table] + indexes_size[table]

        print(f"{table}:")
        print(f"    total rows: {row_count[table]}")
        print(f"    table size: {table_size[table]} bytes ({table_size[table] / row_count[table]:.2f} per row)")
        print(f"    index size: {indexes_size[table]} bytes ({indexes_size[table] / row_count[table]:.2f} per row)")
        print(f"    total size: {relation_size[table]} bytes ({relation_size[table] / row_count[table]:.2f} per row)")

    # Ratios between de different entities/relations.
    print("ratios:")
    print(f"    content/revision:              {row_count['content'] / row_count['revision']:.2f}")
    print(f"    content_early_in_rev/content:  {row_count['content_early_in_rev'] / row_count['content']:.2f}")
    print(f"    content_in_dir/content:        {row_count['content_in_dir'] / row_count['content']:.2f}")
    print(f"    directory/revision:            {row_count['directory'] / row_count['revision']:.2f}")
    print(f"    directory_in_rev/directory:    {row_count['directory_in_rev'] / row_count['directory']:.2f}")
    print(f"    ==============================")
    print(f"    content_early_in_rev/revision: {row_count['content_early_in_rev'] / row_count['revision']:.2f}")
    print(f"    content_in_dir/directory:      {row_count['content_in_dir'] / row_count['directory']:.2f}")
    print(f"    directory_in_rev/revision:     {row_count['directory_in_rev'] / row_count['revision']:.2f}")

    # Metrics for frontiers defined in root directories.
    provenance.cursor.execute(f"""SELECT dir
                                    FROM directory_in_rev
                                    INNER JOIN location
                                      ON loc=location.id
                                    WHERE location.path=%s""", (b"",))
    directories = list(provenance.cursor.fetchall())
    print(f"Total root frontiers used:              {len(directories)}")

    provenance.cursor.execute(f"""SELECT dir
                                    FROM directory_in_rev
                                    INNER JOIN location
                                      ON loc=location.id
                                    WHERE location.path=%s
                                    GROUP BY dir""", (b"",))
    directories = list(provenance.cursor.fetchall())
    print(f"Total distinct root frontiers:          {len(directories)}")

    provenance.cursor.execute(f"""SELECT roots.dir
                                    FROM (SELECT dir, loc
                                            FROM directory_in_rev
                                            INNER JOIN location
                                              ON loc=location.id
                                            WHERE location.path=%s) AS roots
                                    JOIN directory_in_rev
                                      ON directory_in_rev.dir=roots.dir
                                    WHERE directory_in_rev.loc!=roots.loc""", (b"",))
    directories = list(provenance.cursor.fetchall())
    print(f"Total other uses of these frontiers:    {len(directories)}")

    provenance.cursor.execute(f"""SELECT roots.dir
                                    FROM (SELECT dir, loc
                                            FROM directory_in_rev
                                            INNER JOIN location
                                              ON loc=location.id
                                            WHERE location.path=%s) AS roots
                                    JOIN directory_in_rev
                                      ON directory_in_rev.dir=roots.dir
                                    WHERE directory_in_rev.loc!=roots.loc
                                    GROUP BY roots.dir""", (b"",))
    directories = list(provenance.cursor.fetchall())
    print(f"Total distinct other uses of frontiers: {len(directories)}")


# Query the 'limit' most common files inside any isochrone frontier.
# f"SELECT blob, COUNT(blob) AS occur FROM content_early_in_rev GROUP BY blob ORDER BY occur DESC LIMIT {limit}"

# Query the 'limit' most common files outside any isochrone frontier.
# f"SELECT blob, COUNT(blob) AS occur FROM content_in_dir GROUP BY blob ORDER BY occur DESC LIMIT {limit}"
# blob 141557 | occur 34610802

# f"SELECT dir FROM directory_in_rev INNER JOIN location ON loc=location.id WHERE location.path=%s"

# f"SELECT blob, COUNT(blob) AS occur FROM content_in_dir GROUP BY blob ORDER BY occur DESC LIMIT {limit}"

# f"SELECT path FROM location JOIN content_in_dir ON location.id=content_in_dir.loc WHERE blob=%s GROUP BY path"
# f"SELECT ENCODE(location.path::bytea, 'escape'), COUNT(*) FROM content_in_dir INNER JOIN location ON loc=location.id WHERE blob=%s GROUP BY 1 ORDER BY 2 DESC"
# f"SELECT ENCODE(sha1::bytea, 'escape') FROM content WHERE id=%s"
