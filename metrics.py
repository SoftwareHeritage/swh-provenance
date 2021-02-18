#!/usr/bin/env python

import io
import sys

from swh.provenance import get_provenance


# TODO: take conninfo as command line arguments.
conninfo = {
    "cls": "ps",
    "db": {"host": "/var/run/postgresql", "port": "5436", "dbname": "ordered"},
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

    print("ratios:")
    print(f" content/revision:              {row_count['content'] / row_count['revision']:.2f}")
    print(f" content_early_in_rev/content:  {row_count['content_early_in_rev'] / row_count['content']:.2f}")
    print(f" directory/revision:            {row_count['directory'] / row_count['revision']:.2f}")
    print(f" content_in_dir/directory:      {row_count['content_in_dir'] / row_count['directory']:.2f}")
    print(f" directory_in_rev/revision:     {row_count['directory_in_rev'] / row_count['revision']:.2f}")
