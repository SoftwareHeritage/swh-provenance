#!/usr/bin/env python

from swh.provenance import get_provenance
from swh.provenance.postgresql.provenance import ProvenanceStoragePostgreSql
from swh.provenance.provenance import ProvenanceInterface

# TODO: take conninfo as command line arguments.
conninfo = {
    "cls": "local",
    "db": {"host": "/var/run/postgresql", "port": "5436", "dbname": "provenance"},
}


def get_tables_stats(provenance: ProvenanceInterface):
    # TODO: use ProvenanceStorageInterface instead!
    assert isinstance(provenance.storage, ProvenanceStoragePostgreSql)

    tables = {
        "content": dict(),
        "content_early_in_rev": dict(),
        "content_in_dir": dict(),
        "directory": dict(),
        "directory_in_rev": dict(),
        "location": dict(),
        "revision": dict(),
    }

    for table in tables:
        with provenance.storage.transaction() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            tables[table]["row_count"] = cursor.fetchone()[0]

            cursor.execute(f"SELECT pg_table_size('{table}')")
            tables[table]["table_size"] = cursor.fetchone()[0]

            cursor.execute(f"SELECT pg_indexes_size('{table}')")
            tables[table]["indexes_size"] = cursor.fetchone()[0]

            # cursor.execute(f"SELECT pg_total_relation_size('{table}')")
            # relation_size[table] = cursor.fetchone()[0]
            tables[table]["relation_size"] = (
                tables[table]["table_size"] + tables[table]["indexes_size"]
            )

    return tables


if __name__ == "__main__":
    # Get provenance object.
    with get_provenance(**conninfo) as provenance:
        # TODO: use ProvenanceStorageInterface instead!
        assert isinstance(provenance.storage, ProvenanceStoragePostgreSql)

        tables = get_tables_stats(provenance)

        for table in tables:
            row_count = tables[table]["row_count"]
            table_size = tables[table]["table_size"]
            indexes_size = tables[table]["indexes_size"]
            relation_size = tables[table]["relation_size"]

            print(f"{table}:")
            print(f"    total rows: {row_count}")
            if row_count == 0:
                row_count = 1
            print(
                f"    table size: {table_size} bytes ({table_size / row_count:.2f} per row)"
            )
            print(
                f"    index size: {indexes_size} bytes ({indexes_size / row_count:.2f} per row)"
            )
            print(
                f"    total size: {relation_size} bytes ({relation_size / row_count:.2f} per row)"
            )

        # Ratios between de different entities/relations.
        print("ratios:")
        print(
            f"    content/revision:              {tables['content']['row_count'] / (tables['revision']['row_count'] if tables['revision']['row_count'] != 0 else 1):.2f}"
        )
        print(
            f"    content_early_in_rev/content:  {tables['content_early_in_rev']['row_count'] / (tables['content']['row_count'] if tables['content']['row_count'] != 0 else 1):.2f}"
        )
        print(
            f"    content_in_dir/content:        {tables['content_in_dir']['row_count'] / (tables['content']['row_count'] if tables['content']['row_count'] != 0 else 1):.2f}"
        )
        print(
            f"    directory/revision:            {tables['directory']['row_count'] / (tables['revision']['row_count'] if tables['revision']['row_count'] != 0 else 1):.2f}"
        )
        print(
            f"    directory_in_rev/directory:    {tables['directory_in_rev']['row_count'] / (tables['directory']['row_count'] if tables['directory']['row_count'] != 0 else 1):.2f}"
        )
        print(f"    ==============================")
        print(
            f"    content_early_in_rev/revision: {tables['content_early_in_rev']['row_count'] / (tables['revision']['row_count'] if tables['revision']['row_count'] != 0 else 1):.2f}"
        )
        print(
            f"    content_in_dir/directory:      {tables['content_in_dir']['row_count'] / (tables['directory']['row_count'] if tables['directory']['row_count'] != 0 else 1):.2f}"
        )
        print(
            f"    directory_in_rev/revision:     {tables['directory_in_rev']['row_count'] / (tables['revision']['row_count'] if tables['revision']['row_count'] != 0 else 1):.2f}"
        )

        # Metrics for frontiers defined in root directories.
        with provenance.storage.transaction() as cursor:
            cursor.execute(
                f"""SELECT dir
                    FROM directory_in_rev
                    INNER JOIN location
                        ON loc=location.id
                    WHERE location.path=%s""",
                (b"",),
            )
            directories = list(cursor.fetchall())
            print(f"Total root frontiers used:              {len(directories)}")

            cursor.execute(
                f"""SELECT dir
                    FROM directory_in_rev
                    INNER JOIN location
                        ON loc=location.id
                    WHERE location.path=%s
                    GROUP BY dir""",
                (b"",),
            )
            directories = list(cursor.fetchall())
            print(f"Total distinct root frontiers:          {len(directories)}")

            cursor.execute(
                f"""SELECT roots.dir
                    FROM (SELECT dir, loc
                            FROM directory_in_rev
                            INNER JOIN location
                                ON loc=location.id
                            WHERE location.path=%s) AS roots
                    JOIN directory_in_rev
                        ON directory_in_rev.dir=roots.dir
                    WHERE directory_in_rev.loc!=roots.loc""",
                (b"",),
            )
            directories = list(cursor.fetchall())
            print(f"Total other uses of these frontiers:    {len(directories)}")

            cursor.execute(
                f"""SELECT roots.dir
                    FROM (SELECT dir, loc
                            FROM directory_in_rev
                            INNER JOIN location
                                ON loc=location.id
                            WHERE location.path=%s) AS roots
                    JOIN directory_in_rev
                        ON directory_in_rev.dir=roots.dir
                    WHERE directory_in_rev.loc!=roots.loc
                    GROUP BY roots.dir""",
                (b"",),
            )
            directories = list(cursor.fetchall())
            print(f"Total distinct other uses of frontiers: {len(directories)}")
