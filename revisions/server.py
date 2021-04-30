#!/usr/bin/env python

import io
import json
import sys
import zmq

from swh.provenance import get_provenance
from swh.provenance.provenance import ProvenanceInterface


# TODO: take this from a configuration file
conninfo = {
    "provenance": {
        "cls": "local",
        "db": {"host": "/var/run/postgresql", "port": "5436", "dbname": "provenance"},
    },
}


def get_tables_stats(provenance: ProvenanceInterface):
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
        provenance.cursor.execute(f"SELECT COUNT(*) FROM {table}")
        tables[table]["row_count"] = provenance.cursor.fetchone()[0]

        provenance.cursor.execute(f"SELECT pg_table_size('{table}')")
        tables[table]["table_size"] = provenance.cursor.fetchone()[0]

        provenance.cursor.execute(f"SELECT pg_indexes_size('{table}')")
        tables[table]["indexes_size"] = provenance.cursor.fetchone()[0]

        # provenance.cursor.execute(f"SELECT pg_total_relation_size('{table}')")
        # relation_size[table] = provenance.cursor.fetchone()[0]
        tables[table]["relation_size"] = (
            tables[table]["table_size"] + tables[table]["indexes_size"]
        )

    return tables


def init_stats(filename):
    tables = [
        "content",
        "content_early_in_rev",
        "content_in_dir",
        "directory",
        "directory_in_rev",
        "location",
        "revision",
    ]

    header = ["revisions count"]
    for table in tables:
        header.append(f"{table} rows")
        header.append(f"{table} table size")
        header.append(f"{table} index size")
        header.append(f"{table} relation size")

    with io.open(filename, "w") as outfile:
        outfile.write(",".join(header))
        outfile.write("\n")


def write_stats(filename, count, tables):
    line = [str(count)]

    for table, stats in tables.items():
        line.append(str(stats["row_count"]))
        line.append(str(stats["table_size"]))
        line.append(str(stats["indexes_size"]))
        line.append(str(stats["relation_size"]))

    with io.open(filename, "a") as outfile:
        outfile.write(",".join(line))
        outfile.write("\n")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: server <filename> <port> [stats] [limit]")
        print("where")
        print(
            "    filename     : csv file containing the list of revisions to be iterated (one per"
        )
        print(
            "                   line): revision sha1, date in ISO format, root directory sha1."
        )
        print("    port         : server listening port.")
        print(
            "    stats        : number of iteration after which stats should be taken."
        )
        print(
            "    limit        : max number of revisions to be retrieved from the file."
        )
        exit(-1)

    filename = sys.argv[1]
    port = int(sys.argv[2])
    stats = int(sys.argv[3]) if len(sys.argv) > 3 else None
    limit = int(sys.argv[4]) if len(sys.argv) > 4 else None

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{port}")

    provenance = get_provenance(**conninfo["provenance"])

    statsfile = f"stats_{conninfo['provenance']['db']['dbname']}.csv"
    if stats is not None:
        init_stats(statsfile)

    revisions_provider = (
        line.strip().split(",") for line in open(filename, "r") if line.strip()
    )

    for idx, (rev, date, root) in enumerate(revisions_provider):
        if limit is not None and idx > limit:
            break

        if stats is not None and idx > 0 and idx % stats == 0:
            write_stats(statsfile, idx, get_tables_stats(provenance))

        # Wait for next request from client
        request = socket.recv()
        response = {
            "rev": rev,
            "date": date,
            "root": root,
        }
        socket.send_json(response)

    if stats is not None:
        write_stats(statsfile, 0, get_tables_stats(provenance))

    while True:
        # Force all clients to exit
        request = socket.recv()
        socket.send_json(None)
