#!/usr/bin/env python

import io
import itertools
import json
import logging
import subprocess
import sys
import zmq

from swh.model.hashutil import hash_to_hex
from swh.provenance import get_archive, get_provenance
from swh.provenance.provenance import ProvenanceInterface
from swh.provenance.revision import CSVRevisionIterator


# TODO: take this from a configuration file
conninfo = {
    "archive": {
        "cls": "direct",
        "db": {
            "host": "somerset.internal.softwareheritage.org",
            "port": "5433",
            "dbname": "softwareheritage",
            "user": "guest"
        }
    },
    "provenance": {
        "cls": "local",
        "db": {
            "host": "/var/run/postgresql",
            "port": "5436",
            "dbname": "provenance"
        }
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
      "revision": dict()
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
        tables[table]["relation_size"] = tables[table]["table_size"] + tables[table]["indexes_size"]

    return tables


def init_stats(filename):
    tables = [
      "content",
      "content_early_in_rev",
      "content_in_dir",
      "directory",
      "directory_in_rev",
      "location",
      "revision"
    ]

    header = ["revisions count"]
    for table in tables:
        header.append(f"{table} rows")
        header.append(f"{table} table size")
        header.append(f"{table} index size")
        header.append(f"{table} relation size")

    with io.open(filename, "w") as outfile:
        outfile.write(','.join(header))
        outfile.write('\n')


def write_stats(filename, count, tables):
    line = [str(count)]

    for table, stats in tables.items():
        line.append(str(stats["row_count"]))
        line.append(str(stats["table_size"]))
        line.append(str(stats["indexes_size"]))
        line.append(str(stats["relation_size"]))

    with io.open(filename, "a") as outfile:
        outfile.write(','.join(line))
        outfile.write('\n')


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: server <filename> <port> [limit]")
        print("where")
        print("    filename     : csv file containing the list of revisions to be iterated (one per")
        print("                   line): revision sha1, date in ISO format, root directory sha1.")
        print("    port         : server listening port.")
        print("    limit        : max number of revisions to be retrieved from the file.")
        print("    stats        : number of iteration after which stats should be taken.")
        exit(-1)

    filename = sys.argv[1]
    port = int(sys.argv[2])
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
    stats = int(sys.argv[4]) if len(sys.argv) > 3 else None
    
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{port}")

    archive = get_archive(**conninfo["archive"])
    provenance = get_provenance(**conninfo["provenance"])

    statsfile = f"stats_{conninfo['provenance']['db']['dbname']}.csv"
    if stats is not None:
        init_stats(statsfile)

    revisions_provider = (
        line.strip().split(",") for line in open(filename, "r") if line.strip()
    )

    for idx, revision in enumerate(CSVRevisionIterator(revisions_provider, archive, limit=limit)):
        if stats is not None and idx != 0 and idx % stats == 0:
            write_stats(statsfile, idx, get_tables_stats(provenance))

        # Wait for next request from client
        message = socket.recv()
        message = {
            "rev" : hash_to_hex(revision.id),
            "date" : str(revision.date),
            "root" : hash_to_hex(revision.root)
        }
        socket.send_json(message)

    while True:
        # Force all clients to exit
        message = socket.recv()
        socket.send_json(None)
