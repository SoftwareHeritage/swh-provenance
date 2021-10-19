#!/usr/bin/env python

import gzip
import io
import json
import os
import sys
from datetime import datetime, timezone

import iso8601
import yaml
import zmq
from swh.core import config
from swh.provenance import get_provenance
from swh.provenance.provenance import ProvenanceInterface

# All generic config code should reside in swh.core.config
CONFIG_ENVVAR = "SWH_CONFIG_FILENAME"
DEFAULT_PATH = os.environ.get(CONFIG_ENVVAR, None)

UTCEPOCH = datetime.fromtimestamp(0, timezone.utc)


def get_tables_stats(provenance: ProvenanceInterface):
    tables = {
        "content": dict(),
        "content_in_revision": dict(),
        "content_in_directory": dict(),
        "directory": dict(),
        "directory_in_revision": dict(),
        "location": dict(),
        "revision": dict(),
    }

    for table in tables:
        # TODO: use ProvenanceStorageInterface instead!
        with provenance.storage.transaction(readonly=True) as cursor:
            cursor.execute(f"SELECT COUNT(*) AS count FROM {table}")
            tables[table]["row_count"] = cursor.fetchone()["count"]

            # cursor.execute(f"SELECT pg_table_size('{table}') AS size")
            # tables[table]["table_size"] = cursor.fetchone()["size"]

            # cursor.execute(f"SELECT pg_indexes_size('{table}') AS size")
            # tables[table]["indexes_size"] = cursor.fetchone()["size"]

            # # cursor.execute(f"SELECT pg_total_relation_size('{table}') AS size")
            # # relation_size[table] = cursor.fetchone()["size"]
            # tables[table]["relation_size"] = (
            #     tables[table]["table_size"] + tables[table]["indexes_size"]
            # )

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

    header = ["revisions count", "datetime"]
    for table in tables:
        header.append(f"{table} rows")
        # header.append(f"{table} table size")
        # header.append(f"{table} index size")
        # header.append(f"{table} relation size")

    with io.open(filename, "w") as outfile:
        outfile.write(",".join(header))
        outfile.write("\n")


def write_stats(filename, count, tables):
    line = [str(count), str(datetime.now())]

    for table, stats in tables.items():
        line.append(str(stats["row_count"]))
        # line.append(str(stats["table_size"]))
        # line.append(str(stats["indexes_size"]))
        # line.append(str(stats["relation_size"]))

    with io.open(filename, "a") as outfile:
        outfile.write(",".join(line))
        outfile.write("\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: server <filename>")
        print("where")
        print(
            "    filename     : csv file containing the list of revisions to be iterated (one per"
        )
        print(
            "                   line): revision sha1, date in ISO format, root directory sha1."
        )
        exit(-1)

    filename = sys.argv[1]

    config_file = None  # TODO: Add as a cli option
    if (
        config_file is None
        and DEFAULT_PATH is not None
        and config.config_exists(DEFAULT_PATH)
    ):
        config_file = DEFAULT_PATH

    if config_file is None or not os.path.exists(config_file):
        print("No configuration provided")
        exit(-1)

    conf = yaml.safe_load(open(config_file, "rb"))["provenance"]

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{conf['rev_server']['port']}")

    storage_conf = (
        conf["storage"]
        if conf["storage"]["cls"] == "postgresql"
        else conf["storage"]["storage_config"]
    )
    dbname = storage_conf["db"].get("dbname", storage_conf["db"].get("service"))

    stats = conf["rev_server"].get("stats")
    statsfile = f"stats_{dbname}_{datetime.now()}.csv"
    if stats is not None:
        init_stats(statsfile)

    revisions_provider = (
        (line.strip().split(",") for line in io.open(filename, "r") if line.strip())
        if os.path.splitext(filename)[1] == ".csv"
        else (
            line.strip().split(",")
            for line in gzip.open(filename, "rt")
            if line.strip()
        )
    )

    limit = conf["rev_server"].get("limit")
    skip = conf["rev_server"].get("skip", 0)
    with get_provenance(**storage_conf) as provenance:
        for idx, (rev, date, root) in enumerate(revisions_provider):
            if iso8601.parse_date(date) <= UTCEPOCH:
                continue

            if limit is not None and limit <= idx:
                break

            if stats is not None and idx > skip and idx % stats == 0:
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
