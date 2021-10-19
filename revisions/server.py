#!/usr/bin/env python

import gzip
import io
import os
import queue
import sys
import threading
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List

import iso8601
import yaml
import zmq
from swh.core import config
from swh.provenance import get_provenance
from swh.provenance.postgresql.provenance import ProvenanceStoragePostgreSql
from swh.provenance.provenance import ProvenanceInterface

# All generic config code should reside in swh.core.config
CONFIG_ENVVAR = "SWH_CONFIG_FILENAME"
DEFAULT_PATH = os.environ.get(CONFIG_ENVVAR, None)

UTCEPOCH = datetime.fromtimestamp(0, timezone.utc)


class StatsCommand(Enum):
    GET = "get"
    EXIT = "exit"


class StatsWorker(threading.Thread):
    def __init__(self, filename: str, storage_conf: Dict[str, Any]) -> None:
        super().__init__()
        self.filename = filename
        self.queue = queue.Queue()
        self.storage_conf = storage_conf

    def run(self) -> None:
        tables = init_stats(self.filename)
        with get_provenance(**self.storage_conf) as provenance:
            while True:
                try:
                    cmd, idx = self.queue.get(timeout=1)
                    if cmd == StatsCommand.EXIT:
                        break
                    elif cmd == StatsCommand.GET:
                        write_stats(
                            self.filename, idx, get_tables_stats(provenance, tables)
                        )
                except queue.Empty:
                    continue


def get_tables_stats(
    provenance: ProvenanceInterface, tables: List[str]
) -> Dict[str, int]:
    # TODO: use ProvenanceStorageInterface instead!
    assert isinstance(provenance.storage, ProvenanceStoragePostgreSql)
    stats = {}
    for table in tables:
        with provenance.storage.transaction(readonly=True) as cursor:
            cursor.execute(f"SELECT COUNT(*) AS count FROM {table}")
            stats[table] = cursor.fetchone()["count"]
    return stats


def init_stats(filename: str) -> List[str]:
    tables = [
        "content",
        "content_in_revision",
        "content_in_directory",
        "directory",
        "directory_in_revision",
        "location",
        "revision",
    ]
    header = ["revisions count", "datetime"]
    for table in tables:
        header.append(f"{table} rows")
    with io.open(filename, "w") as outfile:
        outfile.write(",".join(header))
        outfile.write("\n")
    return tables


def write_stats(filename: str, count: int, stats: Dict[str, int]) -> None:
    line = [str(count), str(datetime.now())]
    for table, count in stats.items():
        line.append(str(count))
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

    stats = conf["rev_server"].get("stats", False)
    if stats:
        storage_conf = (
            conf["storage"]
            if conf["storage"]["cls"] == "postgresql"
            else conf["storage"]["storage_config"]
        )
        dbname = storage_conf["db"].get("dbname", storage_conf["db"].get("service"))
        statsfile = f"stats_{dbname}_{datetime.now()}.csv"
        worker = StatsWorker(statsfile, storage_conf)
        worker.start()

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
    for idx, (rev, date, root) in enumerate(revisions_provider):
        if iso8601.parse_date(date) <= UTCEPOCH:
            continue

        if limit is not None and limit <= idx:
            break

        if stats and idx > skip and idx % stats == 0:
            worker.queue.put((StatsCommand.GET, idx))

        # Wait for next request from client
        request = socket.recv()
        response = {
            "rev": rev,
            "date": date,
            "root": root,
        }
        socket.send_json(response)

    if stats:
        worker.queue.put((StatsCommand.GET, 0))
        worker.queue.put((StatsCommand.EXIT, None))
        worker.join()

    while True:
        # Force all clients to exit
        request = socket.recv()
        socket.send_json(None)
