#!/usr/bin/env python

import gzip
import io
import os
import queue
import sys
import threading
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional

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


# TODO: move this functions to StatsWorker class
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
    header = ["datetime"]
    for table in tables:
        header.append(f"{table} rows")
    with io.open(filename, "w") as outfile:
        outfile.write(",".join(header))
        outfile.write("\n")
    return tables


def write_stats(filename: str, stats: Dict[str, int]) -> None:
    line = [str(datetime.now())]
    for _, count in stats.items():
        line.append(str(count))
    with io.open(filename, "a") as outfile:
        outfile.write(",".join(line))
        outfile.write("\n")


class Command(Enum):
    TERMINATE = "terminate"


class StatsWorker(threading.Thread):
    def __init__(
        self,
        filename: str,
        storage_conf: Dict[str, Any],
        timeout: float = 300,
        group: None = None,
        target: Optional[Callable[..., Any]] = ...,
        name: Optional[str] = ...,
        args: Iterable[Any] = ...,
        kwargs: Optional[Mapping[str, Any]] = ...,
        *,
        daemon: Optional[bool] = ...,
    ) -> None:
        super().__init__(
            group=group,
            target=target,
            name=name,
            args=args,
            kwargs=kwargs,
            daemon=daemon,
        )
        self.filename = filename
        self.queue = queue.Queue()
        self.storage_conf = storage_conf
        self.timeout = timeout

    def run(self) -> None:
        tables = init_stats(self.filename)
        start = time.monotonic()
        with get_provenance(**self.storage_conf) as provenance:
            while True:
                now = time.monotonic()
                if now - start > self.timeout:
                    write_stats(self.filename, get_tables_stats(provenance, tables))
                    start = now
                try:
                    cmd = self.queue.get(timeout=1)
                    if cmd == Command.TERMINATE:
                        break
                except queue.Empty:
                    continue

    def stop(self) -> None:
        self.queue.put(Command.TERMINATE)
        self.join()


class RevisionWorker(threading.Thread):
    def __init__(
        self,
        filename: str,
        url: str,
        limit: Optional[int] = None,
        size: int = 1,
        skip: int = 0,
        group: None = None,
        target: Optional[Callable[..., Any]] = ...,
        name: Optional[str] = ...,
        args: Iterable[Any] = ...,
        kwargs: Optional[Mapping[str, Any]] = ...,
        *,
        daemon: Optional[bool] = ...,
    ) -> None:
        super().__init__(
            group=group,
            target=target,
            name=name,
            args=args,
            kwargs=kwargs,
            daemon=daemon,
        )
        self.filename = filename
        self.limit = limit
        self.queue = queue.Queue()
        self.size = size
        self.skip = skip
        self.url = url

    def run(self) -> None:
        context = zmq.Context()
        socket: zmq.Socket = context.socket(zmq.REP)
        socket.bind(self.url)

        # TODO: improve this using a context manager
        file = (
            io.open(self.filename, "r")
            if os.path.splitext(self.filename)[1] == ".csv"
            else gzip.open(self.filename, "rt")
        )
        provider = (line.strip().split(",") for line in file if line.strip())

        count = 0
        while True:
            if self.limit is not None and count > self.limit:
                break

            response = []
            for rev, date, root in provider:
                count += 1
                if count <= self.skip or iso8601.parse_date(date) <= UTCEPOCH:
                    continue
                response.append({"rev": rev, "date": date, "root": root})
                if len(response) == self.size:
                    break
            if not response:
                break

            # Wait for next request from client
            # (TODO: make it non-blocking or add timeout)
            socket.recv()
            socket.send_json(response)

            try:
                cmd = self.queue.get(block=False)
                if cmd == Command.TERMINATE:
                    break
            except queue.Empty:
                continue

        while True:  # TODO: improve shutdown logic
            socket.recv()
            socket.send_json(None)
        # context.term()

    def stop(self) -> None:
        self.queue.put(Command.TERMINATE)
        self.join()


if __name__ == "__main__":
    # TODO: improve command line parsing
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

    # Init stats
    statsfile = conf["rev_server"].get("stats_file")
    if statsfile is not None:
        storage_conf = (
            conf["storage"]["storage_config"]
            if conf["storage"]["cls"] == "rabbitmq"
            else conf["storage"]
        )
        statsfile = f"stats_{datetime.now()}_{statsfile}"
        statsworker = StatsWorker(
            statsfile, storage_conf, timeout=conf["rev_server"].get("stats_rate", 300)
        )
        statsworker.start()

    # Init revision provider
    revsfile = sys.argv[1]
    url = f"tcp://*:{conf['rev_server']['port']}"
    revsworker = RevisionWorker(
        revsfile,
        url,
        limit=conf["rev_server"].get("limit"),
        size=conf["rev_server"].get("size", 1),
        skip=conf["rev_server"].get("skip", 0),
    )
    revsworker.start()

    # Wait for user commands
    while True:
        try:
            command = input("Enter EXIT to stop service: ")
            if command.lower() == "exit":
                break
        except KeyboardInterrupt:
            pass

    # Release resources
    revsworker.stop()
    if statsfile:
        statsworker.stop()
