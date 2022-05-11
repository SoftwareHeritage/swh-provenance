#!/usr/bin/env python

from datetime import datetime, timezone
from enum import Enum
import gzip
import io
import os
import queue
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import yaml
import zmq

from swh.core import config
from swh.provenance import get_provenance
from swh.provenance.postgresql.provenance import ProvenanceStoragePostgreSql

CONFIG_ENVVAR = "SWH_CONFIG_FILENAME"

DEFAULT_BATCH_SIZE = 1
DEFAULT_PATH = os.environ.get(CONFIG_ENVVAR, None)
DEFAULT_PORT = 5555
DEFAULT_STATS_RATE = 300
DEFAULT_SKIP_VALUE = 0

UTCEPOCH = datetime.fromtimestamp(0, timezone.utc)


class Command(Enum):
    TERMINATE = "terminate"


class StatsWorker(threading.Thread):
    def __init__(
        self,
        filename: str,
        storage_conf: Dict[str, Any],
        timeout: float = DEFAULT_STATS_RATE,
        group: None = None,
        target: Optional[Callable[..., Any]] = ...,
        name: Optional[str] = ...,
    ) -> None:
        super().__init__(group=group, target=target, name=name)
        self.filename = filename
        self.queue = queue.Queue()
        self.storage_conf = storage_conf
        self.timeout = timeout

    def get_tables_stats(self, tables: List[str]) -> Dict[str, int]:
        # TODO: use ProvenanceStorageInterface instead!
        with get_provenance(**self.storage_conf) as provenance:
            assert isinstance(provenance.storage, ProvenanceStoragePostgreSql)
            stats = {}
            for table in tables:
                with provenance.storage.transaction(readonly=True) as cursor:
                    cursor.execute(f"SELECT COUNT(*) AS count FROM {table}")
                    stats[table] = cursor.fetchone()["count"]
            return stats

    def init_stats(self, filename: str) -> List[str]:
        tables = [
            "origin",
            "revision",
            "revision_in_origin",
            "revision_before_revision",
        ]
        header = ["datetime"]
        for table in tables:
            header.append(f"{table} rows")
        with io.open(filename, "w") as outfile:
            outfile.write(",".join(header))
            outfile.write("\n")
        return tables

    def run(self) -> None:
        tables = self.init_stats(self.filename)
        start = time.monotonic()
        while True:
            now = time.monotonic()
            if now - start > self.timeout:
                self.write_stats(self.filename, self.get_tables_stats(tables))
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

    def write_stats(self, filename: str, stats: Dict[str, int]) -> None:
        line = [str(datetime.now())]
        for _, stat in stats.items():
            line.append(str(stat))
        with io.open(filename, "a") as outfile:
            outfile.write(",".join(line))
            outfile.write("\n")


class OriginWorker(threading.Thread):
    def __init__(
        self,
        filename: str,
        url: str,
        batch_size: int = DEFAULT_BATCH_SIZE,
        limit: Optional[int] = None,
        skip: int = DEFAULT_SKIP_VALUE,
        group: None = None,
        target: Optional[Callable[..., Any]] = ...,
        name: Optional[str] = ...,
    ) -> None:
        super().__init__(group=group, target=target, name=name)
        self.filename = filename
        self.batch_size = batch_size
        self.limit = limit
        self.queue = queue.Queue()
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
        provider = (
            line.strip().rsplit(",", maxsplit=1) for line in file if line.strip()
        )

        count = 0
        while True:
            if self.limit is not None and count > self.limit:
                break

            response = []
            for url, snapshot in provider:
                count += 1
                if count <= self.skip:
                    continue
                response.append({"url": url, "snapshot": snapshot})
                if len(response) == self.batch_size:
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
        print(
            "filename: csv file containing the list of origins to be iterated (one per "
            "line): origin url, snapshot sha1."
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
    stats = conf["org_server"].pop("stats", None)
    if stats is not None:
        storage_conf = (
            conf["storage"]["storage_config"]
            if conf["storage"]["cls"] == "rabbitmq"
            else conf["storage"]
        )
        statsfile = f"stats_{datetime.now()}_{stats.pop('suffix')}"
        statsworker = StatsWorker(statsfile, storage_conf, **stats)
        statsworker.start()

    # Init origin provider
    orgsfile = sys.argv[1]
    host = conf["org_server"].pop("host", None)
    url = f"tcp://*:{conf['org_server'].pop('port', DEFAULT_PORT)}"
    orgsworker = OriginWorker(orgsfile, url, **conf["org_server"])
    orgsworker.start()

    # Wait for user commands
    while True:
        try:
            command = input("Enter EXIT to stop service: ")
            if command.lower() == "exit":
                break
        except KeyboardInterrupt:
            pass

    # Release resources
    orgsworker.stop()
    if stats is not None:
        statsworker.stop()
