#!/usr/bin/env python

import iso8601
import logging
import sys
import time
import zmq

from multiprocessing import Process
from threading import Thread


from swh.model.hashutil import hash_to_bytes
from swh.provenance import get_archive, get_provenance
from swh.provenance.archive import ArchiveInterface
from swh.provenance.provenance import revision_add
from swh.provenance.revision import RevisionEntry
from typing import Any, Dict


# TODO: take this from a configuration file
conninfo = {
    "archive": {
        "cls": "direct",
        "db": {
            "host": "somerset.internal.softwareheritage.org",
            "port": "5433",
            "dbname": "softwareheritage",
            "user": "guest",
        },
    },
    "provenance": {
        "cls": "local",
        "db": {"host": "/var/run/postgresql", "port": "5436", "dbname": "provenance"},
    },
}


class Client(Process):
    def __init__(
        self,
        idx: int,
        threads: int,
        conninfo: Dict[str, Any],
        lower: bool,
        mindepth: int,
    ):
        super().__init__()
        self.idx = idx
        self.threads = threads
        self.conninfo = conninfo
        self.lower = lower
        self.mindepth = mindepth

    def run(self):
        # Using the same archive object for every worker to share internal caches.
        archive = get_archive(**self.conninfo["archive"])

        # Launch as many threads as requested
        workers = []
        for idx in range(self.threads):
            logging.info(f"Process {self.idx}: launching thread {idx}")
            worker = Worker(idx, archive, self.conninfo, self.lower, self.mindepth)
            worker.start()
            workers.append(worker)

        # Wait for all threads to complete their work
        for idx, worker in enumerate(workers):
            logging.info(f"Process {self.idx}: waiting for thread {idx} to finish")
            worker.join()
            logging.info(f"Process {self.idx}: thread {idx} finished executing")


class Worker(Thread):
    def __init__(
        self,
        idx: int,
        archive: ArchiveInterface,
        conninfo: Dict[str, Any],
        lower: bool,
        mindepth: int,
    ):
        super().__init__()
        self.idx = idx
        self.archive = archive
        self.server = conninfo["server"]
        # Each worker has its own provenance object to isolate
        # the processing of each revision.
        self.provenance = get_provenance(**conninfo["provenance"])
        self.lower = lower
        self.mindepth = mindepth

    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(self.server)
        while True:
            socket.send(b"NEXT")
            response = socket.recv_json()

            if response is None:
                break

            # Ensure date has a valid timezone
            date = iso8601.parse_date(response["date"])
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)

            revision = RevisionEntry(
                self.archive,
                hash_to_bytes(response["rev"]),
                date=date,
                root=hash_to_bytes(response["root"]),
            )
            revision_add(
                self.provenance,
                self.archive,
                revision,
                lower=self.lower,
                mindepth=self.mindepth,
            )


if __name__ == "__main__":
    # Check parameters
    if len(sys.argv) != 5:
        print("usage: client <processes> <port> <lower> <mindepth>")
        exit(-1)

    processes = int(sys.argv[1])
    port = int(sys.argv[2])
    threads = 1  # int(sys.argv[2])
    lower = bool(sys.argv[3])
    mindepth = int(sys.argv[4])
    dbname = conninfo["provenance"]["db"]["dbname"]
    conninfo["server"] = f"tcp://localhost:{port}"

    # Set logging level
    # logging.getLogger().setLevel(logging.INFO)

    # Start counter
    start = time.time()

    # Launch as many clients as requested
    clients = []
    for idx in range(processes):
        logging.info(f"MAIN: launching process {idx}")
        client = Client(idx, threads, conninfo, lower, mindepth)
        client.start()
        clients.append(client)

    # Wait for all processes to complete their work
    for idx, client in enumerate(clients):
        logging.info(f"MAIN: waiting for process {idx} to finish")
        client.join()
        logging.info(f"MAIN: process {idx} finished executing")

    # Stop counter and report elapsed time
    stop = time.time()
    print("Elapsed time:", stop - start, "seconds")
