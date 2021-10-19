#!/usr/bin/env python

import logging
import logging.handlers
import sys
import time
from datetime import timezone
from multiprocessing import Process
from threading import Thread
from typing import Any, Dict

import iso8601
import zmq
from swh.model.hashutil import hash_to_bytes
from swh.provenance import get_archive, get_provenance
from swh.provenance.archive import ArchiveInterface
from swh.provenance.revision import RevisionEntry, revision_add

# TODO: take this from a configuration file
conninfo = {
    "archive": {
        "cls": "direct",
        "db": {
            "host": "belvedere.internal.softwareheritage.org",
            "port": "5433",
            "dbname": "softwareheritage",
            "user": "guest",
        },
    },
    "provenance": {
        "cls": "rabbitmq",
        "url": "amqp://localhost:5672/%2f",
        "storage_config": {"cls": "postgresql", "db": {"service": "provenance"}},
    },
}


class Client(Process):
    def __init__(
        self,
        idx: int,
        threads: int,
        conninfo: Dict[str, Any],
        trackall: bool,
        lower: bool,
        mindepth: int,
    ):
        super().__init__()
        self.idx = idx
        self.threads = threads
        self.conninfo = conninfo
        self.trackall = trackall
        self.lower = lower
        self.mindepth = mindepth

    def run(self):
        # Using the same archive object for every worker to share internal caches.
        archive = get_archive(**self.conninfo["archive"])

        # Launch as many threads as requested
        workers = []
        for idx in range(self.threads):
            logging.info(f"Process {self.idx}: launching thread {idx}")
            worker = Worker(
                idx, archive, self.conninfo, self.trackall, self.lower, self.mindepth
            )
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
        trackall: bool,
        lower: bool,
        mindepth: int,
    ):
        super().__init__()
        self.idx = idx
        self.archive = archive
        self.server = conninfo["rev_server"]
        # Each worker has its own provenance object to isolate
        # the processing of each revision.
        # self.provenance = get_provenance(**conninfo["provenance"])
        self.trackall = trackall
        self.lower = lower
        self.mindepth = mindepth
        logging.info(
            f"Worker {self.idx} created ({self.trackall}, {self.lower}, {self.mindepth})"
        )

    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(self.server)
        with get_provenance(**conninfo["provenance"]) as provenance:
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
                    hash_to_bytes(response["rev"]),
                    date=date,
                    root=hash_to_bytes(response["root"]),
                )
                revision_add(
                    provenance,
                    self.archive,
                    [revision],
                    trackall=self.trackall,
                    lower=self.lower,
                    mindepth=self.mindepth,
                )


if __name__ == "__main__":
    # Check parameters
    if len(sys.argv) != 6:
        print("usage: client <processes> <port> <trackall> <lower> <mindepth>")
        exit(-1)

    processes = int(sys.argv[1])
    port = int(sys.argv[2])
    threads = 1  # int(sys.argv[2])
    trackall = sys.argv[3].lower() != "false"
    lower = sys.argv[4].lower() != "false"
    mindepth = int(sys.argv[5])
    conninfo["rev_server"] = f"tcp://localhost:{port}"

    # Start counter
    start = time.time()

    # Launch as many clients as requested
    clients = []
    for idx in range(processes):
        logging.info(f"MAIN: launching process {idx}")
        client = Client(idx, threads, conninfo, trackall, lower, mindepth)
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
