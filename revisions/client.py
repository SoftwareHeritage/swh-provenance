#!/usr/bin/env python

import logging
import subprocess
import sys
import time
import zmq

from multiprocessing import Process
from threading import Thread


from datetime import datetime
from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.provenance import (
    ArchiveInterface,
    get_archive,
    get_provenance
)
from swh.provenance.provenance import revision_add
from swh.provenance.revision import RevisionEntry
from typing import Any, Dict


# TODO: take this from a configuration file
conninfo = {
    "archive": {
        "cls": "ps",
        "db": {
            "host": "somerset.internal.softwareheritage.org",
            "port": "5433",
            "dbname": "softwareheritage",
            "user": "guest"
        }
    },
    "provenance": {
        "cls": "ps",
        "db": {
            "host": "/var/run/postgresql",
            "port": "5436",
            # "dbname": "postgres"
        }
    },
    "server": "tcp://localhost:5556"
}


class Client(Process):
    def __init__(self, idx: int, threads: int, conninfo : Dict[str, Any]):
        super().__init__()
        self.idx = idx
        self.threads = threads
        self.conninfo = conninfo

    def run(self):
        # Using the same archive object for every worker to share internal caches.
        archive = get_archive(**self.conninfo["archive"])

        # Launch as many threads as requested
        workers = []
        for idx in range(self.threads):
            logging.info(f"Process {self.idx}: launching thread {idx}")
            worker = Worker(idx, archive, self.conninfo)
            worker.start()
            workers.append(worker)

        # Wait for all threads to complete their work
        for idx, worker in enumerate(workers):
            logging.info(f"Process {self.idx}: waiting for thread {idx} to finish")
            worker.join()
            logging.info(f"Process {self.idx}: thread {idx} finished executing")


class Worker(Thread):
    def __init__(self, idx: int, archive : ArchiveInterface, conninfo : Dict[str, Any]):
        super().__init__()
        self.idx = idx
        self.archive = archive
        self.server = conninfo["server"]
        # Each worker has its own provenance object to isolate
        # the processing of each revision.
        self.provenance = get_provenance(**conninfo["provenance"])

    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(self.server)
        while True:
            socket.send(b"NEXT")
            message = socket.recv_json()

            if message is None:
                break

            revision = RevisionEntry(
                self.archive,
                hash_to_bytes(message["rev"]),
                date=datetime.fromisoformat(message["date"]),
                root=hash_to_bytes(message["root"])
            )
            revision_add(self.provenance, self.archive, revision)


if __name__ == "__main__":
    # Check parameters
    if len(sys.argv) < 3:
        print("usage: client <processes> <threads>")
        exit(-1)

    processes = int(sys.argv[1])
    threads = int(sys.argv[2])
    dbname = f"proc{processes}thread{threads}"

    # Set logging level
    # logging.getLogger().setLevel(logging.INFO)

    # Create database
    logging.info(f"MAIN: creating provenance database {dbname}")
    status = subprocess.run(
        ["swh", "provenance", "create", "--name", dbname],
        capture_output=True
    )
    if status.returncode != 0:
        logging.error("Failed to create provenance database")
        exit(-1)
    logging.info(f"MAIN: database {dbname} successfuly created")

    conninfo["provenance"]["db"]["dbname"] = dbname

    # Start counter
    start = time.time()

    # Launch as many clients as requested
    clients = []
    for idx in range(processes):
        logging.info(f"MAIN: launching process {idx}")
        client = Client(idx, threads, conninfo)
        client.start()
        clients.append(client)

    # Wait for all processes to complete their work
    for idx, client in enumerate(clients):
        logging.info(f"MAIN: waiting for process {idx} to finish")
        client.join()
        logging.info(f"MAIN: process {idx} finished executing")

    # Stop counter and report elapsed time
    stop = time.time()
    print("Elapsed time:", stop-start, "seconds")
