#!/usr/bin/env python

# import logging
import threading
import zmq

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
            "dbname": "process2"
        }
    },
    "server": "tcp://localhost:5556"
}


class Worker(threading.Thread):
    def __init__(
        self,
        idx : int,
        serverinfo: str,
        conninfo : Dict[str, Any],
        archive : ArchiveInterface
    ):
        super().__init__()
        self.id = idx
        self.serverinfo = serverinfo
        self.archive = archive
        # Each worker has its own provenance object to isolate
        # the processing of each revision.
        self.provenance = get_provenance(**conninfo)

    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(self.serverinfo)
        while True:
            # logging.info(f"Worker {self.id} requesting revision")
            socket.send(b"NEXT")
            message = socket.recv_json()

            if message is None:
                break
            # logging.info(f"Worker {self.id} got new revision")

            revision = RevisionEntry(
                self.archive,
                hash_to_bytes(message["rev"]),
                date=datetime.fromisoformat(message["date"]),
                root=hash_to_bytes(message["root"])
            )
            # logging.info(f"Worker {self.id} processing revision {hash_to_hex(revision.id)}")
            revision_add(self.provenance, self.archive, revision)
            # logging.info(f"Worker {self.id} done with revision {hash_to_hex(revision.id)}")


if __name__ == "__main__":
    # logging.getLogger().setLevel(logging.INFO)

    # Using the same archive object for every worker to share internal caches.
    archive = get_archive(**conninfo["archive"])

    threads = 1     # TODO: make this a command line parameter
    workers = []
    for idx in range(threads):
        # logging.info(f"Launching worker {idx}")
        worker = Worker(idx, conninfo["server"], conninfo["provenance"], archive)
        worker.start()
        workers.append(worker)

    for idx, worker in enumerate(workers):
        # logging.info(f"Waiting for worker {idx} to finish")
        worker.join()
        # logging.info(f"Worker {idx} finished executing")
