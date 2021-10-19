#!/usr/bin/env python

import logging
import logging.handlers
import os
import sys
import time
from datetime import timezone
from multiprocessing import Process
from threading import Thread
from typing import Any, Dict

import iso8601
import yaml
import zmq
from swh.core import config
from swh.model.hashutil import hash_to_bytes
from swh.provenance import get_archive, get_provenance
from swh.provenance.archive import ArchiveInterface
from swh.provenance.revision import RevisionEntry, revision_add

# All generic config code should reside in swh.core.config
CONFIG_ENVVAR = "SWH_CONFIG_FILENAME"
DEFAULT_PATH = os.environ.get(CONFIG_ENVVAR, None)


class Client(Process):
    def __init__(
        self,
        idx: int,
        threads: int,
        conf: Dict[str, Any],
        trackall: bool,
        lower: bool,
        mindepth: int,
    ):
        super().__init__()
        self.idx = idx
        self.threads = threads
        self.conf = conf
        self.trackall = trackall
        self.lower = lower
        self.mindepth = mindepth

    def run(self):
        # Using the same archive object for every worker to share internal caches.
        archive = get_archive(**self.conf["archive"])

        # Launch as many threads as requested
        workers = []
        for idx in range(self.threads):
            logging.info(f"Process {self.idx}: launching thread {idx}")
            worker = Worker(
                idx, archive, self.conf, self.trackall, self.lower, self.mindepth
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
        conf: Dict[str, Any],
        trackall: bool,
        lower: bool,
        mindepth: int,
    ):
        super().__init__()
        self.idx = idx
        self.archive = archive
        self.storage_conf = conf["storage"]
        self.url = f"tcp://{conf['rev_server']['host']}:{conf['rev_server']['port']}"
        # Each worker has its own provenance object to isolate
        # the processing of each revision.
        # self.provenance = get_provenance(**storage_conf)
        self.trackall = trackall
        self.lower = lower
        self.mindepth = mindepth
        logging.info(
            f"Worker {self.idx} created ({self.trackall}, {self.lower}, {self.mindepth})"
        )

    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(self.url)
        with get_provenance(**self.storage_conf) as provenance:
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
    if len(sys.argv) != 5:
        print("usage: client <processes> <trackall> <lower> <mindepth>")
        exit(-1)

    processes = int(sys.argv[1])
    threads = 1  # int(sys.argv[2])
    trackall = sys.argv[2].lower() != "false"
    lower = sys.argv[3].lower() != "false"
    mindepth = int(sys.argv[4])

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

    # Start counter
    start = time.time()

    # Launch as many clients as requested
    clients = []
    for idx in range(processes):
        logging.info(f"MAIN: launching process {idx}")
        client = Client(idx, threads, conf, trackall, lower, mindepth)
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
