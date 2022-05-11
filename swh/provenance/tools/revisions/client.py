#!/usr/bin/env python

from datetime import timezone
import logging
import logging.handlers
import multiprocessing
import os
import sys
import time
from typing import Any, Callable, Dict, List, Optional

import iso8601
from swh.core import config
from swh.model.hashutil import hash_to_bytes
from swh.provenance import get_archive, get_provenance
from swh.provenance.revision import RevisionEntry, revision_add
import yaml
import zmq

CONFIG_ENVVAR = "SWH_CONFIG_FILENAME"

DEFAULT_PATH = os.environ.get(CONFIG_ENVVAR, None)


class Client(multiprocessing.Process):
    def __init__(
        self,
        conf: Dict[str, Any],
        trackall: bool,
        flatten: bool,
        lower: bool,
        mindepth: int,
        group: None = None,
        target: Optional[Callable[..., Any]] = ...,
        name: Optional[str] = ...,
    ) -> None:
        super().__init__(group=group, target=target, name=name)
        self.archive_conf = conf["archive"]
        self.storage_conf = conf["storage"]
        self.url = f"tcp://{conf['rev_server']['host']}:{conf['rev_server']['port']}"
        self.trackall = trackall
        self.flatten = flatten
        self.lower = lower
        self.mindepth = mindepth
        logging.info(f"Client {self.name} created")

    def run(self):
        logging.info(f"Client {self.name} started")
        # XXX: should we reconnect on each iteration to save resources?
        archive = get_archive(**self.archive_conf)

        context = zmq.Context()
        socket: zmq.Socket = context.socket(zmq.REQ)
        socket.connect(self.url)

        with get_provenance(**self.storage_conf) as provenance:
            while True:
                socket.send(b"NEXT")
                response = socket.recv_json()

                if response is None:
                    break

                batch = []
                for revision in response:
                    # Ensure date has a valid timezone
                    date = iso8601.parse_date(revision["date"])
                    if date.tzinfo is None:
                        date = date.replace(tzinfo=timezone.utc)
                    batch.append(
                        RevisionEntry(
                            hash_to_bytes(revision["rev"]),
                            date=date,
                            root=hash_to_bytes(revision["root"]),
                        )
                    )
                revision_add(
                    provenance,
                    archive,
                    batch,
                    trackall=self.trackall,
                    flatten=self.flatten,
                    lower=self.lower,
                    mindepth=self.mindepth,
                )
        logging.info(f"Client {self.name} stopped")


if __name__ == "__main__":
    # Check parameters
    if len(sys.argv) != 6:
        print("usage: client <processes> <trackall> <flatten> <lower> <mindepth>")
        exit(-1)

    processes = int(sys.argv[1])
    trackall = sys.argv[2].lower() != "false"
    flatten = sys.argv[3].lower() != "false"
    lower = sys.argv[4].lower() != "false"
    mindepth = int(sys.argv[5])

    config_file = None  # TODO: add as a cli option
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
    clients: List[Client] = []
    for idx in range(processes):
        logging.info(f"MAIN: launching process {idx}")
        client = Client(
            conf,
            trackall=trackall,
            flatten=flatten,
            lower=lower,
            mindepth=mindepth,
            name=f"worker{idx}",
        )
        client.start()
        clients.append(client)

    # Wait for all processes to complete their work
    for client in clients:
        logging.info(f"MAIN: waiting for process {client.name} to finish")
        client.join()
        logging.info(f"MAIN: process {client.name} finished executing")

    # Stop counter and report elapsed time
    stop = time.time()
    print("Elapsed time:", stop - start, "seconds")
