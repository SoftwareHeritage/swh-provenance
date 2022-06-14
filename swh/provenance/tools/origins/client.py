#!/usr/bin/env python

# Copyright (C) 2021-2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import logging.handlers
import multiprocessing
import os
import sys
import time
from typing import Any, Callable, Dict, List, Optional

import yaml
import zmq

from swh.core import config
from swh.model.hashutil import hash_to_bytes
from swh.provenance import get_archive, get_provenance
from swh.provenance.origin import OriginEntry, origin_add

CONFIG_ENVVAR = "SWH_CONFIG_FILENAME"

DEFAULT_PATH = os.environ.get(CONFIG_ENVVAR, None)
LOG_FORMAT = (
    "%(asctime)s-%(levelname)s - %(process)d - %(name)s.%(funcName)s:%(lineno)d"
    ": %(message)s"
)


class Client(multiprocessing.Process):
    def __init__(
        self,
        conf: Dict[str, Any],
        group: None = None,
        target: Optional[Callable[..., Any]] = ...,
        name: Optional[str] = ...,
    ) -> None:
        super().__init__(group=group, target=target, name=name)
        self.archive_conf = conf["archive"]
        self.storage_conf = conf["storage"]
        self.url = f"tcp://{conf['org_server']['host']}:{conf['org_server']['port']}"
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
                for origin in response:
                    batch.append(
                        OriginEntry(origin["url"], hash_to_bytes(origin["snapshot"]))
                    )
                origin_add(provenance, archive, batch)
        logging.info(f"Client {self.name} stopped")


if __name__ == "__main__":

    logging.basicConfig(level=logging.WARN, format=LOG_FORMAT)

    # Check parameters
    if len(sys.argv) != 2:
        print("usage: client <processes>")
        exit(-1)

    processes = int(sys.argv[1])

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
        client = Client(conf, name=f"worker{idx}")
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
