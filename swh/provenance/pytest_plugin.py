# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import contextlib
import logging
import multiprocessing
from pathlib import Path
import socket
import subprocess
import threading
import time

import grpc
import pytest

from swh.graph.example_dataset import DATASET_DIR
from swh.provenance import get_provenance
from swh.provenance.grpc.swhprovenance_pb2_grpc import ProvenanceServiceStub
from swh.provenance.grpc_server import (
    ExecutableNotFound,
    default_rust_executable_dir,
    spawn_rust_grpc_server,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def swh_provenance(swh_provenance_config):
    yield get_provenance(**swh_provenance_config)


class ProvenanceServerProcess(multiprocessing.Process):
    def __init__(self, config, *args, **kwargs):
        self.config = config
        self.q = multiprocessing.Queue()
        super().__init__(*args, **kwargs)

    def run(self):
        try:
            assert self.config["cls"] == "local_rust"
            (server, port) = spawn_rust_grpc_server(**self.config["grpc_server"])
            self.q.put(
                {
                    "grpc_url": f"localhost:{port}",
                    "port": port,
                    "pid": server.pid,
                }
            )
        except Exception as e:
            if isinstance(e, ExecutableNotFound):
                # hack to add a bit more context and help to the user,
                # especially when this is used from another swh package...
                # XXX on py>=3.11 we could use e.add_note() instead
                e.args = (
                    *e.args,
                    "This probably means you need to build the rust grpc server "
                    "for swh-provenance.",
                )
            logger.exception(e)
            self.q.put(e)

    def start(self, *args, **kwargs):
        super().start()
        self.result = self.q.get()


class StatsdServer:
    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.settimeout(0.1)
        (self.host, self.port) = self._sock.getsockname()
        self._closing = False
        self._thread = threading.Thread(target=self._listen)
        self._thread.start()
        self.datagrams = []
        self.new_datagram = threading.Event()
        """Woken up every time a datagram is added to self.datagrams."""

    def _listen(self):
        while not self._closing:
            try:
                (datagram, addr) = self._sock.recvfrom(4096)
            except TimeoutError:
                continue
            self.datagrams.append(datagram)
            self.new_datagram.set()
        self._sock.close()

    def close(self):
        self._closing = True


@pytest.fixture(scope="session")
def provenance_statsd_server():
    with contextlib.closing(StatsdServer()) as statsd_server:
        yield statsd_server


@pytest.fixture(scope="session", params=["rust"])
def provenance_grpc_backend_implementation(request):
    return request.param


@pytest.fixture(scope="session")
def provenance_database_and_graph(tmpdir_factory):
    database_path = tmpdir_factory.mktemp("provenance_database")
    subprocess.run(
        [
            f"{default_rust_executable_dir({})}/swh-provenance-gen-test-database",
            "main",
            database_path,
        ],
        check=True,
    )
    subprocess.run(
        [
            f"{default_rust_executable_dir({})}/swh-provenance-index",
            "--database",
            f"file://{database_path}",
        ],
        check=True,
    )
    return database_path


@pytest.fixture(scope="session")
def provenance_grpc_server_config(
    provenance_grpc_backend_implementation, provenance_statsd_server, provenance_database_and_graph
):
    return {
        "provenance": {
            "cls": f"local_{provenance_grpc_backend_implementation}",
            "grpc_server": {
                "db": f"file://{provenance_database_and_graph}",
                "graph": provenance_database_and_graph / "graph.json",
                "graph_format": "json",
                "debug": True,
                "statsd_host": provenance_statsd_server.host,
                "statsd_port": provenance_statsd_server.port,
            },
        }
    }


@pytest.fixture(scope="session")
def provenance_grpc_server_process(
    provenance_grpc_server_config, provenance_statsd_server
):
    server = ProvenanceServerProcess(provenance_grpc_server_config["provenance"])

    yield server

    try:
        server.kill()
    except AttributeError:
        # server was never started
        pass


@pytest.fixture(scope="session")
def provenance_grpc_server_started(provenance_grpc_server_process):
    server = provenance_grpc_server_process
    server.start()
    if isinstance(server.result, Exception):
        raise server.result

    # wait for the server to be up
    for _ in range(100):
        try:
            socket.create_connection(("localhost", server.result["port"]), timeout=1.0)
        except ConnectionRefusedError:
            time.sleep(0.01)

    yield server
    server.kill()


@pytest.fixture(scope="module")
def provenance_grpc_stub(provenance_grpc_server):
    with grpc.insecure_channel(provenance_grpc_server) as channel:
        stub = ProvenanceServiceStub(channel)
        yield stub


@pytest.fixture(scope="module")
def provenance_grpc_server(provenance_grpc_server_started):
    yield provenance_grpc_server_started.result["grpc_url"]
