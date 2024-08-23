# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import pytest

from swh.provenance import get_provenance
from swh.provenance.api import server
from swh.provenance.api.client import RemoteProvenance

from .provenance_tests import TestProvenance  # noqa

pytest_plugins = [
    "swh.graph.pytest_plugin",
]


@pytest.fixture
def app_server(graph_grpc_server):
    server.provenance = get_provenance(
        cls="graph",
        url=graph_grpc_server,
    )
    yield server


@pytest.fixture
def app(app_server):
    return app_server.app


@pytest.fixture
def swh_rpc_client_class():
    return RemoteProvenance


@pytest.fixture
def swh_provenance(swh_rpc_client, app_server):
    provenance = swh_rpc_client
    yield provenance


def test_remote(swh_provenance):
    assert swh_provenance.get("") == "SWH Provenance API server"
