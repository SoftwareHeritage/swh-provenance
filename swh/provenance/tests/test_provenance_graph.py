# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from typing import Any, Dict

import pytest
from swh.graph.grpc.swhgraph_pb2 import GraphDirection, TraversalRequest

from .provenance_tests import TestProvenance  # noqa

pytest_plugins = [
    "swh.graph.pytest_plugin",
]


@pytest.fixture
def swh_provenance_config(graph_grpc_server) -> Dict[str, Any]:
    return {
        "cls": "graph",
        "url": graph_grpc_server,
    }


class TestProvenanceGraphGRPC:
    def test_grpc_is_working(self, swh_provenance):
        resp = swh_provenance._stub.Traverse(
            TraversalRequest(
                src=["swh:1:cnt:0000000000000000000000000000000000000007"],
                edges="cnt:dir,dir:dir,dir:rev,rev:rev",
                direction=GraphDirection.BACKWARD,
                max_edges=1000,
            )
        )
        result = list(resp)
        assert len(result) == 6
