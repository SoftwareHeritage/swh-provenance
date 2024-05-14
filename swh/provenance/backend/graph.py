# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import grpc
from swh.graph.grpc import swhgraph_pb2_grpc
from swh.model.swhids import CoreSWHID


class GraphProvenance:
    def __init__(self, url):
        self.graph_url = url
        self._channel = grpc.insecure_channel(self.graph_url)
        self._stub = swhgraph_pb2_grpc.TraversalServiceStub(self._channel)

    def check_config(self) -> bool:
        return True

    def whereis(self, swhid: CoreSWHID):
        return swhid
