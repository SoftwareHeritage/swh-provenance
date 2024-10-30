# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.provenance.grpc.swhprovenance_pb2 import WhereIsOneRequest, WhereIsOneResult


def test_grpc_whereis(provenance_grpc_stub):
    result = provenance_grpc_stub.WhereIsOne(
        WhereIsOneRequest(swhid="swh:1:cnt:0000000000000000000000000000000000000001")
    )
    assert result == WhereIsOneResult(
        swhid="swh:1:cnt:0000000000000000000000000000000000000001",
        anchor="swh:1:rev:0000000000000000000000000000000000000003",
    )
