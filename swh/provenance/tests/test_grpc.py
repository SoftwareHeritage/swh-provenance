# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.provenance.grpc.swhprovenance_pb2 import WhereIsOneRequest, WhereIsOneResult


def test_grpc_whereis1(provenance_grpc_stub):
    # Uses c-in-r only
    result = provenance_grpc_stub.WhereIsOne(
        WhereIsOneRequest(swhid="swh:1:cnt:0000000000000000000000000000000000000001")
    )
    assert result == WhereIsOneResult(
        swhid="swh:1:cnt:0000000000000000000000000000000000000001",
        anchor="swh:1:rev:0000000000000000000000000000000000000003",
    )


def test_grpc_whereis2(provenance_grpc_stub):
    # Uses c-in-d + d-in-r, as the only path from revisions to cnt:0004 is through dir:0006,
    # which is a frontier
    result = provenance_grpc_stub.WhereIsOne(
        WhereIsOneRequest(swhid="swh:1:cnt:0000000000000000000000000000000000000004")
    )
    assert result in (
        WhereIsOneResult(
            swhid="swh:1:cnt:0000000000000000000000000000000000000004",
            anchor="swh:1:rev:0000000000000000000000000000000000000009",
        ),
        WhereIsOneResult(
            swhid="swh:1:cnt:0000000000000000000000000000000000000004",
            anchor="swh:1:rev:0000000000000000000000000000000000000013",
        ),
    )
