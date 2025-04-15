# Copyright (C) 2024-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model.swhids import CoreSWHID, QualifiedSWHID
from swh.provenance import get_provenance


def test_grpc_whereis(provenance_grpc_server):
    provenance_client = get_provenance("grpc", url=provenance_grpc_server)

    assert provenance_client.whereis(
        swhid=CoreSWHID.from_string(
            "swh:1:cnt:0000000000000000000000000000000000000001"
        )
    ) == QualifiedSWHID.from_string(
        "swh:1:cnt:0000000000000000000000000000000000000001"
        ";anchor=swh:1:rev:0000000000000000000000000000000000000003"
        ";origin=https://example.com/swh/graph2"
    )


def test_grpc_whereare(provenance_grpc_server):
    provenance_client = get_provenance("grpc", url=provenance_grpc_server)

    assert set(
        provenance_client.whereare(
            swhids=[
                CoreSWHID.from_string(
                    "swh:1:cnt:0000000000000000000000000000000000000001"
                ),
                CoreSWHID.from_string(
                    "swh:1:cnt:0000000000000000000000000000000000000004"
                ),
            ]
        )
    ) in [
        {
            QualifiedSWHID.from_string(
                "swh:1:cnt:0000000000000000000000000000000000000001"
                ";anchor=swh:1:rev:0000000000000000000000000000000000000003"
                ";origin=https://example.com/swh/graph2"
            ),
            QualifiedSWHID.from_string(
                f"swh:1:cnt:0000000000000000000000000000000000000004"
                f";anchor={anchor}"
                f";origin={origin}"
            ),
        }
        for anchor in [
            "swh:1:rev:0000000000000000000000000000000000000009",
            "swh:1:rev:0000000000000000000000000000000000000013",
        ]
        for origin in (
            ["https://example.com/swh/graph2"]
            if anchor == "swh:1:rev:0000000000000000000000000000000000000013"
            else ["https://example.com/swh/graph", "https://example.com/swh/graph2"]
        )
    ]


def test_grpc_whereis_unknown_swhid(provenance_grpc_server):
    provenance_client = get_provenance("grpc", url=provenance_grpc_server)

    assert (
        provenance_client.whereis(
            swhid=CoreSWHID.from_string(
                "swh:1:cnt:ffffffffffffffffffffffffffffffffffffffff"
            )
        )
        is None
    )


def test_grpc_whereare_unknown_swhid(provenance_grpc_server):
    provenance_client = get_provenance("grpc", url=provenance_grpc_server)

    assert list(
        provenance_client.whereare(
            swhids=[
                CoreSWHID.from_string(
                    "swh:1:cnt:0000000000000000000000000000000000000001"
                ),
                CoreSWHID.from_string(
                    "swh:1:cnt:ffffffffffffffffffffffffffffffffffffffff"
                ),
            ]
        )
    ) == [
        QualifiedSWHID.from_string(
            "swh:1:cnt:0000000000000000000000000000000000000001;origin=https://example.com/swh/graph2;anchor=swh:1:rev:0000000000000000000000000000000000000003"
        ),
        None,
    ]
