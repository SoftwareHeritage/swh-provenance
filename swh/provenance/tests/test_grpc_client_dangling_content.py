# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import subprocess

import pytest

from swh.model.swhids import CoreSWHID, QualifiedSWHID
from swh.provenance import get_provenance
from swh.provenance.grpc_server import default_rust_executable_dir

# locally "redefine" all fixtures that depend on the session-scoped
# provenance_database_and_graph, because we need pytest to call them again.
from swh.provenance.pytest_plugin import (  # noqa
    provenance_grpc_server_config,
    provenance_grpc_server_process,
    provenance_grpc_server_started,
)


@pytest.fixture(scope="session")
def provenance_database_and_graph(tmpdir_factory):
    database_path = tmpdir_factory.mktemp("provenance_database")
    subprocess.run(
        [
            f"{default_rust_executable_dir({})}/swh-provenance-gen-test-database",
            "dangling-content",
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


def test_grpc_whereis_dangling_content(provenance_grpc_server):
    provenance_client = get_provenance("grpc", url=provenance_grpc_server)

    assert provenance_client.whereis(
        swhid=CoreSWHID.from_string(
            "swh:1:cnt:0000000000000000000000000000000000000004"
        )
    ) == QualifiedSWHID.from_string(
        "swh:1:cnt:0000000000000000000000000000000000000004"
    )


def test_grpc_whereare_dangingl_content(provenance_grpc_server):
    provenance_client = get_provenance("grpc", url=provenance_grpc_server)

    assert set(
        provenance_client.whereare(
            swhids=[
                CoreSWHID.from_string(
                    "swh:1:cnt:0000000000000000000000000000000000000004"
                ),
                CoreSWHID.from_string(
                    "swh:1:cnt:0000000000000000000000000000000000000002"
                ),
            ]
        )
    ) == {
        QualifiedSWHID.from_string(
            "swh:1:cnt:0000000000000000000000000000000000000002"
            ";anchor=swh:1:rev:0000000000000000000000000000000000000000"
        ),
        QualifiedSWHID.from_string(
            "swh:1:cnt:0000000000000000000000000000000000000004"
        ),
    }
