# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from contextlib import contextmanager
from typing import Any, Dict, Set

import pytest

from swh.model.swhids import CoreSWHID
from swh.provenance.backend import known_swhid_proxy

from .provenance_tests import TestProvenance, data  # noqa

pytest_plugins = [
    "swh.graph.pytest_plugin",
]


@pytest.fixture
def swh_provenance_config(graph_grpc_server) -> Dict[str, Any]:
    return {
        "cls": "known_swhid_filter",
        "provenance": {
            "cls": "graph",
            "url": graph_grpc_server,
        },
    }


@contextmanager
def patched_ignored_swhids(add: Set[CoreSWHID], remove=None):
    ignored_swhids = known_swhid_proxy.IGNORED_SWHIDS.copy()
    known_swhid_proxy.IGNORED_SWHIDS.update(add)
    if remove:
        known_swhid_proxy.IGNORED_SWHIDS.difference_update(remove)
    try:
        yield
    finally:
        known_swhid_proxy.IGNORED_SWHIDS.clear()
        known_swhid_proxy.IGNORED_SWHIDS.update(ignored_swhids)


class TestProvenanceFilterProxy:
    def test_filtered(self, swh_provenance):
        swhid = data.CONTENTS[0].swhid()
        with patched_ignored_swhids({swhid}):
            result = swh_provenance.whereis(swhid=swhid)
            assert result is None
