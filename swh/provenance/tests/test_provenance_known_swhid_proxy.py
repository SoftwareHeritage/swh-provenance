# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from contextlib import contextmanager
from typing import Any, Dict, Set

import pytest

from swh.model.swhids import CoreSWHID
from swh.provenance import get_provenance
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
def patched_ignored_swhids(
    add: Set[CoreSWHID], dataset: Set[CoreSWHID] = known_swhid_proxy.IGNORED_SWHIDS
):
    ignored_swhids = dataset.copy()
    dataset.update(add)
    try:
        yield
    finally:
        dataset.clear()
        dataset.update(ignored_swhids)


class TestProvenanceFilterProxy:
    def test_whereis_filtered(self, swh_provenance):
        swhid = data.CONTENTS[0].swhid()
        assert swh_provenance.whereis(swhid=swhid) is not None
        with patched_ignored_swhids({swhid}):
            assert swh_provenance.whereis(swhid=swhid) is None

    def test_filtered_licenses(self, swh_provenance, swh_provenance_config):
        swhid = data.CONTENTS[0].swhid()
        cfg2 = swh_provenance_config.copy()
        cfg2["filter_licenses"] = True
        swh_provenance2 = get_provenance(**cfg2)

        assert swh_provenance.filter_licenses is False
        assert swh_provenance2.filter_licenses is True

        assert swh_provenance.whereis(swhid=swhid) is not None
        assert swh_provenance2.whereis(swhid=swhid) is not None

        with patched_ignored_swhids({swhid}, known_swhid_proxy.LICENSES):
            assert swh_provenance.whereis(swhid=swhid) is not None
            assert swh_provenance2.whereis(swhid=swhid) is None  # filtered

    def test_whereare_filtered(self, swh_provenance):
        swhid = data.CONTENTS[0].swhid()
        assert swh_provenance.whereare(swhids=[swhid])[0] is not None
        with patched_ignored_swhids({swhid}):
            assert swh_provenance.whereare(swhids=[swhid])[0] is None
