# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.provenance import get_archive
from swh.provenance.multiplexer.archive import ArchiveMultiplexed
from swh.provenance.storage.archive import ArchiveStorage
from swh.provenance.swhgraph.archive import ArchiveGraph


def test_multiplexer_configuration():
    config = {
        "archives": [
            {
                "cls": "graph",
                "url": "graph_url",
                "storage": {"cls": "remote", "url": "storage_graph_url"},
            },
            {"cls": "api", "storage": {"cls": "remote", "url": "storage_api_url"}},
        ]
    }

    archive = get_archive(cls="multiplexer", **config)
    assert isinstance(archive, ArchiveMultiplexed)
    assert len(archive.archives) == 2
    assert isinstance(archive.archives[0], ArchiveGraph)
    assert isinstance(archive.archives[1], ArchiveStorage)
