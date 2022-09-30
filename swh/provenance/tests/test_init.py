# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.provenance.archive import get_archive
from swh.provenance.archive.multiplexer import ArchiveMultiplexed
from swh.provenance.archive.storage import ArchiveStorage
from swh.provenance.archive.swhgraph import ArchiveGraph


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
    assert isinstance(archive.archives[0][0], str)
    assert isinstance(archive.archives[0][1], ArchiveGraph)
    assert isinstance(archive.archives[1][0], str)
    assert isinstance(archive.archives[1][1], ArchiveStorage)
