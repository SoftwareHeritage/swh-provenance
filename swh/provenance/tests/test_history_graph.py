# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import yaml

from swh.model.hashutil import hash_to_bytes
from swh.provenance.graph import HistoryNode, build_history_graph
from swh.provenance.model import OriginEntry, RevisionEntry
from swh.provenance.origin import origin_add_revision
from swh.provenance.tests.conftest import fill_storage, get_datafile, load_repo_data


def history_graph_from_dict(d) -> HistoryNode:
    """Takes a dictionary representing a tree of HistoryNode objects, and
    recursively builds the corresponding graph."""
    node = HistoryNode(
        entry=RevisionEntry(hash_to_bytes(d["rev"])),
        visited=d.get("visited", False),
        in_history=d.get("in_history", False),
    )
    node.parents = set(
        history_graph_from_dict(parent) for parent in d.get("parents", [])
    )
    return node


@pytest.mark.parametrize(
    "repo, visit",
    (("with-merges", "visits-01"),),
)
@pytest.mark.parametrize("batch", (True, False))
def test_history_graph(provenance, swh_storage, archive, repo, visit, batch):
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data(repo)
    fill_storage(swh_storage, data)

    filename = f"history_graphs_{repo}_{visit}.yaml"

    with open(get_datafile(filename)) as file:
        for expected in yaml.full_load(file):
            entry = OriginEntry(expected["origin"], hash_to_bytes(expected["snapshot"]))
            provenance.origin_add(entry)

            for graph_as_dict in expected["graphs"]:
                expected_graph = history_graph_from_dict(graph_as_dict)
                print("Expected graph:", expected_graph)

                computed_graph = build_history_graph(
                    archive,
                    provenance,
                    RevisionEntry(hash_to_bytes(graph_as_dict["rev"])),
                )
                print("Computed graph:", computed_graph)
                assert computed_graph == expected_graph

                origin_add_revision(provenance, entry, computed_graph)

            if not batch:
                provenance.commit()
