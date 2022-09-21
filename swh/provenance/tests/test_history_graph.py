# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import yaml

from swh.model.hashutil import hash_to_bytes
from swh.provenance.archive import ArchiveInterface
from swh.provenance.graph import HistoryGraph
from swh.provenance.interface import ProvenanceInterface
from swh.provenance.model import OriginEntry, RevisionEntry
from swh.provenance.origin import origin_add_revision
from swh.provenance.tests.conftest import fill_storage, get_datafile, load_repo_data


@pytest.mark.origin_layer
@pytest.mark.parametrize(
    "repo, visit",
    (("with-merges", "visits-01"),),
)
@pytest.mark.parametrize("batch", (True, False))
def test_history_graph(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    repo: str,
    visit: str,
    batch: bool,
) -> None:
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data(repo)
    fill_storage(archive.storage, data)

    filename = f"history_graphs_{repo}_{visit}.yaml"

    with open(get_datafile(filename)) as file:
        for expected in yaml.full_load(file):
            entry = OriginEntry(expected["origin"], hash_to_bytes(expected["snapshot"]))
            provenance.origin_add(entry)

            for expected_graph_as_dict in expected["graphs"]:
                print("Expected graph:", expected_graph_as_dict)

                computed_graph = HistoryGraph(
                    archive,
                    RevisionEntry(hash_to_bytes(expected_graph_as_dict["head"])),
                )
                print("Computed graph:", computed_graph.as_dict())
                assert computed_graph.as_dict() == expected_graph_as_dict

                origin_add_revision(provenance, entry, computed_graph)

            if not batch:
                provenance.flush()
