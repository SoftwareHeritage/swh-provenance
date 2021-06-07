# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone
import pytest
import yaml

from swh.model.hashutil import hash_to_bytes
from swh.provenance.model import DirectoryEntry, RevisionEntry
from swh.provenance.provenance import IsochroneNode, build_isochrone_graph, revision_add
from swh.provenance.tests.conftest import fill_storage, get_datafile, load_repo_data
from swh.provenance.tests.test_provenance_db import ts2dt


def isochrone_graph_from_dict(d, depth=0) -> IsochroneNode:
    """Takes a dictionary representing a tree of IsochroneNode objects, and
    recursively builds the corresponding graph."""
    d = d.copy()

    d["entry"]["id"] = hash_to_bytes(d["entry"]["id"])
    d["entry"]["name"] = bytes(d["entry"]["name"], encoding="utf-8")

    if d["dbdate"] is not None:
        d["dbdate"] = datetime.fromtimestamp(d["dbdate"], timezone.utc)

    if d["maxdate"] is not None:
        d["maxdate"] = datetime.fromtimestamp(d["maxdate"], timezone.utc)

    node = IsochroneNode(
        entry=DirectoryEntry(**d["entry"]),
        dbdate=d["dbdate"],
        depth=depth,
    )
    node.maxdate = d["maxdate"]
    node.known = d["known"]
    node.path = bytes(d["path"], encoding="utf-8")
    node.children = [
        isochrone_graph_from_dict(child, depth=depth + 1) for child in d["children"]
    ]
    return node


@pytest.mark.parametrize(
    "repo, lower, mindepth",
    (
        ("cmdbts2", True, 1),
        # ("cmdbts2", False, 1),
        # ("cmdbts2", True, 2),
        # ("cmdbts2", False, 2),
        # ("out-of-order", True, 1),
    ),
)
def test_isochrone_graph(provenance, swh_storage, archive, repo, lower, mindepth):
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data(repo)
    fill_storage(swh_storage, data)

    revisions = {rev["id"]: rev for rev in data["revision"]}
    filename = f"graphs_{repo}_{'lower' if lower else 'upper'}_{mindepth}.yaml"

    with open(get_datafile(filename)) as file:
        expected = yaml.full_load(file)

        for rev, graph_as_dict in expected.items():
            revision = revisions[hash_to_bytes(rev)]
            entry = RevisionEntry(
                id=revision["id"],
                date=ts2dt(revision["date"]),
                root=revision["directory"],
            )
            expected_graph = isochrone_graph_from_dict(graph_as_dict)
            print("Expected", expected_graph)

            # Create graph for current revision and check it has the expected structure.
            computed_graph = build_isochrone_graph(
                archive,
                provenance,
                entry,
                DirectoryEntry(entry.root),
            )
            print("Computed", computed_graph)
            assert computed_graph == expected_graph

            # Add current revision so that provenance info is kept up to date for the
            # following ones.
            revision_add(provenance, archive, [entry], lower=lower, mindepth=mindepth)
