# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict

import pytest
import yaml

from swh.model.hashutil import hash_to_bytes
from swh.provenance.archive import ArchiveInterface
from swh.provenance.graph import IsochroneNode, build_isochrone_graph
from swh.provenance.interface import ProvenanceInterface
from swh.provenance.model import DirectoryEntry, RevisionEntry
from swh.provenance.revision import revision_add
from swh.provenance.tests.conftest import (
    fill_storage,
    get_datafile,
    load_repo_data,
    ts2dt,
)


def isochrone_graph_from_dict(d: Dict[str, Any], depth: int = 0) -> IsochroneNode:
    """Takes a dictionary representing a tree of IsochroneNode objects, and
    recursively builds the corresponding graph."""
    d = deepcopy(d)

    d["entry"]["id"] = hash_to_bytes(d["entry"]["id"])
    d["entry"]["name"] = bytes(d["entry"]["name"], encoding="utf-8")

    dbdate = d.get("dbdate", None)
    if dbdate is not None:
        dbdate = datetime.fromtimestamp(d["dbdate"], timezone.utc)

    children = d.get("children", [])

    node = IsochroneNode(
        entry=DirectoryEntry(**d["entry"]),
        dbdate=dbdate,
        depth=depth,
    )
    node.maxdate = datetime.fromtimestamp(d["maxdate"], timezone.utc)
    node.invalid = d.get("invalid", False)
    node.path = bytes(d["path"], encoding="utf-8")
    node.children = set(
        isochrone_graph_from_dict(child, depth=depth + 1) for child in children
    )
    return node


@pytest.mark.parametrize(
    "repo, lower, mindepth",
    (
        ("cmdbts2", True, 1),
        ("cmdbts2", False, 1),
        ("cmdbts2", True, 2),
        ("cmdbts2", False, 2),
        ("out-of-order", True, 1),
    ),
)
@pytest.mark.parametrize("batch", (True, False))
def test_isochrone_graph(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    repo: str,
    lower: bool,
    mindepth: int,
    batch: bool,
) -> None:
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data(repo)
    fill_storage(archive.storage, data)

    revisions = {rev["id"]: rev for rev in data["revision"]}
    filename = f"graphs_{repo}_{'lower' if lower else 'upper'}_{mindepth}.yaml"

    with open(get_datafile(filename)) as file:
        for expected in yaml.full_load(file):
            print("# Processing revision", expected["rev"])
            revision = revisions[hash_to_bytes(expected["rev"])]
            entry = RevisionEntry(
                id=revision["id"],
                date=ts2dt(revision["date"]),
                root=revision["directory"],
            )
            expected_graph = isochrone_graph_from_dict(expected["graph"])
            print("Expected graph:", expected_graph)

            # Create graph for current revision and check it has the expected structure.
            assert entry.root is not None
            computed_graph = build_isochrone_graph(
                provenance,
                archive,
                entry,
                DirectoryEntry(entry.root),
            )
            print("Computed graph:", computed_graph)
            assert computed_graph == expected_graph

            # Add current revision so that provenance info is kept up to date for the
            # following ones.
            revision_add(
                provenance,
                archive,
                [entry],
                lower=lower,
                mindepth=mindepth,
                commit=not batch,
            )
