# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime

import pytest

from swh.model.hashutil import hash_to_bytes
from swh.provenance.api.client import split_ranges
from swh.provenance.interface import RelationData, RelationType


def test_split_ranges_for_relation() -> None:
    data = {
        hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"): {
            RelationData(
                hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"), b"/path/1"
            ),
            RelationData(
                hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"), b"/path/2"
            ),
        },
        hash_to_bytes("d0d8929936631ecbcf9147be6b8aa13b13b014e4"): {
            RelationData(
                hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"), b"/path/3"
            ),
        },
        hash_to_bytes("c1d8929936631ecbcf9147be6b8aa13b13b014e4"): {
            RelationData(
                hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"), b"/path/4"
            ),
        },
    }
    assert split_ranges(data, "relation_add", RelationType.CNT_EARLY_IN_REV) == {
        "relation_add.content_in_revision.c": {
            (
                hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
                hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"),
                b"/path/1",
            ),
            (
                hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
                hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"),
                b"/path/2",
            ),
            (
                hash_to_bytes("c1d8929936631ecbcf9147be6b8aa13b13b014e4"),
                hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"),
                b"/path/4",
            ),
        },
        "relation_add.content_in_revision.d": {
            (
                hash_to_bytes("d0d8929936631ecbcf9147be6b8aa13b13b014e4"),
                hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"),
                b"/path/3",
            ),
        },
    }


def test_split_ranges_error_for_relation() -> None:
    set_data = {hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4")}
    with pytest.raises(AssertionError) as ex:
        split_ranges(set_data, "relation_add", RelationType.CNT_EARLY_IN_REV)
    assert "Relation data must be provided in a dictionary" in str(ex.value)

    tuple_values = {
        hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"): (
            hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"),
            b"/path/3",
        )
    }
    with pytest.raises(AssertionError) as ex:
        split_ranges(tuple_values, "relation_add", RelationType.CNT_EARLY_IN_REV)
    assert "Values in the dictionary must be RelationData structures" in str(ex.value)


@pytest.mark.parametrize(
    "entity",
    ("content", "directory", "origin", "revision"),
)
def test_split_ranges_for_entity_without_data(entity: str) -> None:
    data = {
        hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
        hash_to_bytes("d0d8929936631ecbcf9147be6b8aa13b13b014e4"),
        hash_to_bytes("c1d8929936631ecbcf9147be6b8aa13b13b014e4"),
    }
    meth_name = f"{entity}_add"
    assert split_ranges(data, meth_name, None) == {
        f"{meth_name}.unknown.c": {
            (hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),),
            (hash_to_bytes("c1d8929936631ecbcf9147be6b8aa13b13b014e4"),),
        },
        f"{meth_name}.unknown.d": {
            (hash_to_bytes("d0d8929936631ecbcf9147be6b8aa13b13b014e4"),),
        },
    }


@pytest.mark.parametrize(
    "entity",
    ("content", "directory", "origin", "revision"),
)
def test_split_ranges_for_entity_with_data(entity: str) -> None:
    data = {
        hash_to_bytes(
            "c0d8929936631ecbcf9147be6b8aa13b13b014e4"
        ): datetime.fromtimestamp(1000000000),
        hash_to_bytes(
            "d0d8929936631ecbcf9147be6b8aa13b13b014e4"
        ): datetime.fromtimestamp(1000000001),
        hash_to_bytes(
            "c1d8929936631ecbcf9147be6b8aa13b13b014e4"
        ): datetime.fromtimestamp(1000000002),
    }
    meth_name = f"{entity}_add"
    assert split_ranges(data, meth_name, None) == {
        f"{meth_name}.unknown.c": {
            (
                hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
                datetime.fromtimestamp(1000000000),
            ),
            (
                hash_to_bytes("c1d8929936631ecbcf9147be6b8aa13b13b014e4"),
                datetime.fromtimestamp(1000000002),
            ),
        },
        f"{meth_name}.unknown.d": {
            (
                hash_to_bytes("d0d8929936631ecbcf9147be6b8aa13b13b014e4"),
                datetime.fromtimestamp(1000000001),
            ),
        },
    }
