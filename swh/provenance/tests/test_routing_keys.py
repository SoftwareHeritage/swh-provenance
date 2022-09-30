# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.model.hashutil import hash_to_bytes
from swh.provenance.storage.interface import RelationType
from swh.provenance.storage.rabbitmq.server import ProvenanceStorageRabbitMQServer


def test_routing_keys_for_entity() -> None:
    assert (
        ProvenanceStorageRabbitMQServer.get_routing_key(
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            "content_add",
            None,
        )
        == "content_add.unknown.c"
    )


def test_routing_keys_for_relation() -> None:
    assert (
        ProvenanceStorageRabbitMQServer.get_routing_key(
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            "relation_add",
            RelationType.CNT_EARLY_IN_REV,
        )
        == "relation_add.content_in_revision.c"
    )


def test_routing_key_error_for_entity() -> None:
    with pytest.raises(AssertionError) as ex:
        ProvenanceStorageRabbitMQServer.get_routing_key(
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            "content_add",
            RelationType.CNT_EARLY_IN_REV,
        )
    assert "'content_add' requires 'relation' to be None" in str(ex.value)


def test_routing_key_error_for_relation() -> None:
    with pytest.raises(AssertionError) as ex:
        ProvenanceStorageRabbitMQServer.get_routing_key(
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            "relation_add",
            None,
        )
    assert "'relation_add' requires 'relation' to be provided" in str(ex.value)


@pytest.mark.parametrize(
    "entity",
    ("content", "directory", "origin", "revision"),
)
def test_routing_keys_range(entity: str) -> None:
    meth_name = f"{entity}_add"
    for range in ProvenanceStorageRabbitMQServer.get_ranges(entity):
        id = hash_to_bytes(f"{range:x}000000000000000000000000000000000000000")
        assert (
            ProvenanceStorageRabbitMQServer.get_routing_key(id, meth_name, None)
            == f"{meth_name}.unknown.{range:x}"
        )
