# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict

from confluent_kafka import Consumer
import pytest

from swh.model.hashutil import MultiHash
from swh.provenance.tests.conftest import fill_storage, load_repo_data
from swh.storage.interface import StorageInterface

from .test_utils import invoke, write_configuration_path


@pytest.fixture
def swh_storage_backend_config(swh_storage_backend_config, kafka_server, kafka_prefix):
    writer_config = {
        "cls": "kafka",
        "brokers": [kafka_server],
        "client_id": "kafka_writer",
        "prefix": kafka_prefix,
        "anonymize": False,
    }
    yield {**swh_storage_backend_config, "journal_writer": writer_config}


@pytest.mark.origin_layer
def test_cli_origin_from_journal_client(
    swh_storage: StorageInterface,
    swh_storage_backend_config: Dict,
    kafka_prefix: str,
    kafka_server: str,
    consumer: Consumer,
    tmp_path: str,
    provenance,
    postgres_provenance,
) -> None:
    """Test origin journal client cli"""

    # Prepare storage data
    data = load_repo_data("cmdbts2")
    assert len(data["origin"]) >= 1
    origin_url = data["origin"][0]["url"]
    fill_storage(swh_storage, data)

    # Prepare configuration for cli call
    swh_storage_backend_config.pop("journal_writer", None)  # no need for that config
    storage_config_dict = swh_storage_backend_config
    cfg = {
        "journal_client": {
            "cls": "kafka",
            "brokers": [kafka_server],
            "group_id": "toto",
            "prefix": kafka_prefix,
            "stop_on_eof": True,
        },
        "provenance": {
            "archive": {
                "cls": "api",
                "storage": storage_config_dict,
            },
            "storage": {
                "cls": "postgresql",
                "db": postgres_provenance.get_dsn_parameters(),
            },
        },
    }
    config_path = write_configuration_path(cfg, tmp_path)

    # call the cli 'swh provenance origin from-journal'
    result = invoke(["origin", "from-journal"], config_path)
    assert result.exit_code == 0, f"Unexpected result: {result.output}"

    origin_sha1 = MultiHash.from_data(
        origin_url.encode(), hash_names=["sha1"]
    ).digest()["sha1"]
    actual_result = provenance.storage.origin_get([origin_sha1])

    assert actual_result == {origin_sha1: origin_url}


def test_cli_revision_from_journal_client(
    swh_storage: StorageInterface,
    swh_storage_backend_config: Dict,
    kafka_prefix: str,
    kafka_server: str,
    consumer: Consumer,
    tmp_path: str,
    provenance,
    postgres_provenance,
) -> None:
    """Test revision journal client cli"""

    # Prepare storage data
    data = load_repo_data("cmdbts2")
    assert len(data["origin"]) >= 1
    fill_storage(swh_storage, data)

    # Prepare configuration for cli call
    swh_storage_backend_config.pop("journal_writer", None)  # no need for that config
    storage_config_dict = swh_storage_backend_config
    cfg = {
        "journal_client": {
            "cls": "kafka",
            "brokers": [kafka_server],
            "group_id": "toto",
            "prefix": kafka_prefix,
            "stop_on_eof": True,
        },
        "provenance": {
            "archive": {
                "cls": "api",
                "storage": storage_config_dict,
            },
            "storage": {
                "cls": "postgresql",
                "db": postgres_provenance.get_dsn_parameters(),
            },
        },
    }
    config_path = write_configuration_path(cfg, tmp_path)

    revisions = [rev["id"] for rev in data["revision"]]
    result = provenance.storage.revision_get(revisions)
    assert not result

    # call the cli 'swh provenance revision from-journal'
    cli_result = invoke(["revision", "from-journal"], config_path)
    assert cli_result.exit_code == 0, f"Unexpected result: {result.output}"

    result = provenance.storage.revision_get(revisions)

    assert set(result.keys()) == set(revisions)
