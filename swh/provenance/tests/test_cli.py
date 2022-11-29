# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone
import logging
import re
from typing import Dict, List

from click.testing import CliRunner
from confluent_kafka import Producer
import psycopg2.extensions
import pytest

from swh.core.cli import swh as swhmain
import swh.core.cli.db  # noqa ; ensure cli is loaded
from swh.core.db.db_utils import init_admin_extensions
from swh.journal.serializers import key_to_kafka, value_to_kafka
from swh.model.hashutil import MultiHash
import swh.provenance.cli  # noqa ; ensure cli is loaded
from swh.provenance.storage.interface import EntityType, RelationType
from swh.storage.interface import StorageInterface

from .utils import fill_storage, get_datafile, invoke, load_repo_data

logger = logging.getLogger(__name__)


def test_cli_swh_db_help() -> None:
    # swhmain.add_command(provenance_cli)
    result = CliRunner().invoke(swhmain, ["provenance", "-h"])
    assert result.exit_code == 0
    assert "Commands:" in result.output
    commands = result.output.split("Commands:")[1]
    for command in (
        "find-all",
        "find-first",
        "iter-frontiers",
        "iter-origins",
        "iter-revisions",
    ):
        assert f"  {command} " in commands


def test_cli_init_db_default_flavor(postgresql: psycopg2.extensions.connection) -> None:
    "Test that 'swh db init provenance' defaults to a normalized flavored DB"

    dbname = postgresql.dsn
    init_admin_extensions("swh.provenance", dbname)
    result = CliRunner().invoke(swhmain, ["db", "init", "-d", dbname, "provenance"])
    assert result.exit_code == 0, result.output


@pytest.mark.origin_layer
@pytest.mark.parametrize(
    "subcommand",
    (["origin", "from-csv"], ["iter-origins"]),
)
def test_cli_origin_from_csv(
    swh_storage: StorageInterface,
    subcommand: List[str],
    swh_storage_backend_config: Dict,
    provenance,
    tmp_path,
):
    repo = "cmdbts2"
    origin_url = f"https://{repo}"
    data = load_repo_data(repo)
    fill_storage(swh_storage, data)

    assert len(data["origin"]) >= 1
    assert origin_url in [o["url"] for o in data["origin"]]

    cfg = {
        "provenance": {
            "archive": {
                "cls": "api",
                "storage": swh_storage_backend_config,
            },
            "storage": {
                "cls": "postgresql",
                "db": provenance.storage.conn.dsn,
            },
        },
    }

    csv_filepath = get_datafile("origins.csv")
    subcommand = subcommand + [csv_filepath]

    result = invoke(subcommand, config=cfg)
    assert result.exit_code == 0, f"Unexpected result: {result.output}"

    origin_sha1 = MultiHash.from_data(
        origin_url.encode(), hash_names=["sha1"]
    ).digest()["sha1"]
    actual_result = provenance.storage.origin_get([origin_sha1])

    assert actual_result == {origin_sha1: origin_url}


@pytest.mark.kafka
def test_replay(
    provenance_storage,
    provenance_postgresqldb: str,
    kafka_prefix: str,
    kafka_consumer_group: str,
    kafka_server: str,
):
    kafka_prefix += ".swh.journal.provenance"

    producer = Producer(
        {
            "bootstrap.servers": kafka_server,
            "client.id": "test-producer",
            "acks": "all",
        }
    )

    for i in range(10):
        date = datetime.fromtimestamp(i, tz=timezone.utc)
        cntkey = (b"cnt:" + bytes([i])).ljust(20, b"\x00")
        dirkey = (b"dir:" + bytes([i])).ljust(20, b"\x00")
        revkey = (b"rev:" + bytes([i])).ljust(20, b"\x00")

        loc = f"dir/{i}".encode()

        producer.produce(
            topic=kafka_prefix + ".content_in_revision",
            key=key_to_kafka(cntkey),
            value=value_to_kafka({"src": cntkey, "dst": revkey, "path": loc}),
        )
        producer.produce(
            topic=kafka_prefix + ".content_in_directory",
            key=key_to_kafka(cntkey),
            value=value_to_kafka({"src": cntkey, "dst": dirkey, "path": loc}),
        )
        producer.produce(
            topic=kafka_prefix + ".directory_in_revision",
            key=key_to_kafka(dirkey),
            value=value_to_kafka({"src": dirkey, "dst": revkey, "path": loc}),
        )

        # now add dates to entities
        producer.produce(
            topic=kafka_prefix + ".content",
            key=key_to_kafka(cntkey),
            value=value_to_kafka({"id": cntkey, "value": date}),
        )
        producer.produce(
            topic=kafka_prefix + ".directory",
            key=key_to_kafka(dirkey),
            value=value_to_kafka({"id": dirkey, "value": date}),
        )
        producer.produce(
            topic=kafka_prefix + ".revision",
            key=key_to_kafka(revkey),
            value=value_to_kafka({"id": revkey, "value": date}),
        )

    producer.flush()
    logger.debug("Flushed producer")
    config = {
        "provenance": {
            "storage": {
                "cls": "postgresql",
                "db": provenance_postgresqldb,
            },
            "journal_client": {
                "cls": "kafka",
                "brokers": [kafka_server],
                "group_id": kafka_consumer_group,
                "prefix": kafka_prefix,
                "stop_on_eof": True,
            },
        }
    }

    result = invoke(["replay"], config=config)
    expected = r"Done. processed 60 messages\n"

    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    assert len(provenance_storage.entity_get_all(EntityType.CONTENT)) == 10
    assert len(provenance_storage.entity_get_all(EntityType.REVISION)) == 10
    assert len(provenance_storage.entity_get_all(EntityType.DIRECTORY)) == 10
    assert len(provenance_storage.location_get_all()) == 10
    assert len(provenance_storage.relation_get_all(RelationType.CNT_EARLY_IN_REV)) == 10
    assert len(provenance_storage.relation_get_all(RelationType.DIR_IN_REV)) == 10
    assert len(provenance_storage.relation_get_all(RelationType.CNT_IN_DIR)) == 10
