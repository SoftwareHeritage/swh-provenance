# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools

import psycopg2
from psycopg2.extensions import parse_dsn
import pytest

from swh.journal.client import JournalClient
from swh.provenance.algos.revision import revision_add
from swh.provenance.archive.interface import ArchiveInterface
from swh.provenance.model import RevisionEntry
from swh.provenance.provenance import Provenance
from swh.provenance.storage import get_provenance_storage
from swh.provenance.storage.interface import (
    EntityType,
    ProvenanceStorageInterface,
    RelationType,
)
from swh.provenance.storage.replay import (
    ProvenanceObjectDeserializer,
    process_replay_objects,
)

from .utils import fill_storage, load_repo_data, ts2dt


@pytest.fixture(scope="function")
def object_types():
    """Set of object types to precreate topics for."""
    return {
        # objects
        "revision",
        "directory",
        "content",
        "location",
        # relations
        "content_in_revision",
        "content_in_directory",
        "directory_in_revision",
    }


@pytest.fixture()
def replayer_storage_and_client(
    provenance_postgresqldb: str,
    kafka_prefix: str,
    kafka_consumer_group: str,
    kafka_server: str,
):
    cfg = {
        "storage": {
            "cls": "postgresql",
            "db": provenance_postgresqldb,
            "raise_on_commit": True,
        },
        "journal_writer": {
            "cls": "kafka",
            "brokers": [kafka_server],
            "client_id": "kafka_writer",
            "prefix": kafka_prefix,
            "anonymize": False,
            "auto_flush": False,
        },
    }
    with get_provenance_storage(cls="journal", **cfg) as storage:
        deserializer = ProvenanceObjectDeserializer()
        replayer = JournalClient(
            brokers=kafka_server,
            group_id=kafka_consumer_group,
            prefix=kafka_prefix,
            stop_on_eof=True,
            value_deserializer=deserializer.convert,
        )

        yield storage, replayer


@pytest.fixture()
def secondary_db(provenance_postgresqldb: str):
    """Create a new test db

    the new db is named after the db configured in provenance_postgresqldb and
    is using the same template as this later.
    """
    dsn = parse_dsn(provenance_postgresqldb)

    conn = psycopg2.connect(
        dbname="postgres",
        user=dsn["user"],
        password=dsn.get("password"),
        host=dsn["host"],
        port=dsn["port"],
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        dbname = dsn["dbname"]
        template_name = f"{dbname}_tmpl"
        secondary_dbname = f"{dbname}_dst"
        cur.execute(f'CREATE DATABASE "{secondary_dbname}" TEMPLATE "{template_name}"')
    try:
        dsn["dbname"] = secondary_dbname
        yield " ".join(f"{k}={v}" for k, v in dsn.items())
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE "{secondary_dbname}"')


@pytest.mark.kafka
@pytest.mark.parametrize(
    "repo",
    (
        "cmdbts2",
        "out-of-order",
        "with-merges",
    ),
)
def test_provenance_replayer(
    provenance_storage: ProvenanceStorageInterface,
    archive: ArchiveInterface,
    replayer_storage_and_client,
    secondary_db: str,
    repo: str,
):
    """Optimal replayer scenario.

    This:
    - writes objects to a provenance storage (which have a journal writer)
    - replayer consumes objects from the topic and replays them
    - a destination provenance storage is filled from this

    In the end, both storages should have the same content.
    """
    # load test data and fill a swh-storage
    data = load_repo_data(repo)
    fill_storage(archive.storage, data)

    prov_sto_src, replayer = replayer_storage_and_client

    # Fill Kafka by filling the source provenance storage
    revisions = [
        RevisionEntry(
            id=revision["id"],
            date=ts2dt(revision["date"]),
            root=revision["directory"],
        )
        for revision in data["revision"]
    ]

    revision_add(Provenance(prov_sto_src), archive, revisions)

    # now replay the kafka log in a new provenance storage
    with get_provenance_storage(
        cls="postgresql", db=secondary_db, raise_on_commit=True
    ) as prov_sto_dst:
        worker_fn = functools.partial(process_replay_objects, storage=prov_sto_dst)
        replayer.process(worker_fn)

        compare_provenance_storages(prov_sto_src, prov_sto_dst)


def compare_provenance_storages(sto1, sto2):
    entities1 = {etype: sto1.entity_get_all(etype) for etype in EntityType}
    entities2 = {etype: sto2.entity_get_all(etype) for etype in EntityType}
    assert entities1 == entities2

    relations1 = {rtype: sto1.relation_get_all(rtype) for rtype in RelationType}
    relations2 = {rtype: sto2.relation_get_all(rtype) for rtype in RelationType}
    assert relations1 == relations2
