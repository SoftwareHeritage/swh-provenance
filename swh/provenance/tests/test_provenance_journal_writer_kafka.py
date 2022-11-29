# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Generator

from confluent_kafka import Consumer
import pytest

from swh.provenance import get_provenance_storage
from swh.provenance.storage.interface import ProvenanceStorageInterface

from .test_provenance_storage import (  # noqa
    TestProvenanceStorage as _TestProvenanceStorage,
)


@pytest.fixture()
def provenance_storage(
    provenance_postgresqldb: str,
    kafka_prefix: str,
    kafka_server: str,
    consumer: Consumer,
) -> Generator[ProvenanceStorageInterface, None, None]:
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
            "prefix": "swh.provenance",
            "anonymize": False,
        },
    }
    with get_provenance_storage(cls="journal", **cfg) as storage:
        yield storage


@pytest.mark.kafka
class TestProvenanceStorageJournal(_TestProvenanceStorage):
    pass
