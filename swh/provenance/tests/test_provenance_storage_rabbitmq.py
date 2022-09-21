from typing import Any, Dict, Generator

import pytest

from swh.provenance import get_provenance_storage
from swh.provenance.interface import ProvenanceStorageInterface

from .test_provenance_storage import TestProvenanceStorage  # noqa: F401


@pytest.fixture()
def provenance_storage(
    provenance_postgresqldb: Dict[str, str],
    rabbitmq,
) -> Generator[ProvenanceStorageInterface, None, None]:
    """Return a working and initialized ProvenanceStorageInterface object"""

    from swh.provenance.api.server import ProvenanceStorageRabbitMQServer

    host = rabbitmq.args["host"]
    port = rabbitmq.args["port"]
    rabbitmq_params: Dict[str, Any] = {
        "url": f"amqp://guest:guest@{host}:{port}/%2f",
        "storage_config": {
            "cls": "postgresql",
            "db": provenance_postgresqldb,
            "raise_on_commit": True,
        },
    }
    server = ProvenanceStorageRabbitMQServer(
        url=rabbitmq_params["url"], storage_config=rabbitmq_params["storage_config"]
    )
    server.start()
    try:
        with get_provenance_storage(cls="rabbitmq", **rabbitmq_params) as storage:
            yield storage
    finally:
        server.stop()
