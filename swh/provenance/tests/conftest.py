# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
from os import path
from typing import Any, Dict, Generator, List

from _pytest.fixtures import SubRequest
import msgpack
import psycopg2.extensions
import pytest
from pytest_postgresql.factories import postgresql

from swh.journal.serializers import msgpack_ext_hook
from swh.model.model import BaseModel, TimestampWithTimezone
from swh.provenance import get_provenance, get_provenance_storage
from swh.provenance.archive import ArchiveInterface
from swh.provenance.interface import ProvenanceInterface, ProvenanceStorageInterface
from swh.provenance.storage.archive import ArchiveStorage
from swh.storage.interface import StorageInterface
from swh.storage.replay import OBJECT_CONVERTERS, OBJECT_FIXERS, process_replay_objects


@pytest.fixture(
    params=[
        "with-path",
        "without-path",
        "with-path-denormalized",
        "without-path-denormalized",
    ]
)
def provenance_postgresqldb(
    request: SubRequest,
    postgresql: psycopg2.extensions.connection,
) -> Dict[str, str]:
    """return a working and initialized provenance db"""
    from swh.core.db.db_utils import (
        init_admin_extensions,
        populate_database_for_package,
    )

    init_admin_extensions("swh.provenance", postgresql.dsn)
    populate_database_for_package(
        "swh.provenance", postgresql.dsn, flavor=request.param
    )
    return postgresql.get_dsn_parameters()


@pytest.fixture(params=["postgresql", "rabbitmq"])
def provenance_storage(
    request: SubRequest,
    provenance_postgresqldb: Dict[str, str],
) -> Generator[ProvenanceStorageInterface, None, None]:
    """Return a working and initialized ProvenanceStorageInterface object"""

    if request.param == "rabbitmq":
        from swh.provenance.api.server import ProvenanceStorageRabbitMQServer

        rabbitmq = request.getfixturevalue("rabbitmq")
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
        with get_provenance_storage(cls=request.param, **rabbitmq_params) as storage:
            yield storage
        server.stop()

    else:
        # in test sessions, we DO want to raise any exception occurring at commit time
        with get_provenance_storage(
            cls=request.param, db=provenance_postgresqldb, raise_on_commit=True
        ) as storage:
            yield storage


provenance_postgresql = postgresql("postgresql_proc", dbname="provenance_tests")


@pytest.fixture
def provenance(
    provenance_postgresql: psycopg2.extensions.connection,
) -> Generator[ProvenanceInterface, None, None]:
    """Return a working and initialized ProvenanceInterface object"""

    from swh.core.db.db_utils import (
        init_admin_extensions,
        populate_database_for_package,
    )

    init_admin_extensions("swh.provenance", provenance_postgresql.dsn)
    populate_database_for_package(
        "swh.provenance", provenance_postgresql.dsn, flavor="with-path"
    )
    # in test sessions, we DO want to raise any exception occurring at commit time
    with get_provenance(
        cls="postgresql",
        db=provenance_postgresql.get_dsn_parameters(),
        raise_on_commit=True,
    ) as provenance:
        yield provenance


@pytest.fixture
def archive(swh_storage: StorageInterface) -> ArchiveInterface:
    """Return an ArchiveStorage-based ArchiveInterface object"""
    return ArchiveStorage(swh_storage)


def fill_storage(storage: StorageInterface, data: Dict[str, List[dict]]) -> None:
    objects = {
        objtype: [objs_from_dict(objtype, d) for d in dicts]
        for objtype, dicts in data.items()
    }
    process_replay_objects(objects, storage=storage)


def get_datafile(fname: str) -> str:
    return path.join(path.dirname(__file__), "data", fname)


# TODO: this should return Dict[str, List[BaseModel]] directly, but it requires
#       refactoring several tests
def load_repo_data(repo: str) -> Dict[str, List[dict]]:
    data: Dict[str, List[dict]] = {}
    with open(get_datafile(f"{repo}.msgpack"), "rb") as fobj:
        unpacker = msgpack.Unpacker(
            fobj,
            raw=False,
            ext_hook=msgpack_ext_hook,
            strict_map_key=False,
            timestamp=3,  # convert Timestamp in datetime objects (tz UTC)
        )
        for msg in unpacker:
            if len(msg) == 2:  # old format
                objtype, objd = msg
            else:  # now we should have a triplet (type, key, value)
                objtype, _, objd = msg
            data.setdefault(objtype, []).append(objd)
    return data


def objs_from_dict(object_type: str, dict_repr: dict) -> BaseModel:
    if object_type in OBJECT_FIXERS:
        dict_repr = OBJECT_FIXERS[object_type](dict_repr)
    obj = OBJECT_CONVERTERS[object_type](dict_repr)
    return obj


def ts2dt(ts: Dict[str, Any]) -> datetime:
    return TimestampWithTimezone.from_dict(ts).to_datetime()
