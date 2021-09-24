# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta, timezone
from os import path
from typing import Any, Dict, Generator, Iterable

from _pytest.fixtures import SubRequest
import mongomock.database
import msgpack
import psycopg2.extensions
import pytest
from pytest_postgresql.factories import postgresql

from swh.journal.serializers import msgpack_ext_hook
from swh.provenance import get_provenance, get_provenance_storage
from swh.provenance.archive import ArchiveInterface
from swh.provenance.interface import ProvenanceInterface, ProvenanceStorageInterface
from swh.provenance.storage.archive import ArchiveStorage
from swh.storage.interface import StorageInterface
from swh.storage.replay import process_replay_objects


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
    from swh.core.cli.db import populate_database_for_package

    populate_database_for_package(
        "swh.provenance", postgresql.dsn, flavor=request.param
    )
    return postgresql.get_dsn_parameters()


@pytest.fixture(params=["mongodb", "postgresql"])
def provenance_storage(
    request: SubRequest,
    provenance_postgresqldb: Dict[str, str],
    mongodb: mongomock.database.Database,
) -> Generator[ProvenanceStorageInterface, None, None]:
    """Return a working and initialized ProvenanceStorageInterface object"""

    if request.param == "mongodb":
        mongodb_params = {
            "host": mongodb.client.address[0],
            "port": mongodb.client.address[1],
            "dbname": mongodb.name,
        }
        with get_provenance_storage(
            cls=request.param, db=mongodb_params, engine="mongomock"
        ) as storage:
            yield storage

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

    from swh.core.cli.db import populate_database_for_package

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


def get_datafile(fname: str) -> str:
    return path.join(path.dirname(__file__), "data", fname)


def load_repo_data(repo: str) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    with open(get_datafile(f"{repo}.msgpack"), "rb") as fobj:
        unpacker = msgpack.Unpacker(
            fobj,
            raw=False,
            ext_hook=msgpack_ext_hook,
            strict_map_key=False,
            timestamp=3,  # convert Timestamp in datetime objects (tz UTC)
        )
        for objtype, objd in unpacker:
            data.setdefault(objtype, []).append(objd)
    return data


def filter_dict(d: Dict[Any, Any], keys: Iterable[Any]) -> Dict[Any, Any]:
    return {k: v for (k, v) in d.items() if k in keys}


def fill_storage(storage: StorageInterface, data: Dict[str, Any]) -> None:
    process_replay_objects(data, storage=storage)


# TODO: remove this function in favour of TimestampWithTimezone.to_datetime
#       from swh.model.model
def ts2dt(ts: Dict[str, Any]) -> datetime:
    timestamp = datetime.fromtimestamp(
        ts["timestamp"]["seconds"], timezone(timedelta(minutes=ts["offset"]))
    )
    return timestamp.replace(microsecond=ts["timestamp"]["microseconds"])
