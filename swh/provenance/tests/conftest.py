# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial
from typing import Generator

from _pytest.fixtures import SubRequest
import psycopg2.extensions
import pytest
from pytest_postgresql import factories

from swh.core.db.db_utils import initialize_database_for_module
from swh.provenance import get_provenance
from swh.provenance.archive.interface import ArchiveInterface
from swh.provenance.archive.storage import ArchiveStorage
from swh.provenance.interface import ProvenanceInterface
from swh.provenance.storage import get_provenance_storage
from swh.provenance.storage.interface import ProvenanceStorageInterface
from swh.provenance.storage.postgresql import ProvenanceStoragePostgreSql
from swh.storage.interface import StorageInterface

provenance_postgresql_proc = factories.postgresql_proc(
    load=[
        partial(
            initialize_database_for_module,
            modname="provenance",
            version=ProvenanceStoragePostgreSql.current_version,
        )
    ],
)

postgres_provenance = factories.postgresql("provenance_postgresql_proc")


@pytest.fixture()
def provenance_postgresqldb(request, postgres_provenance) -> str:
    return postgres_provenance.dsn


@pytest.fixture()
def provenance_storage(
    request: SubRequest,
    provenance_postgresqldb: str,
) -> Generator[ProvenanceStorageInterface, None, None]:
    """Return a working and initialized ProvenanceStorageInterface object"""

    # in test sessions, we DO want to raise any exception occurring at commit time
    with get_provenance_storage(
        cls="postgresql", db=provenance_postgresqldb, raise_on_commit=True
    ) as storage:
        yield storage


@pytest.fixture
def provenance(
    postgres_provenance: psycopg2.extensions.connection,
) -> Generator[ProvenanceInterface, None, None]:
    """Return a working and initialized ProvenanceInterface object"""

    from swh.core.db.db_utils import (
        init_admin_extensions,
        populate_database_for_package,
    )

    init_admin_extensions("swh.provenance", postgres_provenance.dsn)
    populate_database_for_package(
        "swh.provenance",
        postgres_provenance.dsn,
    )
    # in test sessions, we DO want to raise any exception occurring at commit time
    with get_provenance(
        cls="postgresql",
        db=postgres_provenance.dsn,
        raise_on_commit=True,
    ) as provenance:
        yield provenance


@pytest.fixture
def archive(swh_storage: StorageInterface) -> ArchiveInterface:
    """Return an ArchiveStorage-based ArchiveInterface object"""
    return ArchiveStorage(swh_storage)
