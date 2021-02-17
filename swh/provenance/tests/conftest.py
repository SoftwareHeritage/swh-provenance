# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob
from os import path

import pytest

from swh.core.db.pytest_plugin import postgresql_fact
from swh.core.utils import numfile_sortkey as sortkey
from swh.model.tests.swh_model_data import TEST_OBJECTS
import swh.provenance
from swh.provenance.postgresql.archive import ArchivePostgreSQL
from swh.provenance.storage.archive import ArchiveStorage

SQL_DIR = path.join(path.dirname(swh.provenance.__file__), "sql")
SQL_FILES = [
    sqlfile
    for sqlfile in sorted(glob.glob(path.join(SQL_DIR, "*.sql")), key=sortkey)
    if "-without-path-" not in sqlfile
]

provenance_db = postgresql_fact(
    "postgresql_proc", db_name="provenance", dump_files=SQL_FILES
)


@pytest.fixture
def provenance(provenance_db):
    """return a working and initialized provenance db"""
    from swh.provenance.postgresql.provenancedb_with_path import (
        ProvenanceWithPathDB as ProvenanceDB,
    )

    return ProvenanceDB(provenance_db)


@pytest.fixture
def swh_storage_with_objects(swh_storage):
    """return a Storage object (postgresql-based by default) with a few of each
    object type in it

    The inserted content comes from swh.model.tests.swh_model_data.
    """
    for obj_type in (
        "content",
        "skipped_content",
        "directory",
        "revision",
        "release",
        "snapshot",
        "origin",
        "origin_visit",
        "origin_visit_status",
    ):
        getattr(swh_storage, f"{obj_type}_add")(TEST_OBJECTS[obj_type])
    return swh_storage


@pytest.fixture
def archive_direct(swh_storage_with_objects):
    return ArchivePostgreSQL(swh_storage_with_objects.get_db().conn)


@pytest.fixture
def archive_api(swh_storage_with_objects):
    return ArchiveStorage(swh_storage_with_objects)
