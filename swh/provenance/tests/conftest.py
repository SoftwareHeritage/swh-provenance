# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob
from os import path

import pytest

from swh.core.db.pytest_plugin import postgresql_fact
from swh.core.utils import numfile_sortkey as sortkey
import swh.provenance

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
