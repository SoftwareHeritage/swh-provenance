# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial

from pytest_postgresql import factories

from swh.core.db.db_utils import initialize_database_for_module
from swh.provenance.storage.postgresql import ProvenanceStoragePostgreSql

from .test_provenance_storage import TestProvenanceStorage  # noqa: F401

provenance_postgresql_proc = factories.postgresql_proc(
    load=[
        partial(
            initialize_database_for_module,
            modname="provenance",
            flavor="without-path",
            version=ProvenanceStoragePostgreSql.current_version,
        )
    ],
)
