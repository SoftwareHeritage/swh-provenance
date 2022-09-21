from functools import partial

from pytest_postgresql import factories

from swh.core.db.db_utils import initialize_database_for_module
from swh.provenance.postgresql.provenance import ProvenanceStoragePostgreSql

from .test_provenance_storage import TestProvenanceStorage  # noqa: F401

provenance_postgresql_proc = factories.postgresql_proc(
    load=[
        partial(
            initialize_database_for_module,
            modname="provenance",
            flavor="without-path-denormalized",
            version=ProvenanceStoragePostgreSql.current_version,
        )
    ],
)
