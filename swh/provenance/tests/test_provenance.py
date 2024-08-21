# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from functools import partial
import os
from typing import Any, Dict

import pytest
from pytest_postgresql import factories
from swh.core.db.db_utils import initialize_database_for_module
from swh.model.swhids import CoreSWHID

from swh.provenance.backend.postgresql import PostgresqlProvenance

os.environ["LC_ALL"] = "C.UTF-8"


provenance_postgresql_proc = factories.postgresql_proc(
    load=[
        partial(
            initialize_database_for_module,
            "provenance",
            PostgresqlProvenance.current_version,
        )
    ],
)

postgres_provenance = factories.postgresql("provenance_postgresql_proc")


@pytest.fixture
def swh_provenance_config(postgres_provenance) -> Dict[str, Any]:
    return {
        "cls": "postgresql",
        "db": postgres_provenance.info.dsn,
    }


class TestProvenance:
    def test_where_is(self, swh_provenance):
        swhid = CoreSWHID.from_string(
            "swh:1:cnt:8ff44f081d43176474b267de5451f2c2e88089d0"
        )
        assert swh_provenance.whereis(swhid) == swhid
