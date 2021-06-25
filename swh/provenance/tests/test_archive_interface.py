# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
from operator import itemgetter

import psycopg2
import pytest

from swh.provenance.postgresql.archive import ArchivePostgreSQL
from swh.provenance.postgresql.db_utils import adapt_conn
from swh.provenance.storage.archive import ArchiveStorage
from swh.provenance.tests.conftest import fill_storage, load_repo_data


@pytest.mark.parametrize(
    "repo",
    ("cmdbts2", "out-of-order", "with-merges"),
)
def test_archive_interface(repo, swh_storage):
    archive_api = ArchiveStorage(swh_storage)
    dsn = swh_storage.get_db().conn.dsn
    with psycopg2.connect(dsn) as conn:
        adapt_conn(conn)
        archive_direct = ArchivePostgreSQL(conn)
        # read data/README.md for more details on how these datasets are generated
        data = load_repo_data(repo)
        fill_storage(swh_storage, data)

        for directory in data["directory"]:
            entries_api = sorted(
                archive_api.directory_ls(directory["id"]), key=itemgetter("name")
            )
            entries_direct = sorted(
                archive_direct.directory_ls(directory["id"]), key=itemgetter("name")
            )
            assert entries_api == entries_direct

        for revision in data["revision"]:
            parents_api = Counter(archive_api.revision_get_parents(revision["id"]))
            parents_direct = Counter(
                archive_direct.revision_get_parents(revision["id"])
            )
            assert parents_api == parents_direct

        for snapshot in data["snapshot"]:
            heads_api = Counter(archive_api.snapshot_get_heads(snapshot["id"]))
            heads_direct = Counter(archive_direct.snapshot_get_heads(snapshot["id"]))
            assert heads_api == heads_direct
