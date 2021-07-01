# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.provenance.revision import CSVRevisionIterator
from swh.provenance.tests.conftest import fill_storage, load_repo_data
from swh.provenance.tests.test_provenance_db import ts2dt
from swh.storage.postgresql.storage import Storage


@pytest.mark.parametrize(
    "repo",
    (
        "cmdbts2",
        "out-of-order",
    ),
)
def test_archive_direct_revision_iterator(swh_storage: Storage, repo: str) -> None:
    """Test CSVRevisionIterator"""
    data = load_repo_data(repo)
    fill_storage(swh_storage, data)
    revisions_csv = [
        (rev["id"], ts2dt(rev["date"]), rev["directory"]) for rev in data["revision"]
    ]
    revisions = list(CSVRevisionIterator(revisions_csv))
    assert revisions
    assert len(revisions) == len(data["revision"])
