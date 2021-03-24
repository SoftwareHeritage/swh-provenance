# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.provenance.revision import CSVRevisionIterator
from swh.provenance.tests.test_provenance_db import ts2dt


def test_archive_direct_revision_iterator(storage_and_CMDBTS, archive_direct):
    """Test CSVRevisionIterator"""
    storage, data = storage_and_CMDBTS
    revisions_csv = [
        (rev["id"], ts2dt(rev["date"]).isoformat(), rev["directory"])
        for rev in data["revision"]
    ]
    revisions = list(CSVRevisionIterator(revisions_csv, archive_direct))
    assert revisions
    assert len(revisions) == len(data["revision"])
