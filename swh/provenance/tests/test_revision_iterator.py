# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import datetime

from swh.model.model import TimestampWithTimezone
from swh.model.tests.swh_model_data import TEST_OBJECTS
from swh.provenance.revision import CSVRevisionIterator


def ts_to_dt(ts_with_tz: TimestampWithTimezone) -> datetime.datetime:
    """converts a TimestampWithTimezone into a datetime"""
    ts = ts_with_tz.timestamp
    timestamp = datetime.datetime.fromtimestamp(ts.seconds, datetime.timezone.utc)
    timestamp = timestamp.replace(microsecond=ts.microseconds)
    return timestamp


def test_archive_direct_revision_iterator(swh_storage_with_objects, archive_direct):
    """Test FileOriginIterator"""
    revisions_csv = [
        (rev.id, ts_to_dt(rev.date).isoformat(), rev.directory)
        for rev in TEST_OBJECTS["revision"]
    ]
    revisions = list(CSVRevisionIterator(revisions_csv, archive_direct))
    assert revisions
    assert len(revisions) == len(TEST_OBJECTS["revision"])
