# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from swh.model.tests.swh_model_data import TEST_OBJECTS
from swh.provenance.origin import OriginEntry
from swh.provenance.provenance import origin_add
from swh.provenance.storage.archive import ArchiveStorage


def ts2dt(ts: dict) -> datetime.datetime:
    timestamp = datetime.datetime.fromtimestamp(
        ts["timestamp"]["seconds"],
        datetime.timezone(datetime.timedelta(minutes=ts["offset"])),
    )
    return timestamp.replace(microsecond=ts["timestamp"]["microseconds"])


def test_provenance_origin_add(provenance, swh_storage_with_objects):
    """Test the ProvenanceDB.origin_add() method"""
    for origin in TEST_OBJECTS["origin"]:
        entry = OriginEntry(url=origin.url, revisions=[])
        origin_add(ArchiveStorage(swh_storage_with_objects), provenance, entry)
    # TODO: check some facts here
