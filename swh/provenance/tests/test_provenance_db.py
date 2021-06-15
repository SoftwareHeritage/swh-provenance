# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from swh.model.tests.swh_model_data import TEST_OBJECTS
from swh.provenance.model import OriginEntry
from swh.provenance.origin import origin_add
from swh.provenance.postgresql.provenancedb_with_path import ProvenanceWithPathDB
from swh.provenance.postgresql.provenancedb_without_path import ProvenanceWithoutPathDB
from swh.provenance.storage.archive import ArchiveStorage


def ts2dt(ts: dict) -> datetime.datetime:
    timestamp = datetime.datetime.fromtimestamp(
        ts["timestamp"]["seconds"],
        datetime.timezone(datetime.timedelta(minutes=ts["offset"])),
    )
    return timestamp.replace(microsecond=ts["timestamp"]["microseconds"])


def test_provenance_origin_add(provenance, swh_storage_with_objects):
    """Test the origin_add function"""
    archive = ArchiveStorage(swh_storage_with_objects)
    for status in TEST_OBJECTS["origin_visit_status"]:
        if status.snapshot is not None:
            entry = OriginEntry(
                url=status.origin, date=status.date, snapshot=status.snapshot
            )
            origin_add(provenance, archive, [entry])
    # TODO: check some facts here


def test_provenance_flavor(provenance):
    assert provenance.storage.flavor in ("with-path", "without-path")
    if provenance.storage.flavor == "with-path":
        backend_class = ProvenanceWithPathDB
    else:
        backend_class = ProvenanceWithoutPathDB
    assert isinstance(provenance.storage, backend_class)
