# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta, timezone

from swh.model.model import OriginVisitStatus
from swh.model.tests.swh_model_data import TEST_OBJECTS
from swh.provenance.interface import ProvenanceInterface
from swh.provenance.model import OriginEntry
from swh.provenance.origin import origin_add
from swh.provenance.postgresql.provenancedb import ProvenanceDB
from swh.provenance.storage.archive import ArchiveStorage
from swh.storage.postgresql.storage import Storage


# TODO: remove this function in favour of TimestampWithTimezone.to_datetime
#       from swh.model.model
def ts2dt(ts: dict) -> datetime:
    timestamp = datetime.fromtimestamp(
        ts["timestamp"]["seconds"], timezone(timedelta(minutes=ts["offset"]))
    )
    return timestamp.replace(microsecond=ts["timestamp"]["microseconds"])


def test_provenance_origin_add(
    provenance: ProvenanceInterface, swh_storage_with_objects: Storage
) -> None:
    """Test the origin_add function"""
    archive = ArchiveStorage(swh_storage_with_objects)
    for status in TEST_OBJECTS["origin_visit_status"]:
        assert isinstance(status, OriginVisitStatus)
        if status.snapshot is not None:
            entry = OriginEntry(url=status.origin, snapshot=status.snapshot)
            origin_add(provenance, archive, [entry])
    # TODO: check some facts here


def test_provenance_flavor(provenance: ProvenanceInterface) -> None:
    if isinstance(provenance.storage, ProvenanceDB):
        assert provenance.storage.flavor in (
            "with-path",
            "without-path",
            "with-path-denormalized",
            "without-path-denormalized",
        )
