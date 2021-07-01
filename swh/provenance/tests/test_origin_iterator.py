# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model.model import OriginVisitStatus
from swh.model.tests.swh_model_data import TEST_OBJECTS
from swh.provenance.origin import CSVOriginIterator
from swh.storage.algos.origin import (
    iter_origin_visit_statuses,
    iter_origin_visits,
    iter_origins,
)
from swh.storage.postgresql.storage import Storage


def test_origin_iterator(swh_storage_with_objects: Storage) -> None:
    """Test CSVOriginIterator"""
    origins_csv = []
    for origin in iter_origins(swh_storage_with_objects):
        for visit in iter_origin_visits(swh_storage_with_objects, origin.url):
            if visit.visit is not None:
                for status in iter_origin_visit_statuses(
                    swh_storage_with_objects, origin.url, visit.visit
                ):
                    if status.snapshot is not None:
                        origins_csv.append((status.origin, status.snapshot))
    origins = list(CSVOriginIterator(origins_csv))
    assert origins
    assert len(origins) == len(
        list(
            {
                status.origin
                for status in TEST_OBJECTS["origin_visit_status"]
                if isinstance(status, OriginVisitStatus) and status.snapshot is not None
            }
        )
    )
