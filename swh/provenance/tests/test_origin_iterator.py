# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.provenance.origin import CSVOriginIterator
from swh.provenance.tests.conftest import fill_storage, load_repo_data
from swh.storage.algos.origin import (
    iter_origin_visit_statuses,
    iter_origin_visits,
    iter_origins,
)
from swh.storage.interface import StorageInterface


@pytest.mark.parametrize(
    "repo",
    (
        "cmdbts2",
        "out-of-order",
    ),
)
def test_origin_iterator(swh_storage: StorageInterface, repo: str) -> None:
    """Test CSVOriginIterator"""
    data = load_repo_data(repo)
    fill_storage(swh_storage, data)

    origins_csv = []
    for origin in iter_origins(swh_storage):
        for visit in iter_origin_visits(swh_storage, origin.url):
            if visit.visit is not None:
                for status in iter_origin_visit_statuses(
                    swh_storage, origin.url, visit.visit
                ):
                    if status.snapshot is not None:
                        origins_csv.append((status.origin, status.snapshot))
    origins = list(CSVOriginIterator(origins_csv))

    assert origins
    # there can be more origins, depending on the additional extra visits.yaml
    # file used during dataset generation (see data/generate_storage_from_git)
    assert len(origins) >= len(data["origin"])
    # but we can check it's a subset
    assert set(o.url for o in origins) <= set(o["url"] for o in data["origin"])
