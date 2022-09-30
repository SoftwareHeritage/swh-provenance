# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.provenance.directory import CSVDirectoryIterator
from swh.provenance.tests.conftest import fill_storage, load_repo_data
from swh.storage.interface import StorageInterface


@pytest.mark.parametrize(
    "repo",
    (
        "cmdbts2",
        "out-of-order",
    ),
)
def test_revision_iterator(swh_storage: StorageInterface, repo: str) -> None:
    """Test CSVDirectoryIterator"""
    data = load_repo_data(repo)
    fill_storage(swh_storage, data)

    directories_ids = [dir["id"] for dir in data["directory"]]
    directories = list(CSVDirectoryIterator(directories_ids))

    assert directories
    assert len(directories) == len(data["directory"])
