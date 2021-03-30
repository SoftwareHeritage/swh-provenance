# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.provenance.storage.archive import ArchiveStorage

from .test_provenance_db import (  # noqa
    test_provenance_add_revision,
    test_provenance_content_find_first,
    test_provenance_heuristics,
)


@pytest.fixture
def archive(swh_storage_with_objects):
    """Return a ArchiveStorage based StorageInterface object"""
    archive = ArchiveStorage(swh_storage_with_objects)
    yield archive
