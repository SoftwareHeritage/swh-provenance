# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import pytest

from swh.model.tests.swh_model_data import TEST_OBJECTS
from swh.provenance.origin import ArchiveOriginIterator


def test_archive_direct_origin_iterator(swh_storage_with_objects, archive_direct):
    """Test ArchiveOriginIterator against the ArchivePostgreSQL"""
    # XXX
    pytest.xfail("Iterate Origins is currently unsupported by ArchivePostgreSQL")
    origins = list(ArchiveOriginIterator(archive_direct))
    assert origins
    assert len(origins) == len(TEST_OBJECTS["origin"])


def test_archive_api_origin_iterator(swh_storage_with_objects, archive_api):
    """Test ArchiveOriginIterator against the ArchiveStorage"""
    origins = list(ArchiveOriginIterator(archive_api))
    assert origins
    assert len(origins) == len(TEST_OBJECTS["origin"])
