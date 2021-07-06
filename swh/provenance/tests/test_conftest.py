# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.provenance.interface import ProvenanceInterface
from swh.storage.postgresql.storage import Storage


def test_provenance_fixture(provenance: ProvenanceInterface) -> None:
    """Check the 'provenance' fixture produce a working ProvenanceDB object"""
    assert provenance
    provenance.flush()  # should be a noop


def test_storage(swh_storage_with_objects: Storage) -> None:
    """Check the 'swh_storage_with_objects' fixture produce a working Storage
    object with at least some Content, Revision and Directory in it"""
    assert swh_storage_with_objects
    assert swh_storage_with_objects.content_get_random()
    assert swh_storage_with_objects.directory_get_random()
    assert swh_storage_with_objects.revision_get_random()
