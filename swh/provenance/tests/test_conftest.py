# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def test_provenance_fixture(provenance):
    """Check the 'provenance' fixture produce a working ProvenanceDB object"""
    assert provenance
    provenance.commit()  # should be a noop


def test_storage(swh_storage_with_objects):
    """Check the 'swh_storage_with_objects' fixture produce a working Storage
    object with at least some Content, Revision and Directory in it"""
    assert swh_storage_with_objects
    assert swh_storage_with_objects.content_get_random()
    assert swh_storage_with_objects.directory_get_random()
    assert swh_storage_with_objects.revision_get_random()
