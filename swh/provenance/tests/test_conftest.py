# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.provenance.interface import ProvenanceInterface
from swh.provenance.tests.conftest import fill_storage, load_repo_data
from swh.storage.interface import StorageInterface


def test_provenance_fixture(provenance: ProvenanceInterface) -> None:
    """Check the 'provenance' fixture produce a working ProvenanceDB object"""
    assert provenance
    provenance.flush()  # should be a noop


def test_fill_storage(swh_storage: StorageInterface) -> None:
    """Check the 'fill_storage' test utility produces a working Storage
    object with at least some Content, Revision and Directory in it"""
    data = load_repo_data("cmdbts2")
    fill_storage(swh_storage, data)

    assert swh_storage
    assert swh_storage.content_get_random()
    assert swh_storage.directory_get_random()
    assert swh_storage.revision_get_random()
