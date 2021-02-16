# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def test_provenance_fixture(provenance):
    assert provenance
    provenance.insert_all()  # should be a noop
