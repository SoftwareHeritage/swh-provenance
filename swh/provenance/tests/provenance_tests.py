# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model.swhids import CoreSWHID


class TestProvenance:
    def test_where_is(self, swh_provenance):
        swhid = CoreSWHID.from_string(
            "swh:1:cnt:8ff44f081d43176474b267de5451f2c2e88089d0"
        )
        assert swh_provenance.whereis(swhid) == swhid
