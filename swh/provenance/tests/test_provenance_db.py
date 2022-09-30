# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.provenance.interface import ProvenanceInterface
from swh.provenance.storage.postgresql import ProvenanceStoragePostgreSql


def test_provenance_flavor(provenance: ProvenanceInterface) -> None:
    if isinstance(provenance.storage, ProvenanceStoragePostgreSql):
        assert provenance.storage.flavor in (
            "with-path",
            "without-path",
            "with-path-denormalized",
            "without-path-denormalized",
        )
