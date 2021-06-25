# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from ..interface import RelationType
from .provenancedb_base import ProvenanceDBBase


class ProvenanceWithPathDB(ProvenanceDBBase):
    def _relation_uses_location_table(self, relation: RelationType) -> bool:
        src, *_ = relation.value.split("_")
        return src in ("content", "directory")
