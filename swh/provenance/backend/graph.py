# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model.swhids import CoreSWHID


class GraphProvenance:
    def __init__(self, graph):
        self.graph = graph

    def check_config(self) -> bool:
        return True

    def whereis(self, swhid: CoreSWHID):
        return swhid
