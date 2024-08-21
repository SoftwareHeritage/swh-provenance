# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Optional

from swh.core.api import remote_api_endpoint
from swh.model.swhids import CoreSWHID, ExtendedSWHID
from typing_extensions import Protocol, runtime_checkable


@runtime_checkable
class ProvenanceInterface(Protocol):
    @remote_api_endpoint("check_config")
    def check_config(self) -> bool:
        """Check that the storage is configured and ready to go."""
        ...

    @remote_api_endpoint("whereis")
    def whereis(self, *, swhid: CoreSWHID) -> Optional[ExtendedSWHID]:
        """Looks for the first occurrence of the given SHWID."""
        ...
