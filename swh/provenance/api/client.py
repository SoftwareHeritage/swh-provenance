# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core.api import RPCClient

from swh.provenance import exc
from swh.provenance.interface import ProvenanceInterface


class RemoteProvenance(RPCClient):
    """Proxy to a remote provenance API"""

    backend_class = ProvenanceInterface
    reraise_exceptions = [
        exc.ProvenanceException,
    ]
