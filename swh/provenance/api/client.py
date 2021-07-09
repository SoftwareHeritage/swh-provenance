# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core.api import RPCClient

from ..interface import ProvenanceStorageInterface
from .serializers import DECODERS, ENCODERS


class RemoteProvenanceStorage(RPCClient):
    """Proxy to a remote provenance storage API"""

    backend_class = ProvenanceStorageInterface
    extra_type_decoders = DECODERS
    extra_type_encoders = ENCODERS
