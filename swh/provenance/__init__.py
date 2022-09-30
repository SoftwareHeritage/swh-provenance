# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from .interface import ProvenanceInterface
from .storage import get_provenance_storage


def get_provenance(**kwargs) -> ProvenanceInterface:
    """Get an provenance object with arguments ``args``.

    Args:
        args: dictionary of arguments to retrieve a swh.provenance.storage
        class (see :func:`get_provenance_storage` for details)

    Returns:
        an instance of provenance object
    """
    from .provenance import Provenance

    return Provenance(get_provenance_storage(**kwargs))


get_datastore = get_provenance_storage
