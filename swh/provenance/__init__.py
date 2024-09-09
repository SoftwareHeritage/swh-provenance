# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import importlib
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    # importing swh.storage.interface triggers the load of 300+ modules, so...
    from swh.provenance.interface import ProvenanceInterface


PROVENANCE_IMPLEMENTATIONS = {
    "graph": "swh.provenance.backend.graph.GraphProvenance",
    "remote": "swh.provenance.api.client.RemoteProvenance",
    "postgresql": "swh.provenance.backend.postgresql.PostgresqlProvenance",
    "known_swhid_filter": "swh.provenance.backend.known_swhid_proxy.KnownSwhidFilterProvenance",
}

ProvenanceSpec = Dict[str, Any]


def get_provenance(cls: str, **kwargs: ProvenanceSpec) -> "ProvenanceInterface":
    """Get a provenance service of class `cls` with arguments `args`.

    Args:
        cls: provenance's class
        args: dictionary of arguments passed to the
            search class constructor

    Returns:
        an instance of swh.provenance's classes

    Raises:
        ValueError if passed an unknown search class.

    """
    class_path = PROVENANCE_IMPLEMENTATIONS.get(cls)
    if class_path is None:
        raise ValueError(
            "Unknown provenance class `%s`. Supported: %s"
            % (cls, ", ".join(PROVENANCE_IMPLEMENTATIONS))
        )

    (module_path, class_name) = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path, package=__package__)
    Provenance = getattr(module, class_name)
    return Provenance(**kwargs)
