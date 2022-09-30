# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from .interface import ArchiveInterface


def get_archive(cls: str, **kwargs) -> ArchiveInterface:
    """Get an ArchiveInterface-like object of class ``cls`` with arguments ``args``.

    Args:
        cls: archive's class, either 'api', 'direct' or 'graph'
        args: dictionary of arguments passed to the archive class constructor

    Returns:
        an instance of archive object (either using swh.storage API or direct
        queries to the archive's database)

    Raises:
        :cls:`ValueError` if passed an unknown archive class.
    """
    if cls == "api":
        from swh.storage import get_storage

        from .storage import ArchiveStorage

        return ArchiveStorage(get_storage(**kwargs["storage"]))

    elif cls == "direct":
        from swh.core.db import BaseDb

        from .postgresql import ArchivePostgreSQL

        return ArchivePostgreSQL(BaseDb.connect(**kwargs["db"]).conn)

    elif cls == "graph":
        try:
            from swh.storage import get_storage

            from .swhgraph import ArchiveGraph

            return ArchiveGraph(kwargs.get("url"), get_storage(**kwargs["storage"]))

        except ModuleNotFoundError:
            raise EnvironmentError(
                "Graph configuration required but module is not installed."
            )
    elif cls == "multiplexer":

        from .multiplexer import ArchiveMultiplexed

        archives = []
        for ctr, archive in enumerate(kwargs["archives"]):
            name = archive.pop("name", f"backend_{ctr}")
            archives.append((name, get_archive(**archive)))

        return ArchiveMultiplexed(archives)
    else:
        raise ValueError
