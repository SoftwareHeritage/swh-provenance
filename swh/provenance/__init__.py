# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from typing import TYPE_CHECKING
import warnings

if TYPE_CHECKING:
    from .archive import ArchiveInterface
    from .interface import ProvenanceInterface, ProvenanceStorageInterface


def get_archive(cls: str, **kwargs) -> ArchiveInterface:
    """Get an archive object of class ``cls`` with arguments ``args``.

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

        from .storage.archive import ArchiveStorage

        return ArchiveStorage(get_storage(**kwargs["storage"]))

    elif cls == "direct":
        from swh.core.db import BaseDb

        from .postgresql.archive import ArchivePostgreSQL

        return ArchivePostgreSQL(BaseDb.connect(**kwargs["db"]).conn)

    elif cls == "graph":
        try:
            from swh.storage import get_storage

            from .swhgraph.archive import ArchiveGraph

            return ArchiveGraph(kwargs.get("url"), get_storage(**kwargs["storage"]))

        except ModuleNotFoundError:
            raise EnvironmentError(
                "Graph configuration required but module is not installed."
            )
    elif cls == "multiplexer":

        from .multiplexer.archive import ArchiveMultiplexed

        archives = []
        for ctr, archive in enumerate(kwargs["archives"]):
            name = archive.pop("name", f"backend_{ctr}")
            archives.append((name, get_archive(**archive)))

        return ArchiveMultiplexed(archives)
    else:
        raise ValueError


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


def get_provenance_storage(cls: str, **kwargs) -> ProvenanceStorageInterface:
    """Get an archive object of class ``cls`` with arguments ``args``.

    Args:
        cls: storage's class, only 'local' is currently supported
        args: dictionary of arguments passed to the storage class constructor

    Returns:
        an instance of storage object

    Raises:
        :cls:`ValueError` if passed an unknown archive class.
    """
    if cls in ["local", "postgresql"]:
        from swh.provenance.postgresql.provenance import ProvenanceStoragePostgreSql

        if cls == "local":
            warnings.warn(
                '"local" class is deprecated for provenance storage, please '
                'use "postgresql" class instead.',
                DeprecationWarning,
            )

        raise_on_commit = kwargs.get("raise_on_commit", False)
        return ProvenanceStoragePostgreSql(
            raise_on_commit=raise_on_commit, **kwargs["db"]
        )

    elif cls == "rabbitmq":
        from .api.client import ProvenanceStorageRabbitMQClient

        rmq_storage = ProvenanceStorageRabbitMQClient(**kwargs)
        if TYPE_CHECKING:
            assert isinstance(rmq_storage, ProvenanceStorageInterface)
        return rmq_storage

    raise ValueError


get_datastore = get_provenance_storage
