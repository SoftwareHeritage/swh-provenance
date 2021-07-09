# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .archive import ArchiveInterface
    from .interface import ProvenanceInterface, ProvenanceStorageInterface


def get_archive(cls: str, **kwargs) -> ArchiveInterface:
    """Get an archive object of class ``cls`` with arguments ``args``.

    Args:
        cls: archive's class, either 'api' or 'direct'
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
    if cls == "local":
        from swh.core.db import BaseDb

        from .postgresql.provenancedb_base import ProvenanceDBBase

        conn = BaseDb.connect(**kwargs["db"]).conn
        raise_on_commit = kwargs.get("raise_on_commit", False)
        if ProvenanceDBBase(conn, raise_on_commit).flavor == "with-path":
            from .postgresql.provenancedb_with_path import ProvenanceWithPathDB

            return ProvenanceWithPathDB(conn, raise_on_commit)
        else:
            from .postgresql.provenancedb_without_path import ProvenanceWithoutPathDB

            return ProvenanceWithoutPathDB(conn, raise_on_commit)
    else:
        raise ValueError
