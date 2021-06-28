from typing import TYPE_CHECKING

from .postgresql.db_utils import connect

if TYPE_CHECKING:
    from .archive import ArchiveInterface
    from .provenance import ProvenanceInterface, ProvenanceStorageInterface


def get_archive(cls: str, **kwargs) -> "ArchiveInterface":
    if cls == "api":
        from swh.storage import get_storage

        from .storage.archive import ArchiveStorage

        return ArchiveStorage(get_storage(**kwargs["storage"]))
    elif cls == "direct":
        from .postgresql.archive import ArchivePostgreSQL

        return ArchivePostgreSQL(connect(kwargs["db"]))
    else:
        raise NotImplementedError


def get_provenance(**kwargs) -> "ProvenanceInterface":
    from .backend import ProvenanceBackend

    return ProvenanceBackend(get_provenance_storage(**kwargs))


def get_provenance_storage(cls: str, **kwargs) -> "ProvenanceStorageInterface":
    if cls == "local":
        from .postgresql.provenancedb_base import ProvenanceDBBase

        conn = connect(kwargs["db"])
        flavor = ProvenanceDBBase(conn).flavor
        if flavor == "with-path":
            from .postgresql.provenancedb_with_path import ProvenanceWithPathDB

            return ProvenanceWithPathDB(conn)
        else:
            from .postgresql.provenancedb_without_path import ProvenanceWithoutPathDB

            return ProvenanceWithoutPathDB(conn)
    else:
        raise NotImplementedError
