from typing import TYPE_CHECKING

from .postgresql.db_utils import connect

if TYPE_CHECKING:
    from swh.provenance.archive import ArchiveInterface
    from swh.provenance.provenance import ProvenanceInterface


def get_archive(cls: str, **kwargs) -> "ArchiveInterface":
    if cls == "api":
        from swh.provenance.storage.archive import ArchiveStorage
        from swh.storage import get_storage

        return ArchiveStorage(get_storage(**kwargs["storage"]))
    elif cls == "direct":
        from swh.provenance.postgresql.archive import ArchivePostgreSQL

        return ArchivePostgreSQL(connect(kwargs["db"]))
    else:
        raise NotImplementedError


def get_provenance(cls: str, **kwargs) -> "ProvenanceInterface":
    if cls == "local":
        conn = connect(kwargs["db"])
        if kwargs.get("with_path", True):
            from swh.provenance.postgresql.provenancedb_with_path import (
                ProvenanceWithPathDB,
            )

            return ProvenanceWithPathDB(conn)
        else:
            from swh.provenance.postgresql.provenancedb_without_path import (
                ProvenanceWithoutPathDB,
            )

            return ProvenanceWithoutPathDB(conn)
    else:
        raise NotImplementedError
