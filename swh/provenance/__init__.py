from .archive import ArchiveInterface
from .postgresql.archive import ArchivePostgreSQL
from .postgresql.db_utils import connect
from .storage.archive import ArchiveStorage
from .provenance import ProvenanceInterface


def get_archive(cls: str, **kwargs) -> ArchiveInterface:
    if cls == "api":
        return ArchiveStorage(**kwargs["storage"])
    elif cls == "direct":
        conn = connect(kwargs["db"])
        return ArchivePostgreSQL(conn)
    else:
        raise NotImplementedError


def get_provenance(cls: str, **kwargs) -> ProvenanceInterface:
    if cls == "local":
        conn = connect(kwargs["db"])
        if kwargs.get("with_path", True):
            from .postgresql.provenance_with_path import ProvenanceWithPathDB
            return ProvenanceWithPathDB(conn)
        else:
            from .postgresql.provenance_without_path import ProvenanceWithoutPathDB
            return ProvenanceWithoutPathDB(conn)
    else:
        raise NotImplementedError
