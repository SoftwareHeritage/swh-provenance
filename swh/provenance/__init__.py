from .archive import ArchiveInterface
from .provenance import ProvenanceInterface
from .storage.archive import ArchiveStorage
from .postgresql.archive import ArchivePostgreSQL
from .postgresql.db_utils import connect
from .postgresql.provenance import ProvenancePostgreSQL


def get_archive(cls: str, **kwargs) -> ArchiveInterface:
    if cls == "api":
        return ArchiveStorage(**kwargs["storage"])
    elif cls == "ps":
        conn = connect(kwargs["db"])
        return ArchivePostgreSQL(conn)
    else:
        raise NotImplementedError


def get_provenance(cls: str, **kwargs) -> ProvenanceInterface:
    if cls == "ps":
        conn = connect(kwargs["db"])
        return ProvenancePostgreSQL(conn)
    else:
        raise NotImplementedError
