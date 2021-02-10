from .archive import ArchiveInterface
from .postgresql.archive import ArchivePostgreSQL
from .postgresql.db_utils import connect
from .postgresql.provenance import ProvenancePostgreSQL
from .postgresql_nopath.provenance import ProvenancePostgreSQLNoPath
from .provenance import ProvenanceInterface
from .storage.archive import ArchiveStorage


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
            return ProvenancePostgreSQL(conn)
        else:
            return ProvenancePostgreSQLNoPath(conn)
    else:
        raise NotImplementedError
