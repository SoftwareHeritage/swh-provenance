from typing import TYPE_CHECKING
import warnings

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
        if "with_path" in kwargs:
            warnings.warn(
                "Usage of the 'with-path' config option is deprecated. "
                "The db flavor is now used instead.",
                DeprecationWarning,
            )

        with_path = kwargs.get("with_path")
        from swh.provenance.provenance import ProvenanceBackend

        prov = ProvenanceBackend(conn)
        if with_path is not None:
            flavor = "with-path" if with_path else "without-path"
            if prov.storage.flavor != flavor:
                raise ValueError(
                    "The given flavor does not match the flavor stored in the backend."
                )
        return prov
    else:
        raise NotImplementedError
