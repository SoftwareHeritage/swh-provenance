from typing import Any, Dict, Iterable

from typing_extensions import Protocol, runtime_checkable

from swh.model.model import Sha1Git


@runtime_checkable
class ArchiveInterface(Protocol):
    def directory_ls(self, id: Sha1Git) -> Iterable[Dict[str, Any]]:
        """List entries for one directory.

        Args:
            id: sha1 id of the directory to list entries from.

        Yields:
            dictionary of entries in such directory containing only the keys "name",
            "target" and "type".

        """
        ...

    def revision_get_parents(self, id: Sha1Git) -> Iterable[Sha1Git]:
        """List parents of one revision.

        Args:
            revisions: sha1 id of the revision to list parents from.

        Yields:
            sha1 ids for the parents of such revision.

        """
        ...

    def snapshot_get_heads(self, id: Sha1Git) -> Iterable[Sha1Git]:
        """List all revisions targeted by one snapshot.

        Args:
            id: sha1 id of the snapshot.

        Yields:
            sha1 ids of revisions that a target of such snapshot.

        """
        ...
