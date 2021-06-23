from typing import Any, Dict, Iterable

from typing_extensions import Protocol, runtime_checkable

from swh.model.model import Revision, Sha1Git


@runtime_checkable
class ArchiveInterface(Protocol):
    def directory_ls(self, id: Sha1Git) -> Iterable[Dict[str, Any]]:
        """List entries for one directory.

        Args:
            id: sha1 id of the directory to list entries from.

        Yields:
            directory entries for such directory.

        """
        ...

    def revision_get(self, ids: Iterable[Sha1Git]) -> Iterable[Revision]:
        """Given a list of sha1, return the revisions' information

        Args:
            revisions: list of sha1s for the revisions to be retrieved

        Yields:
            revisions matching the identifiers. If a revision does
            not exist, the provided sha1 is simply ignored.

        """
        ...

    def snapshot_get_heads(self, id: Sha1Git) -> Iterable[Sha1Git]:
        """List all revisions pointed by one snapshot.

        Args:
            id: sha1 id of the snapshot.

        Yields:
            sha1 ids of found revisions.

        """
        ...
