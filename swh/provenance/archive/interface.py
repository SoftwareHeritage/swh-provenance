# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterable, Tuple

from typing_extensions import Protocol, runtime_checkable

from swh.model.model import Sha1Git
from swh.storage.interface import StorageInterface


@runtime_checkable
class ArchiveInterface(Protocol):
    storage: StorageInterface

    def directory_ls(self, id: Sha1Git, minsize: int = 0) -> Iterable[Dict[str, Any]]:
        """List entries for one directory.

        Args:
            id: sha1 id of the directory to list entries from.

        Yields:
            dictionary of entries in such directory containing only the keys "name",
            "target" and "type".

        """
        ...

    def revision_get_some_outbound_edges(
        self, id: Sha1Git
    ) -> Iterable[Tuple[Sha1Git, Sha1Git]]:
        """List some outbound edges from one revision. For each revision listed,
        all of its outbound edges must be returned.

        Args:
            id: sha1 id of the revision to list parents from.

        Returns:
            list of edges (revision_id, parent_revision_id)

        """
        ...

    def snapshot_get_heads(self, id: Sha1Git) -> Iterable[Sha1Git]:
        """List all revisions targeted by one snapshot.

        Args:
            id: sha1 id of the snapshot.

        Yields:
            sha1 ids of revisions that are a target of such snapshot.

        """
        ...
