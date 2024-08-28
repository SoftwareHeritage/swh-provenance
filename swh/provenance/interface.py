# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import List, Optional

from typing_extensions import Protocol, runtime_checkable

from swh.core.api import remote_api_endpoint
from swh.model.swhids import CoreSWHID, QualifiedSWHID


@runtime_checkable
class ProvenanceInterface(Protocol):
    @remote_api_endpoint("check_config")
    def check_config(self) -> bool:
        """Check that the storage is configured and ready to go."""
        ...

    @remote_api_endpoint("whereis")
    def whereis(self, *, swhid: CoreSWHID) -> Optional[QualifiedSWHID]:
        """Given a SWHID return a QualifiedSWHID with some provenance info:

        - the release or revision containing that content or directory
        - the url of the origin containing that content or directory

        This can also be called for revision, release or snapshot to retrieve
        origin url information if any. When using a revision, the anchor will
        be an association release if any.

        If no anchor could be found, this function return None.

        note: The quality of the result is not guaranteed whatsoever. Since the
        definition of "best" likely vary from one usage to the next, this API
        will evolve in the futur when this notion get better defined.

        For example, if we are looking for provenance information to detect
        prior art. We search for the first appearance of a content. So the
        "best answer" is the oldest content, something a bit tricky to
        determine as we can't fully trust the date of revision. On the other
        hand, if we try to know which library are used and at which version,
        to detect CVE or outdated dependencies, the best answer is the most
        recent release/revision in the authoritative origin relevant to a
        content.  Finding the authoritative origin is a challenge in itself.

        """
        ...

    @remote_api_endpoint("whereare")
    def whereare(self, *, swhids: List[CoreSWHID]) -> List[Optional[QualifiedSWHID]]:
        """Given a SWHID list return a list of provenance info:

        See `whereis` documentation for details on the provenance info.
        """
        ...
