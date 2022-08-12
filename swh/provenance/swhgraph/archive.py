# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterable, Tuple

from swh.core.statsd import statsd
from swh.model.model import Sha1Git
from swh.model.swhids import CoreSWHID, ObjectType
from swh.storage.interface import StorageInterface

ARCHIVE_DURATION_METRIC = "swh_provenance_archive_graph_duration_seconds"


class ArchiveGraph:
    def __init__(self, graph, storage: StorageInterface) -> None:
        self.graph = graph
        self.storage = storage  # required by ArchiveInterface

    @statsd.timed(metric=ARCHIVE_DURATION_METRIC, tags={"method": "directory_ls"})
    def directory_ls(self, id: Sha1Git, minsize: int = 0) -> Iterable[Dict[str, Any]]:
        raise NotImplementedError

    @statsd.timed(
        metric=ARCHIVE_DURATION_METRIC,
        tags={"method": "revision_get_some_outbound_edges"},
    )
    def revision_get_some_outbound_edges(
        self, revision_id: Sha1Git
    ) -> Iterable[Tuple[Sha1Git, Sha1Git]]:
        src = CoreSWHID(object_type=ObjectType.REVISION, object_id=revision_id)
        request = self.graph.visit_edges(str(src), edges="rev:rev")

        for edge in request:
            if edge:
                yield (
                    CoreSWHID.from_string(edge[0]).object_id,
                    CoreSWHID.from_string(edge[1]).object_id,
                )

    @statsd.timed(metric=ARCHIVE_DURATION_METRIC, tags={"method": "snapshot_get_heads"})
    def snapshot_get_heads(self, id: Sha1Git) -> Iterable[Sha1Git]:
        src = CoreSWHID(object_type=ObjectType.SNAPSHOT, object_id=id)
        request = self.graph.visit_nodes(
            str(src), edges="snp:rev,snp:rel,rel:rev", return_types="rev"
        )

        yield from (
            CoreSWHID.from_string(swhid).object_id for swhid in request if swhid
        )
