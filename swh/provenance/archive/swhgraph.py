# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterable, Tuple

from google.protobuf.field_mask_pb2 import FieldMask
import grpc

from swh.core.statsd import statsd
from swh.graph.grpc import swhgraph_pb2, swhgraph_pb2_grpc
from swh.model.model import Sha1Git
from swh.model.swhids import CoreSWHID, ObjectType
from swh.storage.interface import StorageInterface

ARCHIVE_DURATION_METRIC = "swh_provenance_archive_graph_duration_seconds"


class ArchiveGraph:
    def __init__(self, url, storage: StorageInterface) -> None:
        self.graph_url = url
        self._channel = grpc.insecure_channel(self.graph_url)
        self._stub = swhgraph_pb2_grpc.TraversalServiceStub(self._channel)
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
        src = str(CoreSWHID(object_type=ObjectType.REVISION, object_id=revision_id))
        request = self._stub.Traverse(
            swhgraph_pb2.TraversalRequest(
                src=[src],
                edges="rev:rev",
                max_edges=1000,
                mask=FieldMask(paths=["swhid", "successor"]),
            )
        )
        try:
            for node in request:
                obj_id = CoreSWHID.from_string(node.swhid).object_id
                if node.successor:
                    for parent in node.successor:
                        yield (obj_id, CoreSWHID.from_string(parent.swhid).object_id)
        except grpc.RpcError as e:
            if (
                e.code() == grpc.StatusCode.INVALID_ARGUMENT
                and "Unknown SWHID" in e.details()
            ):
                pass
            raise

    @statsd.timed(metric=ARCHIVE_DURATION_METRIC, tags={"method": "snapshot_get_heads"})
    def snapshot_get_heads(self, id: Sha1Git) -> Iterable[Sha1Git]:
        src = str(CoreSWHID(object_type=ObjectType.SNAPSHOT, object_id=id))
        request = self._stub.Traverse(
            swhgraph_pb2.TraversalRequest(
                src=[src],
                edges="snp:rev,snp:rel,rel:rev",
                return_nodes=swhgraph_pb2.NodeFilter(types="rev"),
                mask=FieldMask(paths=["swhid"]),
            )
        )
        try:
            yield from (CoreSWHID.from_string(node.swhid).object_id for node in request)
        except grpc.RpcError as e:
            if (
                e.code() == grpc.StatusCode.INVALID_ARGUMENT
                and "Unknown SWHID" in e.details()
            ):
                pass
            raise
