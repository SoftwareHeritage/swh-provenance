# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import List, Optional

import grpc

from swh.model.swhids import CoreSWHID, QualifiedSWHID
from swh.provenance.grpc.swhprovenance_pb2 import WhereAreOneRequest, WhereIsOneRequest
from swh.provenance.grpc.swhprovenance_pb2_grpc import ProvenanceServiceStub

logger = logging.getLogger(__name__)


class GrpcProvenance:
    def __init__(self, url: str):
        self._channel = grpc.insecure_channel(url)
        self._stub = ProvenanceServiceStub(self._channel)

    def check_config(self) -> bool:
        # if the constructor successfully connected, it means we are good
        return True

    def whereis(self, *, swhid: CoreSWHID) -> Optional[QualifiedSWHID]:
        str_swhid = str(swhid)
        try:
            result = self._stub.WhereIsOne(WhereIsOneRequest(swhid=str_swhid))
        except grpc.RpcError as exc:
            if exc.code() == grpc.StatusCode.NOT_FOUND:
                logger.debug("Unknown SWHID: %s", swhid)
                return None
            else:
                raise
        if result is None:
            return None
        else:
            assert result.swhid == str_swhid
            return QualifiedSWHID(
                object_type=swhid.object_type,
                object_id=swhid.object_id,
                anchor=result.anchor or None,
                origin=result.origin or None,
            )

    def whereare(self, *, swhids: List[CoreSWHID]) -> List[Optional[QualifiedSWHID]]:
        results: List[Optional[QualifiedSWHID]] = []

        for result in self._stub.WhereAreOne(
            WhereAreOneRequest(swhid=list(map(str, swhids)))
        ):
            if result is None:
                results.append(None)
            else:
                swhid = CoreSWHID.from_string(result.swhid)
                results.append(
                    QualifiedSWHID(
                        object_type=swhid.object_type,
                        object_id=swhid.object_id,
                        anchor=result.anchor or None,
                        origin=result.origin or None,
                    )
                )

        return results
