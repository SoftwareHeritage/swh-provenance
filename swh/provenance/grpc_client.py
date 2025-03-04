# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import List, Optional

import grpc

from swh.model.swhids import CoreSWHID, QualifiedSWHID
from swh.provenance.grpc.swhprovenance_pb2 import WhereAreOneRequest, WhereIsOneRequest
from swh.provenance.grpc.swhprovenance_pb2_grpc import ProvenanceServiceStub


class GrpcProvenance:
    def __init__(self, url: str):
        self._channel = grpc.insecure_channel(url)
        self._stub = ProvenanceServiceStub(self._channel)

    def check_config(self) -> bool:
        # if the constructor successfully connected, it means we are good
        return False

    def whereis(self, *, swhid: CoreSWHID) -> Optional[QualifiedSWHID]:
        str_swhid = str(swhid)
        result = self._stub.WhereIsOne(WhereIsOneRequest(swhid=str_swhid))
        if result is None or result.anchor is None:
            return None
        else:
            assert result.swhid == str_swhid
            return QualifiedSWHID(
                object_type=swhid.object_type,
                object_id=swhid.object_id,
                anchor=CoreSWHID.from_string(result.anchor),
                origin=result.origin or None,
            )

    def whereare(self, *, swhids: List[CoreSWHID]) -> List[Optional[QualifiedSWHID]]:
        results: List[Optional[QualifiedSWHID]] = []

        for result in self._stub.WhereAreOne(
            WhereAreOneRequest(swhid=list(map(str, swhids)))
        ):
            if result is None or result.anchor is None:
                results.append(None)
            else:
                swhid = CoreSWHID.from_string(result.swhid)
                results.append(
                    QualifiedSWHID(
                        object_type=swhid.object_type,
                        object_id=swhid.object_id,
                        anchor=result.anchor,
                        origin=result.origin or None,
                    )
                )

        return results
