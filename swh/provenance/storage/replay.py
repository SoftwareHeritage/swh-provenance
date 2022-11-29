# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
from datetime import datetime
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

try:
    from systemd.daemon import notify
except ImportError:
    notify = None

from swh.core.statsd import statsd
from swh.journal.serializers import kafka_to_value
from swh.provenance.storage.interface import (
    DirectoryData,
    RelationData,
    RelationType,
    RevisionData,
    Sha1Git,
)

from .interface import ProvenanceStorageInterface

logger = logging.getLogger(__name__)

REPLAY_OPERATIONS_METRIC = "swh_provenance_replayer_operations_total"
REPLAY_DURATION_METRIC = "swh_provenance_replayer_duration_seconds"


def cvrt_directory(msg_d):
    return (msg_d["id"], DirectoryData(date=msg_d["value"], flat=False))


def cvrt_revision(msg_d):
    return (msg_d["id"], RevisionData(date=msg_d["value"], origin=None))


def cvrt_default(msg_d):
    return (msg_d["id"], msg_d["value"])


def cvrt_relation(msg_d):
    return (
        msg_d["src"],
        RelationData(dst=msg_d["dst"], path=msg_d["path"], dst_date=msg_d["dst_date"]),
    )


OBJECT_CONVERTERS: Dict[str, Callable[[Dict], Tuple[bytes, Any]]] = {
    "directory": cvrt_directory,
    "revision": cvrt_revision,
    "content": cvrt_default,
    "content_in_revision": cvrt_relation,
    "content_in_directory": cvrt_relation,
    "directory_in_revision": cvrt_relation,
}


class ProvenanceObjectDeserializer:
    def __init__(
        self,
        raise_on_error: bool = False,
        reporter: Optional[Callable[[str, bytes], None]] = None,
    ):
        self.reporter = reporter
        self.raise_on_error = raise_on_error

    def convert(self, object_type: str, msg: bytes) -> Optional[Tuple[bytes, Any]]:
        dict_repr = kafka_to_value(msg)
        obj = OBJECT_CONVERTERS[object_type](dict_repr)
        return obj

    def report_failure(self, msg: bytes, obj: Dict):
        if self.reporter:
            self.reporter(obj["id"].hex(), msg)


def process_replay_objects(
    all_objects: Dict[str, List[Tuple[bytes, Any]]],
    *,
    storage: ProvenanceStorageInterface,
) -> None:
    for object_type, objects in all_objects.items():
        logger.debug("Inserting %s %s objects", len(objects), object_type)
        with statsd.timed(REPLAY_DURATION_METRIC, tags={"object_type": object_type}):
            _insert_objects(object_type, objects, storage)
        statsd.increment(
            REPLAY_OPERATIONS_METRIC, len(objects), tags={"object_type": object_type}
        )
    if notify:
        notify("WATCHDOG=1")


def _insert_objects(
    object_type: str,
    objects: List[Tuple[bytes, Any]],
    storage: ProvenanceStorageInterface,
) -> None:
    """Insert objects of type object_type in the storage."""
    if object_type not in OBJECT_CONVERTERS:
        logger.warning("Received a series of %s, this should not happen", object_type)
        return

    if "_in_" in object_type:
        reldata = defaultdict(set)
        for k, v in objects:
            reldata[k].add(v)
        storage.relation_add(relation=RelationType(object_type), data=reldata)
    elif object_type in ("revision", "directory"):
        entitydata: Dict[Sha1Git, Union[RevisionData, DirectoryData]] = {}
        for k, v in objects:
            if k not in entitydata or entitydata[k].date > v.date:
                entitydata[k] = v
        getattr(storage, f"{object_type}_add")(entitydata)
    else:
        data: Dict[Sha1Git, datetime] = {}
        for k, v in objects:
            assert isinstance(v, datetime)
            if k not in data or data[k] > v:
                data[k] = v
        getattr(storage, f"{object_type}_add")(data)
