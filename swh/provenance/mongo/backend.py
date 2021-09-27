# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from datetime import datetime, timezone
import os
from types import TracebackType
from typing import Any, Dict, Generator, Iterable, List, Optional, Set, Type, Union

from bson import ObjectId
import mongomock
import pymongo

from swh.core.statsd import statsd
from swh.model.model import Sha1Git

from ..interface import (
    EntityType,
    ProvenanceResult,
    ProvenanceStorageInterface,
    RelationData,
    RelationType,
    RevisionData,
)

STORAGE_DURATION_METRIC = "swh_provenance_storage_mongodb_duration_seconds"


class ProvenanceStorageMongoDb:
    def __init__(self, engine: str, **kwargs):
        self.engine = engine
        self.dbname = kwargs.pop("dbname")
        self.conn_args = kwargs

    def __enter__(self) -> ProvenanceStorageInterface:
        self.open()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "close"})
    def close(self) -> None:
        self.db.client.close()

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "content_add"})
    def content_add(
        self, cnts: Union[Iterable[Sha1Git], Dict[Sha1Git, Optional[datetime]]]
    ) -> bool:
        data = cnts if isinstance(cnts, dict) else dict.fromkeys(cnts)
        existing = {
            x["sha1"]: x
            for x in self.db.content.find(
                {"sha1": {"$in": list(data)}}, {"sha1": 1, "ts": 1, "_id": 1}
            )
        }
        for sha1, date in data.items():
            ts = datetime.timestamp(date) if date is not None else None
            if sha1 in existing:
                cnt = existing[sha1]
                if ts is not None and (cnt["ts"] is None or ts < cnt["ts"]):
                    self.db.content.update_one(
                        {"_id": cnt["_id"]}, {"$set": {"ts": ts}}
                    )
            else:
                self.db.content.insert_one(
                    {
                        "sha1": sha1,
                        "ts": ts,
                        "revision": {},
                        "directory": {},
                    }
                )
        return True

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "content_find_first"})
    def content_find_first(self, id: Sha1Git) -> Optional[ProvenanceResult]:
        # get all the revisions
        # iterate and find the earliest
        content = self.db.content.find_one({"sha1": id})
        if not content:
            return None

        occurs = []
        for revision in self.db.revision.find(
            {"_id": {"$in": [ObjectId(obj_id) for obj_id in content["revision"]]}}
        ):
            if revision["preferred"] is not None:
                origin = self.db.origin.find_one({"sha1": revision["preferred"]})
            else:
                origin = {"url": None}

            for path in content["revision"][str(revision["_id"])]:
                occurs.append(
                    ProvenanceResult(
                        content=id,
                        revision=revision["sha1"],
                        date=datetime.fromtimestamp(revision["ts"], timezone.utc),
                        origin=origin["url"],
                        path=path,
                    )
                )
        return sorted(occurs, key=lambda x: (x.date, x.revision, x.origin, x.path))[0]

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "content_find_all"})
    def content_find_all(
        self, id: Sha1Git, limit: Optional[int] = None
    ) -> Generator[ProvenanceResult, None, None]:
        content = self.db.content.find_one({"sha1": id})
        if not content:
            return None

        occurs = []
        for revision in self.db.revision.find(
            {"_id": {"$in": [ObjectId(obj_id) for obj_id in content["revision"]]}}
        ):
            if revision["preferred"] is not None:
                origin = self.db.origin.find_one({"sha1": revision["preferred"]})
            else:
                origin = {"url": None}

            for path in content["revision"][str(revision["_id"])]:
                occurs.append(
                    ProvenanceResult(
                        content=id,
                        revision=revision["sha1"],
                        date=datetime.fromtimestamp(revision["ts"], timezone.utc),
                        origin=origin["url"],
                        path=path,
                    )
                )
        for directory in self.db.directory.find(
            {"_id": {"$in": [ObjectId(obj_id) for obj_id in content["directory"]]}}
        ):
            for revision in self.db.revision.find(
                {"_id": {"$in": [ObjectId(obj_id) for obj_id in directory["revision"]]}}
            ):
                if revision["preferred"] is not None:
                    origin = self.db.origin.find_one({"sha1": revision["preferred"]})
                else:
                    origin = {"url": None}

                for suffix in content["directory"][str(directory["_id"])]:
                    for prefix in directory["revision"][str(revision["_id"])]:
                        path = (
                            os.path.join(prefix, suffix)
                            if prefix not in [b".", b""]
                            else suffix
                        )
                        occurs.append(
                            ProvenanceResult(
                                content=id,
                                revision=revision["sha1"],
                                date=datetime.fromtimestamp(
                                    revision["ts"], timezone.utc
                                ),
                                origin=origin["url"],
                                path=path,
                            )
                        )
        yield from sorted(occurs, key=lambda x: (x.date, x.revision, x.origin, x.path))

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "content_get"})
    def content_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, datetime]:
        return {
            x["sha1"]: datetime.fromtimestamp(x["ts"], timezone.utc)
            for x in self.db.content.find(
                {"sha1": {"$in": list(ids)}, "ts": {"$ne": None}},
                {"sha1": 1, "ts": 1, "_id": 0},
            )
        }

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "directory_add"})
    def directory_add(
        self, dirs: Union[Iterable[Sha1Git], Dict[Sha1Git, Optional[datetime]]]
    ) -> bool:
        data = dirs if isinstance(dirs, dict) else dict.fromkeys(dirs)
        existing = {
            x["sha1"]: x
            for x in self.db.directory.find(
                {"sha1": {"$in": list(data)}}, {"sha1": 1, "ts": 1, "_id": 1}
            )
        }
        for sha1, date in data.items():
            ts = datetime.timestamp(date) if date is not None else None
            if sha1 in existing:
                dir = existing[sha1]
                if ts is not None and (dir["ts"] is None or ts < dir["ts"]):
                    self.db.directory.update_one(
                        {"_id": dir["_id"]}, {"$set": {"ts": ts}}
                    )
            else:
                self.db.directory.insert_one({"sha1": sha1, "ts": ts, "revision": {}})
        return True

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "directory_get"})
    def directory_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, datetime]:
        return {
            x["sha1"]: datetime.fromtimestamp(x["ts"], timezone.utc)
            for x in self.db.directory.find(
                {"sha1": {"$in": list(ids)}, "ts": {"$ne": None}},
                {"sha1": 1, "ts": 1, "_id": 0},
            )
        }

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "entity_get_all"})
    def entity_get_all(self, entity: EntityType) -> Set[Sha1Git]:
        return {
            x["sha1"]
            for x in self.db.get_collection(entity.value).find(
                {}, {"sha1": 1, "_id": 0}
            )
        }

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "location_add"})
    def location_add(self, paths: Iterable[bytes]) -> bool:
        # TODO: implement this methods if path are to be stored in a separate collection
        return True

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "location_get_all"})
    def location_get_all(self) -> Set[bytes]:
        contents = self.db.content.find({}, {"revision": 1, "_id": 0, "directory": 1})
        paths: List[Iterable[bytes]] = []
        for content in contents:
            paths.extend(value for _, value in content["revision"].items())
            paths.extend(value for _, value in content["directory"].items())

        dirs = self.db.directory.find({}, {"revision": 1, "_id": 0})
        for each_dir in dirs:
            paths.extend(value for _, value in each_dir["revision"].items())
        return set(sum(paths, []))

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "open"})
    def open(self) -> None:
        if self.engine == "mongomock":
            self.db = mongomock.MongoClient(**self.conn_args).get_database(self.dbname)
        else:
            # assume real MongoDB server by default
            self.db = pymongo.MongoClient(**self.conn_args).get_database(self.dbname)

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "origin_add"})
    def origin_add(self, orgs: Dict[Sha1Git, str]) -> bool:
        existing = {
            x["sha1"]: x
            for x in self.db.origin.find(
                {"sha1": {"$in": list(orgs)}}, {"sha1": 1, "url": 1, "_id": 1}
            )
        }
        for sha1, url in orgs.items():
            if sha1 not in existing:
                # add new origin
                self.db.origin.insert_one({"sha1": sha1, "url": url})
        return True

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "origin_get"})
    def origin_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, str]:
        return {
            x["sha1"]: x["url"]
            for x in self.db.origin.find(
                {"sha1": {"$in": list(ids)}}, {"sha1": 1, "url": 1, "_id": 0}
            )
        }

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "revision_add"})
    def revision_add(
        self, revs: Union[Iterable[Sha1Git], Dict[Sha1Git, RevisionData]]
    ) -> bool:
        data = (
            revs
            if isinstance(revs, dict)
            else dict.fromkeys(revs, RevisionData(date=None, origin=None))
        )
        existing = {
            x["sha1"]: x
            for x in self.db.revision.find(
                {"sha1": {"$in": list(data)}},
                {"sha1": 1, "ts": 1, "preferred": 1, "_id": 1},
            )
        }
        for sha1, info in data.items():
            ts = datetime.timestamp(info.date) if info.date is not None else None
            preferred = info.origin
            if sha1 in existing:
                rev = existing[sha1]
                if ts is None or (rev["ts"] is not None and ts >= rev["ts"]):
                    ts = rev["ts"]
                if preferred is None:
                    preferred = rev["preferred"]
                if ts != rev["ts"] or preferred != rev["preferred"]:
                    self.db.revision.update_one(
                        {"_id": rev["_id"]},
                        {"$set": {"ts": ts, "preferred": preferred}},
                    )
            else:
                self.db.revision.insert_one(
                    {
                        "sha1": sha1,
                        "preferred": preferred,
                        "origin": [],
                        "revision": [],
                        "ts": ts,
                    }
                )
        return True

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "revision_get"})
    def revision_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, RevisionData]:
        return {
            x["sha1"]: RevisionData(
                date=datetime.fromtimestamp(x["ts"], timezone.utc) if x["ts"] else None,
                origin=x["preferred"],
            )
            for x in self.db.revision.find(
                {
                    "sha1": {"$in": list(ids)},
                    "$or": [{"preferred": {"$ne": None}}, {"ts": {"$ne": None}}],
                },
                {"sha1": 1, "preferred": 1, "ts": 1, "_id": 0},
            )
        }

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "relation_add"})
    def relation_add(
        self, relation: RelationType, data: Dict[Sha1Git, Set[RelationData]]
    ) -> bool:
        src_relation, *_, dst_relation = relation.value.split("_")

        dst_objs = {
            x["sha1"]: x["_id"]
            for x in self.db.get_collection(dst_relation).find(
                {
                    "sha1": {
                        "$in": list({rel.dst for rels in data.values() for rel in rels})
                    }
                },
                {"_id": 1, "sha1": 1},
            )
        }

        denorm: Dict[Sha1Git, Any] = {}
        for src, rels in data.items():
            for rel in rels:
                if src_relation != "revision":
                    denorm.setdefault(src, {}).setdefault(
                        str(dst_objs[rel.dst]), []
                    ).append(rel.path)
                else:
                    denorm.setdefault(src, []).append(dst_objs[rel.dst])

        src_objs = {
            x["sha1"]: x
            for x in self.db.get_collection(src_relation).find(
                {"sha1": {"$in": list(denorm.keys())}}
            )
        }

        for sha1, dsts in denorm.items():
            # update
            if src_relation != "revision":
                k = {
                    obj_id: list(set(paths + dsts.get(obj_id, [])))
                    for obj_id, paths in src_objs[sha1][dst_relation].items()
                }
                self.db.get_collection(src_relation).update_one(
                    {"_id": src_objs[sha1]["_id"]},
                    {"$set": {dst_relation: dict(dsts, **k)}},
                )
            else:
                self.db.get_collection(src_relation).update_one(
                    {"_id": src_objs[sha1]["_id"]},
                    {
                        "$set": {
                            dst_relation: list(set(src_objs[sha1][dst_relation] + dsts))
                        }
                    },
                )
        return True

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "relation_get"})
    def relation_get(
        self, relation: RelationType, ids: Iterable[Sha1Git], reverse: bool = False
    ) -> Dict[Sha1Git, Set[RelationData]]:
        src, *_, dst = relation.value.split("_")
        sha1s = set(ids)
        if not reverse:
            empty: Union[Dict[str, bytes], List[str]] = {} if src != "revision" else []
            src_objs = {
                x["sha1"]: x[dst]
                for x in self.db.get_collection(src).find(
                    {"sha1": {"$in": list(sha1s)}, dst: {"$ne": empty}},
                    {"_id": 0, "sha1": 1, dst: 1},
                )
            }
            dst_ids = list(
                {ObjectId(obj_id) for _, value in src_objs.items() for obj_id in value}
            )
            dst_objs = {
                x["sha1"]: x["_id"]
                for x in self.db.get_collection(dst).find(
                    {"_id": {"$in": dst_ids}}, {"_id": 1, "sha1": 1}
                )
            }
            if src != "revision":
                return {
                    src_sha1: {
                        RelationData(dst=dst_sha1, path=path)
                        for dst_sha1, dst_obj_id in dst_objs.items()
                        for dst_obj_str, paths in denorm.items()
                        for path in paths
                        if dst_obj_id == ObjectId(dst_obj_str)
                    }
                    for src_sha1, denorm in src_objs.items()
                }
            else:
                return {
                    src_sha1: {
                        RelationData(dst=dst_sha1, path=None)
                        for dst_sha1, dst_obj_id in dst_objs.items()
                        for dst_obj_ref in denorm
                        if dst_obj_id == dst_obj_ref
                    }
                    for src_sha1, denorm in src_objs.items()
                }
        else:
            dst_objs = {
                x["sha1"]: x["_id"]
                for x in self.db.get_collection(dst).find(
                    {"sha1": {"$in": list(sha1s)}}, {"_id": 1, "sha1": 1}
                )
            }
            src_objs = {
                x["sha1"]: x[dst]
                for x in self.db.get_collection(src).find(
                    {}, {"_id": 0, "sha1": 1, dst: 1}
                )
            }
            result: Dict[Sha1Git, Set[RelationData]] = {}
            if src != "revision":
                for dst_sha1, dst_obj_id in dst_objs.items():
                    for src_sha1, denorm in src_objs.items():
                        for dst_obj_str, paths in denorm.items():
                            if dst_obj_id == ObjectId(dst_obj_str):
                                result.setdefault(src_sha1, set()).update(
                                    RelationData(dst=dst_sha1, path=path)
                                    for path in paths
                                )
            else:
                for dst_sha1, dst_obj_id in dst_objs.items():
                    for src_sha1, denorm in src_objs.items():
                        if dst_obj_id in {
                            ObjectId(dst_obj_str) for dst_obj_str in denorm
                        }:
                            result.setdefault(src_sha1, set()).add(
                                RelationData(dst=dst_sha1, path=None)
                            )
            return result

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "relation_get_all"})
    def relation_get_all(
        self, relation: RelationType
    ) -> Dict[Sha1Git, Set[RelationData]]:
        src, *_, dst = relation.value.split("_")
        empty: Union[Dict[str, bytes], List[str]] = {} if src != "revision" else []
        src_objs = {
            x["sha1"]: x[dst]
            for x in self.db.get_collection(src).find(
                {dst: {"$ne": empty}}, {"_id": 0, "sha1": 1, dst: 1}
            )
        }
        dst_ids = list(
            {ObjectId(obj_id) for _, value in src_objs.items() for obj_id in value}
        )
        dst_objs = {
            x["_id"]: x["sha1"]
            for x in self.db.get_collection(dst).find(
                {"_id": {"$in": dst_ids}}, {"_id": 1, "sha1": 1}
            )
        }
        if src != "revision":
            return {
                src_sha1: {
                    RelationData(dst=dst_sha1, path=path)
                    for dst_obj_id, dst_sha1 in dst_objs.items()
                    for dst_obj_str, paths in denorm.items()
                    for path in paths
                    if dst_obj_id == ObjectId(dst_obj_str)
                }
                for src_sha1, denorm in src_objs.items()
            }
        else:
            return {
                src_sha1: {
                    RelationData(dst=dst_sha1, path=None)
                    for dst_obj_id, dst_sha1 in dst_objs.items()
                    for dst_obj_ref in denorm
                    if dst_obj_id == dst_obj_ref
                }
                for src_sha1, denorm in src_objs.items()
            }

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "with_path"})
    def with_path(self) -> bool:
        return True
