# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
from operator import itemgetter
from typing import Any
from typing import Counter as TCounter
from typing import Dict, Iterable, List, Set, Tuple, Type, Union

import pytest

from swh.core.db import BaseDb
from swh.graph.naive_client import NaiveClient
from swh.model.model import (
    BaseModel,
    Content,
    Directory,
    DirectoryEntry,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Revision,
    Sha1Git,
    Snapshot,
    SnapshotBranch,
    TargetType,
)
from swh.model.swhids import CoreSWHID, ExtendedObjectType, ExtendedSWHID
from swh.provenance.archive import ArchiveInterface
from swh.provenance.multiplexer.archive import ArchiveMultiplexed
from swh.provenance.postgresql.archive import ArchivePostgreSQL
from swh.provenance.storage.archive import ArchiveStorage
from swh.provenance.swhgraph.archive import ArchiveGraph
from swh.provenance.tests.conftest import fill_storage, load_repo_data
from swh.storage.interface import StorageInterface
from swh.storage.postgresql.storage import Storage


class ArchiveNoop:
    storage: StorageInterface

    def directory_ls(self, id: Sha1Git, minsize: int = 0) -> Iterable[Dict[str, Any]]:
        return []

    def revision_get_some_outbound_edges(
        self, revision_id: Sha1Git
    ) -> Iterable[Tuple[Sha1Git, Sha1Git]]:
        return []

    def snapshot_get_heads(self, id: Sha1Git) -> Iterable[Sha1Git]:
        return []


def check_directory_ls(
    reference: ArchiveInterface, archive: ArchiveInterface, data: Dict[str, List[dict]]
) -> None:
    for directory in data["directory"]:
        entries_ref = sorted(
            reference.directory_ls(directory["id"]), key=itemgetter("name")
        )
        entries = sorted(archive.directory_ls(directory["id"]), key=itemgetter("name"))
        assert entries_ref == entries


def check_revision_get_some_outbound_edges(
    reference: ArchiveInterface, archive: ArchiveInterface, data: Dict[str, List[dict]]
) -> None:
    for revision in data["revision"]:
        parents_ref: TCounter[Tuple[Sha1Git, Sha1Git]] = Counter(
            reference.revision_get_some_outbound_edges(revision["id"])
        )
        parents: TCounter[Tuple[Sha1Git, Sha1Git]] = Counter(
            archive.revision_get_some_outbound_edges(revision["id"])
        )

        # Check that all the reference outbound edges are included in the other
        # archives's outbound edges
        assert set(parents_ref.items()) <= set(parents.items())


def check_snapshot_get_heads(
    reference: ArchiveInterface, archive: ArchiveInterface, data: Dict[str, List[dict]]
) -> None:
    for snapshot in data["snapshot"]:
        heads_ref: TCounter[Sha1Git] = Counter(
            reference.snapshot_get_heads(snapshot["id"])
        )
        heads: TCounter[Sha1Git] = Counter(archive.snapshot_get_heads(snapshot["id"]))
        assert heads_ref == heads


def get_object_class(object_type: str) -> Type[BaseModel]:
    if object_type == "origin":
        return Origin
    elif object_type == "origin_visit":
        return OriginVisit
    elif object_type == "origin_visit_status":
        return OriginVisitStatus
    elif object_type == "content":
        return Content
    elif object_type == "directory":
        return Directory
    elif object_type == "revision":
        return Revision
    elif object_type == "snapshot":
        return Snapshot
    raise ValueError


def data_to_model(data: Dict[str, List[dict]]) -> Dict[str, List[BaseModel]]:
    model: Dict[str, List[BaseModel]] = {}
    for object_type, objects in data.items():
        for object in objects:
            model.setdefault(object_type, []).append(
                get_object_class(object_type).from_dict(object)
            )
    return model


def add_link(
    edges: Set[
        Tuple[
            Union[CoreSWHID, ExtendedSWHID, str], Union[CoreSWHID, ExtendedSWHID, str]
        ]
    ],
    src_obj: Union[Origin, Snapshot, Revision, Directory, Content],
    dst_id: bytes,
    dst_type: ExtendedObjectType,
) -> None:
    swhid = ExtendedSWHID(object_type=dst_type, object_id=dst_id)
    edges.add((src_obj.swhid(), swhid))


def get_graph_data(
    data: Dict[str, List[dict]]
) -> Tuple[
    List[Union[CoreSWHID, ExtendedSWHID, str]],
    List[
        Tuple[
            Union[CoreSWHID, ExtendedSWHID, str], Union[CoreSWHID, ExtendedSWHID, str]
        ]
    ],
]:
    nodes: Set[Union[CoreSWHID, ExtendedSWHID, str]] = set()
    edges: Set[
        Tuple[
            Union[CoreSWHID, ExtendedSWHID, str], Union[CoreSWHID, ExtendedSWHID, str]
        ]
    ] = set()

    model = data_to_model(data)

    for origin in model["origin"]:
        assert isinstance(origin, Origin)
        nodes.add(origin.swhid())
        for status in model["origin_visit_status"]:
            assert isinstance(status, OriginVisitStatus)
            if status.origin == origin.url and status.snapshot is not None:
                add_link(edges, origin, status.snapshot, ExtendedObjectType.SNAPSHOT)

    for snapshot in model["snapshot"]:
        assert isinstance(snapshot, Snapshot)
        nodes.add(snapshot.swhid())
        for branch in snapshot.branches.values():
            assert isinstance(branch, SnapshotBranch)
            if branch.target_type in [TargetType.RELEASE, TargetType.REVISION]:
                target_type = (
                    ExtendedObjectType.RELEASE
                    if branch.target_type == TargetType.RELEASE
                    else ExtendedObjectType.REVISION
                )
                add_link(edges, snapshot, branch.target, target_type)

    for revision in model["revision"]:
        assert isinstance(revision, Revision)
        nodes.add(revision.swhid())
        # root directory
        add_link(edges, revision, revision.directory, ExtendedObjectType.DIRECTORY)
        # parent
        for parent in revision.parents:
            add_link(edges, revision, parent, ExtendedObjectType.REVISION)

    for directory in model["directory"]:
        assert isinstance(directory, Directory)
        nodes.add(directory.swhid())
        for entry in directory.entries:
            assert isinstance(entry, DirectoryEntry)
            if entry.type == "file":
                target_type = ExtendedObjectType.CONTENT
            elif entry.type == "dir":
                target_type = ExtendedObjectType.DIRECTORY
            elif entry.type == "rev":
                target_type = ExtendedObjectType.REVISION
            else:
                assert False, "unknown directory entry type"
            add_link(edges, directory, entry.target, target_type)

    for content in model["content"]:
        assert isinstance(content, Content)
        nodes.add(content.swhid())

    return list(nodes), list(edges)


@pytest.mark.parametrize(
    "repo",
    ("cmdbts2", "out-of-order", "with-merges"),
)
def test_archive_interface(repo: str, archive: ArchiveInterface) -> None:
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data(repo)
    fill_storage(archive.storage, data)

    # test against ArchiveStorage
    archive_api = ArchiveStorage(archive.storage)
    check_directory_ls(archive, archive_api, data)
    check_revision_get_some_outbound_edges(archive, archive_api, data)
    check_snapshot_get_heads(archive, archive_api, data)

    # test against ArchivePostgreSQL
    assert isinstance(archive.storage, Storage)
    dsn = archive.storage.get_db().conn.dsn
    with BaseDb.connect(dsn).conn as conn:
        BaseDb.adapt_conn(conn)
        archive_direct = ArchivePostgreSQL(conn)
        check_directory_ls(archive, archive_direct, data)
        check_revision_get_some_outbound_edges(archive, archive_direct, data)
        check_snapshot_get_heads(archive, archive_direct, data)

    # test against ArchiveGraph
    nodes, edges = get_graph_data(data)
    graph = NaiveClient(nodes=nodes, edges=edges)
    archive_graph = ArchiveGraph(graph, archive.storage)
    with pytest.raises(NotImplementedError):
        check_directory_ls(archive, archive_graph, data)
    check_revision_get_some_outbound_edges(archive, archive_graph, data)
    check_snapshot_get_heads(archive, archive_graph, data)

    # test against ArchiveMultiplexer
    archive_multiplexed = ArchiveMultiplexed(
        [("noop", ArchiveNoop()), ("graph", archive_graph), ("api", archive_api)]
    )
    check_directory_ls(archive, archive_multiplexed, data)
    check_revision_get_some_outbound_edges(archive, archive_multiplexed, data)
    check_snapshot_get_heads(archive, archive_multiplexed, data)


def test_noop_multiplexer():
    archive = ArchiveMultiplexed([("noop", ArchiveNoop())])

    assert not archive.directory_ls(Sha1Git(b"abcd"))
    assert not archive.revision_get_some_outbound_edges(Sha1Git(b"abcd"))
    assert not archive.snapshot_get_heads(Sha1Git(b"abcd"))
