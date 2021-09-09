# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone
import inspect
import os
from typing import Any, Dict, Iterable, Optional, Set

from swh.model.hashutil import hash_to_bytes
from swh.model.identifiers import origin_identifier
from swh.model.model import Sha1Git
from swh.provenance.archive import ArchiveInterface
from swh.provenance.interface import (
    EntityType,
    ProvenanceInterface,
    ProvenanceResult,
    ProvenanceStorageInterface,
    RelationData,
    RelationType,
    RevisionData,
)
from swh.provenance.model import OriginEntry, RevisionEntry
from swh.provenance.mongo.backend import ProvenanceStorageMongoDb
from swh.provenance.origin import origin_add
from swh.provenance.provenance import Provenance
from swh.provenance.revision import revision_add
from swh.provenance.tests.conftest import fill_storage, load_repo_data, ts2dt


def test_provenance_storage_content(
    provenance_storage: ProvenanceStorageInterface,
) -> None:
    """Tests content methods for every `ProvenanceStorageInterface` implementation."""

    # Read data/README.md for more details on how these datasets are generated.
    data = load_repo_data("cmdbts2")

    # Add all content present in the current repo to the storage, just assigning their
    # creation dates. Then check that the returned results when querying are the same.
    cnts = {cnt["sha1_git"] for idx, cnt in enumerate(data["content"]) if idx % 2 == 0}
    cnt_dates = {
        cnt["sha1_git"]: cnt["ctime"]
        for idx, cnt in enumerate(data["content"])
        if idx % 2 == 1
    }
    assert cnts or cnt_dates
    assert provenance_storage.content_add(cnts)
    assert provenance_storage.content_add(cnt_dates)
    assert provenance_storage.content_get(set(cnt_dates.keys())) == cnt_dates
    assert provenance_storage.entity_get_all(EntityType.CONTENT) == cnts | set(
        cnt_dates.keys()
    )


def test_provenance_storage_directory(
    provenance_storage: ProvenanceStorageInterface,
) -> None:
    """Tests directory methods for every `ProvenanceStorageInterface` implementation."""

    # Read data/README.md for more details on how these datasets are generated.
    data = load_repo_data("cmdbts2")

    # Of all directories present in the current repo, only assign a date to those
    # containing blobs (picking the max date among the available ones). Then check that
    # the returned results when querying are the same.
    def getmaxdate(
        directory: Dict[str, Any], contents: Iterable[Dict[str, Any]]
    ) -> Optional[datetime]:
        dates = [
            content["ctime"]
            for entry in directory["entries"]
            for content in contents
            if entry["type"] == "file" and entry["target"] == content["sha1_git"]
        ]
        return max(dates) if dates else None

    dirs = {
        dir["id"]
        for dir in data["directory"]
        if getmaxdate(dir, data["content"]) is None
    }
    dir_dates = {
        dir["id"]: getmaxdate(dir, data["content"])
        for dir in data["directory"]
        if getmaxdate(dir, data["content"]) is not None
    }
    assert dirs
    assert provenance_storage.directory_add(dirs)
    assert provenance_storage.directory_add(dir_dates)
    assert provenance_storage.directory_get(set(dir_dates.keys())) == dir_dates
    assert provenance_storage.entity_get_all(EntityType.DIRECTORY) == dirs | set(
        dir_dates.keys()
    )


def test_provenance_storage_location(
    provenance_storage: ProvenanceStorageInterface,
) -> None:
    """Tests location methods for every `ProvenanceStorageInterface` implementation."""

    # Read data/README.md for more details on how these datasets are generated.
    data = load_repo_data("cmdbts2")

    # Add all names of entries present in the directories of the current repo as paths
    # to the storage. Then check that the returned results when querying are the same.
    paths = {entry["name"] for dir in data["directory"] for entry in dir["entries"]}
    assert provenance_storage.location_add(paths)

    if isinstance(provenance_storage, ProvenanceStorageMongoDb):
        # TODO: remove this when `location_add` is properly implemented for MongoDb.
        return

    if provenance_storage.with_path():
        assert provenance_storage.location_get_all() == paths
    else:
        assert provenance_storage.location_get_all() == set()


def test_provenance_storage_origin(
    provenance_storage: ProvenanceStorageInterface,
) -> None:
    """Tests origin methods for every `ProvenanceStorageInterface` implementation."""

    # Read data/README.md for more details on how these datasets are generated.
    data = load_repo_data("cmdbts2")

    # Test origin methods.
    # Add all origins present in the current repo to the storage. Then check that the
    # returned results when querying are the same.
    orgs = {hash_to_bytes(origin_identifier(org)): org["url"] for org in data["origin"]}
    assert orgs
    assert provenance_storage.origin_add(orgs)
    assert provenance_storage.origin_get(set(orgs.keys())) == orgs
    assert provenance_storage.entity_get_all(EntityType.ORIGIN) == set(orgs.keys())


def test_provenance_storage_revision(
    provenance_storage: ProvenanceStorageInterface,
) -> None:
    """Tests revision methods for every `ProvenanceStorageInterface` implementation."""

    # Read data/README.md for more details on how these datasets are generated.
    data = load_repo_data("cmdbts2")

    # Test revision methods.
    # Add all revisions present in the current repo to the storage, assigning their
    # dates and an arbitrary origin to each one. Then check that the returned results
    # when querying are the same.
    origin = next(iter(data["origin"]))
    origin_sha1 = hash_to_bytes(origin_identifier(origin))
    # Origin must be inserted in advance.
    assert provenance_storage.origin_add({origin_sha1: origin["url"]})

    revs = {rev["id"] for idx, rev in enumerate(data["revision"]) if idx % 6 == 0}
    rev_data = {
        rev["id"]: RevisionData(
            date=ts2dt(rev["date"]) if idx % 2 != 0 else None,
            origin=origin_sha1 if idx % 3 != 0 else None,
        )
        for idx, rev in enumerate(data["revision"])
        if idx % 6 != 0
    }
    assert revs
    assert provenance_storage.revision_add(revs)
    assert provenance_storage.revision_add(rev_data)
    assert provenance_storage.revision_get(set(rev_data.keys())) == rev_data
    assert provenance_storage.entity_get_all(EntityType.REVISION) == revs | set(
        rev_data.keys()
    )


def dircontent(
    data: Dict[str, Any],
    ref: Sha1Git,
    dir: Dict[str, Any],
    prefix: bytes = b"",
) -> Iterable[RelationData]:
    content = {
        RelationData(entry["target"], ref, os.path.join(prefix, entry["name"]))
        for entry in dir["entries"]
        if entry["type"] == "file"
    }
    for entry in dir["entries"]:
        if entry["type"] == "dir":
            child = next(
                subdir
                for subdir in data["directory"]
                if subdir["id"] == entry["target"]
            )
            content.update(
                dircontent(data, ref, child, os.path.join(prefix, entry["name"]))
            )
    return content


def entity_add(
    storage: ProvenanceStorageInterface, entity: EntityType, ids: Set[Sha1Git]
) -> bool:
    if entity == EntityType.CONTENT:
        return storage.content_add({sha1: None for sha1 in ids})
    elif entity == EntityType.DIRECTORY:
        return storage.directory_add({sha1: None for sha1 in ids})
    else:  # entity == EntityType.REVISION:
        return storage.revision_add(
            {sha1: RevisionData(date=None, origin=None) for sha1 in ids}
        )


def relation_add_and_compare_result(
    storage: ProvenanceStorageInterface, relation: RelationType, data: Set[RelationData]
) -> None:
    # Source, destinations and locations must be added in advance.
    src, *_, dst = relation.value.split("_")
    if src != "origin":
        assert entity_add(storage, EntityType(src), {entry.src for entry in data})
    if dst != "origin":
        assert entity_add(storage, EntityType(dst), {entry.dst for entry in data})
    if storage.with_path():
        assert storage.location_add(
            {entry.path for entry in data if entry.path is not None}
        )

    assert data
    assert storage.relation_add(relation, data)

    for row in data:
        assert relation_compare_result(
            storage.relation_get(relation, [row.src]),
            {entry for entry in data if entry.src == row.src},
            storage.with_path(),
        )
        assert relation_compare_result(
            storage.relation_get(
                relation,
                [row.dst],
                reverse=True,
            ),
            {entry for entry in data if entry.dst == row.dst},
            storage.with_path(),
        )

    assert relation_compare_result(
        storage.relation_get_all(relation), data, storage.with_path()
    )


def relation_compare_result(
    computed: Set[RelationData], expected: Set[RelationData], with_path: bool
) -> bool:
    return {
        RelationData(row.src, row.dst, row.path if with_path else None)
        for row in expected
    } == computed


def test_provenance_storage_relation(
    provenance_storage: ProvenanceStorageInterface,
) -> None:
    """Tests relation methods for every `ProvenanceStorageInterface` implementation."""

    # Read data/README.md for more details on how these datasets are generated.
    data = load_repo_data("cmdbts2")

    # Test content-in-revision relation.
    # Create flat models of every root directory for the revisions in the dataset.
    cnt_in_rev: Set[RelationData] = set()
    for rev in data["revision"]:
        root = next(
            subdir for subdir in data["directory"] if subdir["id"] == rev["directory"]
        )
        cnt_in_rev.update(dircontent(data, rev["id"], root))
    relation_add_and_compare_result(
        provenance_storage, RelationType.CNT_EARLY_IN_REV, cnt_in_rev
    )

    # Test content-in-directory relation.
    # Create flat models for every directory in the dataset.
    cnt_in_dir: Set[RelationData] = set()
    for dir in data["directory"]:
        cnt_in_dir.update(dircontent(data, dir["id"], dir))
    relation_add_and_compare_result(
        provenance_storage, RelationType.CNT_IN_DIR, cnt_in_dir
    )

    # Test content-in-directory relation.
    # Add root directories to their correspondent revision in the dataset.
    dir_in_rev = {
        RelationData(rev["directory"], rev["id"], b".") for rev in data["revision"]
    }
    relation_add_and_compare_result(
        provenance_storage, RelationType.DIR_IN_REV, dir_in_rev
    )

    # Test revision-in-origin relation.
    # Add all revisions that are head of some snapshot branch to the corresponding
    # origin.
    rev_in_org = {
        RelationData(
            branch["target"],
            hash_to_bytes(origin_identifier({"url": status["origin"]})),
            None,
        )
        for status in data["origin_visit_status"]
        if status["snapshot"] is not None
        for snapshot in data["snapshot"]
        if snapshot["id"] == status["snapshot"]
        for _, branch in snapshot["branches"].items()
        if branch["target_type"] == "revision"
    }
    # Origins must be inserted in advance (cannot be done by `entity_add` inside
    # `relation_add_and_compare_result`).
    orgs = {
        hash_to_bytes(origin_identifier(origin)): origin["url"]
        for origin in data["origin"]
    }
    assert provenance_storage.origin_add(orgs)

    relation_add_and_compare_result(
        provenance_storage, RelationType.REV_IN_ORG, rev_in_org
    )

    # Test revision-before-revision relation.
    # For each revision in the data set add an entry for each parent to the relation.
    rev_before_rev = {
        RelationData(parent, rev["id"], None)
        for rev in data["revision"]
        for parent in rev["parents"]
    }
    relation_add_and_compare_result(
        provenance_storage, RelationType.REV_BEFORE_REV, rev_before_rev
    )


def test_provenance_storage_find(
    archive: ArchiveInterface,
    provenance: ProvenanceInterface,
    provenance_storage: ProvenanceStorageInterface,
) -> None:
    """Tests `content_find_first` and `content_find_all` methods for every
    `ProvenanceStorageInterface` implementation.
    """

    # Read data/README.md for more details on how these datasets are generated.
    data = load_repo_data("cmdbts2")
    fill_storage(archive.storage, data)

    # Test content_find_first and content_find_all, first only executing the
    # revision-content algorithm, then adding the origin-revision layer.
    def adapt_result(
        result: Optional[ProvenanceResult], with_path: bool
    ) -> Optional[ProvenanceResult]:
        if result is not None:
            return ProvenanceResult(
                result.content,
                result.revision,
                result.date,
                result.origin,
                result.path if with_path else b"",
            )
        return result

    # Execute the revision-content algorithm on both storages.
    revisions = [
        RevisionEntry(id=rev["id"], date=ts2dt(rev["date"]), root=rev["directory"])
        for rev in data["revision"]
    ]
    revision_add(provenance, archive, revisions)
    revision_add(Provenance(provenance_storage), archive, revisions)

    assert adapt_result(
        ProvenanceResult(
            content=hash_to_bytes("20329687bb9c1231a7e05afe86160343ad49b494"),
            revision=hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            date=datetime.fromtimestamp(1000000000.0, timezone.utc),
            origin=None,
            path=b"A/B/C/a",
        ),
        provenance_storage.with_path(),
    ) == provenance_storage.content_find_first(
        hash_to_bytes("20329687bb9c1231a7e05afe86160343ad49b494")
    )

    for cnt in {cnt["sha1_git"] for cnt in data["content"]}:
        assert adapt_result(
            provenance.storage.content_find_first(cnt), provenance_storage.with_path()
        ) == provenance_storage.content_find_first(cnt)
        assert {
            adapt_result(occur, provenance_storage.with_path())
            for occur in provenance.storage.content_find_all(cnt)
        } == set(provenance_storage.content_find_all(cnt))

    # Execute the origin-revision algorithm on both storages.
    origins = [
        OriginEntry(url=sta["origin"], snapshot=sta["snapshot"])
        for sta in data["origin_visit_status"]
        if sta["snapshot"] is not None
    ]
    origin_add(provenance, archive, origins)
    origin_add(Provenance(provenance_storage), archive, origins)

    assert adapt_result(
        ProvenanceResult(
            content=hash_to_bytes("20329687bb9c1231a7e05afe86160343ad49b494"),
            revision=hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            date=datetime.fromtimestamp(1000000000.0, timezone.utc),
            origin="https://cmdbts2",
            path=b"A/B/C/a",
        ),
        provenance_storage.with_path(),
    ) == provenance_storage.content_find_first(
        hash_to_bytes("20329687bb9c1231a7e05afe86160343ad49b494")
    )

    for cnt in {cnt["sha1_git"] for cnt in data["content"]}:
        assert adapt_result(
            provenance.storage.content_find_first(cnt), provenance_storage.with_path()
        ) == provenance_storage.content_find_first(cnt)
        assert {
            adapt_result(occur, provenance_storage.with_path())
            for occur in provenance.storage.content_find_all(cnt)
        } == set(provenance_storage.content_find_all(cnt))


def test_types(provenance_storage: ProvenanceInterface) -> None:
    """Checks all methods of ProvenanceStorageInterface are implemented by this
    backend, and that they have the same signature."""
    # Create an instance of the protocol (which cannot be instantiated
    # directly, so this creates a subclass, then instantiates it)
    interface = type("_", (ProvenanceStorageInterface,), {})()

    assert "content_find_first" in dir(interface)

    missing_methods = []

    for meth_name in dir(interface):
        if meth_name.startswith("_"):
            continue
        interface_meth = getattr(interface, meth_name)
        try:
            concrete_meth = getattr(provenance_storage, meth_name)
        except AttributeError:
            if not getattr(interface_meth, "deprecated_endpoint", False):
                # The backend is missing a (non-deprecated) endpoint
                missing_methods.append(meth_name)
            continue

        expected_signature = inspect.signature(interface_meth)
        actual_signature = inspect.signature(concrete_meth)

        assert expected_signature == actual_signature, meth_name

    assert missing_methods == []

    # If all the assertions above succeed, then this one should too.
    # But there's no harm in double-checking.
    # And we could replace the assertions above by this one, but unlike
    # the assertions above, it doesn't explain what is missing.
    assert isinstance(provenance_storage, ProvenanceStorageInterface)
