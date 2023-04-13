# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta, timezone
import hashlib
import inspect
import os
from typing import Any, Dict, Iterable, Optional, Set, Tuple

import pytest

from swh.model.hashutil import hash_to_bytes
from swh.model.model import Origin, Sha1Git
from swh.provenance.algos.origin import origin_add
from swh.provenance.algos.revision import revision_add
from swh.provenance.archive import ArchiveInterface
from swh.provenance.interface import ProvenanceInterface
from swh.provenance.model import OriginEntry, RevisionEntry
from swh.provenance.provenance import Provenance
from swh.provenance.storage.interface import (
    DirectoryData,
    EntityType,
    ProvenanceResult,
    ProvenanceStorageInterface,
    RelationData,
    RelationType,
    RevisionData,
)

from .utils import fill_storage, load_repo_data, ts2dt

UTC = timezone.utc


class TestProvenanceStorage:
    def test_provenance_storage_content(
        self,
        provenance_storage: ProvenanceStorageInterface,
    ) -> None:
        """Tests content methods for every `ProvenanceStorageInterface` implementation."""

        # Read data/README.md for more details on how these datasets are generated.
        data = load_repo_data("cmdbts2")

        # Add all content present in the current repo to the storage, just assigning their
        # creation dates. Then check that the returned results when querying are the same.
        cnt_dates = {cnt["sha1_git"]: cnt["ctime"] for cnt in data["content"]}

        expected_dates = {
            cnt["sha1_git"]: cnt["ctime"].astimezone(UTC) for cnt in data["content"]
        }
        assert provenance_storage.content_add(cnt_dates)
        assert provenance_storage.content_get(set(cnt_dates.keys())) == expected_dates
        assert provenance_storage.entity_get_all(EntityType.CONTENT) == set(
            cnt_dates.keys()
        )

    def test_provenance_storage_content_invalid_dates(
        self,
        provenance_storage: ProvenanceStorageInterface,
    ) -> None:
        """Tests content methods for every `ProvenanceStorageInterface` implementation."""

        # Read data/README.md for more details on how these datasets are generated.
        data = load_repo_data("cmdbts2")

        # Add all content present in the current repo to the storage, just assigning their
        # creation dates. Then check that the returned results when querying are the same.
        cnt_dates = {
            cnt["sha1_git"]: cnt["ctime"].replace(
                tzinfo=timezone(-timedelta(hours=23, minutes=59, seconds=59))
            )
            for cnt in data["content"]
        }
        expected_dates = {
            sha1_git: date.astimezone(UTC) for sha1_git, date in cnt_dates.items()
        }

        assert provenance_storage.content_add(cnt_dates)
        assert provenance_storage.content_get(set(cnt_dates.keys())) == expected_dates
        assert provenance_storage.entity_get_all(EntityType.CONTENT) == set(
            cnt_dates.keys()
        )

        # Add all content present in the current repo to the storage, just assigning their
        # creation dates. Then check that the returned results when querying are the same.
        cnt_dates = {
            cnt["sha1_git"]: cnt["ctime"].replace(
                tzinfo=timezone(timedelta(hours=23, minutes=59, seconds=59))
            )
            for cnt in data["content"]
        }
        expected_dates = {
            sha1_git: date.astimezone(UTC) for sha1_git, date in cnt_dates.items()
        }

        assert provenance_storage.content_add(cnt_dates)
        assert provenance_storage.content_get(set(cnt_dates.keys())) == expected_dates
        assert provenance_storage.entity_get_all(EntityType.CONTENT) == set(
            cnt_dates.keys()
        )

    def test_provenance_storage_directory(
        self,
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

        flat_values = (False, True)
        dir_dates = {}
        for idx, dir in enumerate(data["directory"]):
            date = getmaxdate(dir, data["content"])
            if date is not None:
                dir_dates[dir["id"]] = DirectoryData(
                    date=date, flat=flat_values[idx % 2]
                )
        assert provenance_storage.directory_add(dir_dates)
        assert provenance_storage.directory_get(set(dir_dates.keys())) == dir_dates
        assert provenance_storage.entity_get_all(EntityType.DIRECTORY) == set(
            dir_dates.keys()
        )

    def test_provenance_storage_location(
        self,
        provenance_storage: ProvenanceStorageInterface,
    ) -> None:
        """Tests location methods for every `ProvenanceStorageInterface` implementation."""

        # Read data/README.md for more details on how these datasets are generated.
        data = load_repo_data("cmdbts2")

        # Add all names of entries present in the directories of the current repo as paths
        # to the storage. Then check that the returned results when querying are the same.
        paths = {
            hashlib.sha1(entry["name"]).digest(): entry["name"]
            for dir in data["directory"]
            for entry in dir["entries"]
        }
        assert provenance_storage.location_add(paths)
        assert provenance_storage.location_get_all() == paths

    @pytest.mark.origin_layer
    def test_provenance_storage_origin(
        self,
        provenance_storage: ProvenanceStorageInterface,
    ) -> None:
        """Tests origin methods for every `ProvenanceStorageInterface` implementation."""

        # Read data/README.md for more details on how these datasets are generated.
        data = load_repo_data("cmdbts2")

        # Test origin methods.
        # Add all origins present in the current repo to the storage. Then check that the
        # returned results when querying are the same.
        orgs = {Origin(url=org["url"]).id: org["url"] for org in data["origin"]}
        assert orgs
        assert provenance_storage.origin_add(orgs)
        assert provenance_storage.origin_get(set(orgs.keys())) == orgs
        assert provenance_storage.entity_get_all(EntityType.ORIGIN) == set(orgs.keys())

    def test_provenance_storage_revision(
        self,
        provenance_storage: ProvenanceStorageInterface,
    ) -> None:
        """Tests revision methods for every `ProvenanceStorageInterface` implementation."""

        # Read data/README.md for more details on how these datasets are generated.
        data = load_repo_data("cmdbts2")

        # Test revision methods.
        # Add all revisions present in the current repo to the storage, assigning their
        # dates and an arbitrary origin to each one. Then check that the returned results
        # when querying are the same.
        origin = Origin(url=next(iter(data["origin"]))["url"])
        # Origin must be inserted in advance.
        assert provenance_storage.origin_add({origin.id: origin.url})

        revs = {rev["id"] for idx, rev in enumerate(data["revision"])}
        rev_data = {
            rev["id"]: RevisionData(
                date=ts2dt(rev["date"]) if idx % 2 != 0 else None,
                origin=origin.id if idx % 3 != 0 else None,
            )
            for idx, rev in enumerate(data["revision"])
        }
        assert revs
        assert provenance_storage.revision_add(rev_data)
        assert provenance_storage.revision_get(set(rev_data.keys())) == {
            k: v
            for (k, v) in rev_data.items()
            if v.date is not None or v.origin is not None
        }
        assert provenance_storage.entity_get_all(EntityType.REVISION) == set(rev_data)

    def test_provenance_storage_relation_revision_layer(
        self,
        provenance_storage: ProvenanceStorageInterface,
    ) -> None:
        """Tests relation methods for every `ProvenanceStorageInterface` implementation."""

        # Read data/README.md for more details on how these datasets are generated.
        data = load_repo_data("cmdbts2")

        # Test content-in-revision relation.
        # Create flat models of every root directory for the revisions in the dataset.
        cnt_in_rev: Dict[Sha1Git, Set[RelationData]] = {}
        for rev in data["revision"]:
            root = next(
                subdir
                for subdir in data["directory"]
                if subdir["id"] == rev["directory"]
            )
            for cnt, rel in dircontent(
                data=data, ref=rev["id"], dir=root, ref_date=ts2dt(rev["date"])
            ):
                cnt_in_rev.setdefault(cnt, set()).add(rel)
        relation_add_and_compare_result(
            provenance_storage, RelationType.CNT_EARLY_IN_REV, cnt_in_rev
        )

        # Test content-in-directory relation.
        # Create flat models for every directory in the dataset.
        cnt_in_dir: Dict[Sha1Git, Set[RelationData]] = {}
        for dir in data["directory"]:
            for cnt, rel in dircontent(data=data, ref=dir["id"], dir=dir):
                cnt_in_dir.setdefault(cnt, set()).add(rel)
        relation_add_and_compare_result(
            provenance_storage, RelationType.CNT_IN_DIR, cnt_in_dir
        )

        # Test content-in-directory relation.
        # Add root directories to their correspondent revision in the dataset.
        dir_in_rev: Dict[Sha1Git, Set[RelationData]] = {}
        for rev in data["revision"]:
            dir_in_rev.setdefault(rev["directory"], set()).add(
                RelationData(dst=rev["id"], path=b".")
            )
        relation_add_and_compare_result(
            provenance_storage, RelationType.DIR_IN_REV, dir_in_rev
        )

    @pytest.mark.origin_layer
    def test_provenance_storage_relation_origin_layer(
        self,
        provenance_storage: ProvenanceStorageInterface,
    ) -> None:
        """Tests relation methods for every `ProvenanceStorageInterface` implementation."""

        # Read data/README.md for more details on how these datasets are generated.
        data = load_repo_data("cmdbts2")

        # Test revision-in-origin relation.
        # Origins must be inserted in advance (cannot be done by `entity_add` inside
        # `relation_add_and_compare_result`).
        orgs = {Origin(url=org["url"]).id: org["url"] for org in data["origin"]}
        assert provenance_storage.origin_add(orgs)
        # Add all revisions that are head of some snapshot branch to the corresponding
        # origin.
        rev_in_org: Dict[Sha1Git, Set[RelationData]] = {}
        for status in data["origin_visit_status"]:
            if status["snapshot"] is not None:
                for snapshot in data["snapshot"]:
                    if snapshot["id"] == status["snapshot"]:
                        for branch in snapshot["branches"].values():
                            if branch["target_type"] == "revision":
                                rev_in_org.setdefault(branch["target"], set()).add(
                                    RelationData(
                                        dst=Origin(url=status["origin"]).id,
                                        path=None,
                                    )
                                )
        relation_add_and_compare_result(
            provenance_storage, RelationType.REV_IN_ORG, rev_in_org
        )

        # Test revision-before-revision relation.
        # For each revision in the data set add an entry for each parent to the relation.
        rev_before_rev: Dict[Sha1Git, Set[RelationData]] = {}
        for rev in data["revision"]:
            for parent in rev["parents"]:
                rev_before_rev.setdefault(parent, set()).add(
                    RelationData(dst=rev["id"], path=None)
                )
        relation_add_and_compare_result(
            provenance_storage, RelationType.REV_BEFORE_REV, rev_before_rev
        )

    def test_provenance_storage_find_revision_layer(
        self,
        provenance: ProvenanceInterface,
        provenance_storage: ProvenanceStorageInterface,
        archive: ArchiveInterface,
    ) -> None:
        """Tests `content_find_first` and `content_find_all` methods for every
        `ProvenanceStorageInterface` implementation.
        """

        # Read data/README.md for more details on how these datasets are generated.
        data = load_repo_data("cmdbts2")
        fill_storage(archive.storage, data)

        # Test content_find_first and content_find_all, first only executing the
        # revision-content algorithm, then adding the origin-revision layer.

        # Execute the revision-content algorithm on both storages.
        revisions = [
            RevisionEntry(id=rev["id"], date=ts2dt(rev["date"]), root=rev["directory"])
            for rev in data["revision"]
        ]
        revision_add(provenance, archive, revisions)
        revision_add(Provenance(provenance_storage), archive, revisions)

        assert ProvenanceResult(
            content=hash_to_bytes("20329687bb9c1231a7e05afe86160343ad49b494"),
            revision=hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            date=datetime.fromtimestamp(1000000000.0, timezone.utc),
            origin=None,
            path=b"A/B/C/a",
        ) == provenance_storage.content_find_first(
            hash_to_bytes("20329687bb9c1231a7e05afe86160343ad49b494")
        )

        for cnt in {cnt["sha1_git"] for cnt in data["content"]}:
            assert provenance.storage.content_find_first(
                cnt
            ) == provenance_storage.content_find_first(cnt)
            assert set(provenance.storage.content_find_all(cnt)) == set(
                provenance_storage.content_find_all(cnt)
            )

    @pytest.mark.origin_layer
    def test_provenance_storage_find_origin_layer(
        self,
        provenance: ProvenanceInterface,
        provenance_storage: ProvenanceStorageInterface,
        archive: ArchiveInterface,
    ) -> None:
        """Tests `content_find_first` and `content_find_all` methods for every
        `ProvenanceStorageInterface` implementation.
        """

        # Read data/README.md for more details on how these datasets are generated.
        data = load_repo_data("cmdbts2")
        fill_storage(archive.storage, data)

        # Execute the revision-content algorithm on both storages.
        revisions = [
            RevisionEntry(id=rev["id"], date=ts2dt(rev["date"]), root=rev["directory"])
            for rev in data["revision"]
        ]
        revision_add(provenance, archive, revisions)
        revision_add(Provenance(provenance_storage), archive, revisions)

        # Test content_find_first and content_find_all, first only executing the
        # revision-content algorithm, then adding the origin-revision layer.

        # Execute the origin-revision algorithm on both storages.
        origins = [
            OriginEntry(url=sta["origin"], snapshot=sta["snapshot"])
            for sta in data["origin_visit_status"]
            if sta["snapshot"] is not None
        ]
        origin_add(provenance, archive, origins)
        origin_add(Provenance(provenance_storage), archive, origins)

        assert ProvenanceResult(
            content=hash_to_bytes("20329687bb9c1231a7e05afe86160343ad49b494"),
            revision=hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            date=datetime.fromtimestamp(1000000000.0, timezone.utc),
            origin="https://cmdbts2",
            path=b"A/B/C/a",
        ) == provenance_storage.content_find_first(
            hash_to_bytes("20329687bb9c1231a7e05afe86160343ad49b494")
        )

        for cnt in {cnt["sha1_git"] for cnt in data["content"]}:
            assert provenance.storage.content_find_first(
                cnt
            ) == provenance_storage.content_find_first(cnt)
            assert set(provenance.storage.content_find_all(cnt)) == set(
                provenance_storage.content_find_all(cnt)
            )

    def test_types(self, provenance_storage: ProvenanceStorageInterface) -> None:
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


def dircontent(
    data: Dict[str, Any],
    ref: Sha1Git,
    dir: Dict[str, Any],
    prefix: bytes = b"",
    ref_date: Optional[datetime] = None,
) -> Iterable[Tuple[Sha1Git, RelationData]]:
    content = {
        (
            entry["target"],
            RelationData(
                dst=ref, path=os.path.join(prefix, entry["name"]), dst_date=ref_date
            ),
        )
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
                dircontent(
                    data=data,
                    ref=ref,
                    dir=child,
                    prefix=os.path.join(prefix, entry["name"]),
                    ref_date=ref_date,
                )
            )
    return content


def entity_add(
    storage: ProvenanceStorageInterface, entity: EntityType, ids: Set[Sha1Git]
) -> bool:
    now = datetime.now(tz=timezone.utc)
    if entity == EntityType.CONTENT:
        return storage.content_add({sha1: now for sha1 in ids})
    elif entity == EntityType.DIRECTORY:
        return storage.directory_add(
            {sha1: DirectoryData(date=now, flat=False) for sha1 in ids}
        )
    else:  # entity == EntityType.REVISION:
        return storage.revision_add(
            {sha1: RevisionData(date=None, origin=None) for sha1 in ids}
        )


def relation_add_and_compare_result(
    storage: ProvenanceStorageInterface,
    relation: RelationType,
    data: Dict[Sha1Git, Set[RelationData]],
) -> None:
    # Source, destinations and locations must be added in advance.
    src, *_, dst = relation.value.split("_")
    srcs = {sha1 for sha1 in data}
    if src != "origin":
        assert entity_add(storage, EntityType(src), srcs)
    dsts = {rel.dst for rels in data.values() for rel in rels}
    if dst != "origin":
        assert entity_add(storage, EntityType(dst), dsts)
    assert storage.location_add(
        {
            hashlib.sha1(rel.path).digest(): rel.path
            for rels in data.values()
            for rel in rels
            if rel.path is not None
        }
    )

    assert data
    assert storage.relation_add(relation, data)

    for src_sha1 in srcs:
        relation_compare_result(
            storage.relation_get(relation, [src_sha1]),
            {src_sha1: data[src_sha1]},
        )
    for dst_sha1 in dsts:
        relation_compare_result(
            storage.relation_get(relation, [dst_sha1], reverse=True),
            {
                src_sha1: {
                    RelationData(dst=dst_sha1, path=rel.path)
                    for rel in rels
                    if dst_sha1 == rel.dst
                }
                for src_sha1, rels in data.items()
                if dst_sha1 in {rel.dst for rel in rels}
            },
        )
    relation_compare_result(
        storage.relation_get_all(relation),
        data,
    )


def relation_compare_result(
    computed: Dict[Sha1Git, Set[RelationData]],
    expected: Dict[Sha1Git, Set[RelationData]],
) -> None:
    assert {
        src_sha1: {RelationData(dst=rel.dst, path=rel.path) for rel in rels}
        for src_sha1, rels in expected.items()
    } == computed
