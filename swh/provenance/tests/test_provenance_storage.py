# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
import inspect
import os
from typing import Any, Dict, Iterable, Optional, Set

import pytest

from swh.model.hashutil import hash_to_bytes
from swh.model.identifiers import origin_identifier
from swh.model.model import Sha1Git
from swh.provenance.interface import (
    EntityType,
    ProvenanceInterface,
    ProvenanceResult,
    ProvenanceStorageInterface,
    RelationData,
    RelationType,
)
from swh.provenance.tests.conftest import load_repo_data, ts2dt


def relation_add_and_compare_result(
    relation: RelationType,
    data: Set[RelationData],
    refstorage: ProvenanceStorageInterface,
    storage: ProvenanceStorageInterface,
    with_path: bool = True,
) -> None:
    assert data
    assert refstorage.relation_add(relation, data) == storage.relation_add(
        relation, data
    )

    assert relation_compare_result(
        refstorage.relation_get(relation, (reldata.src for reldata in data)),
        storage.relation_get(relation, (reldata.src for reldata in data)),
        with_path,
    )
    assert relation_compare_result(
        refstorage.relation_get(
            relation,
            (reldata.dst for reldata in data),
            reverse=True,
        ),
        storage.relation_get(
            relation,
            (reldata.dst for reldata in data),
            reverse=True,
        ),
        with_path,
    )
    assert relation_compare_result(
        refstorage.relation_get_all(relation),
        storage.relation_get_all(relation),
        with_path,
    )


def relation_compare_result(
    expected: Set[RelationData], computed: Set[RelationData], with_path: bool
) -> bool:
    return {
        RelationData(reldata.src, reldata.dst, reldata.path if with_path else None)
        for reldata in expected
    } == computed


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


@pytest.mark.parametrize(
    "repo",
    ("cmdbts2", "out-of-order", "with-merges"),
)
def test_provenance_storage(
    provenance: ProvenanceInterface,
    provenance_storage: ProvenanceStorageInterface,
    repo: str,
) -> None:
    """Tests every ProvenanceStorageInterface implementation against the one provided
    for provenance.storage."""
    # Read data/README.md for more details on how these datasets are generated.
    data = load_repo_data(repo)

    # Assuming provenance.storage has the 'with-path' flavor.
    assert provenance.storage.with_path()

    # Test origin methods.
    # Add all origins present in the current repo to both storages. Then check that the
    # inserted data is the same in both cases.
    org_urls = {
        hash_to_bytes(origin_identifier(org)): org["url"] for org in data["origin"]
    }
    assert org_urls
    assert provenance.storage.origin_set_url(
        org_urls
    ) == provenance_storage.origin_set_url(org_urls)

    assert provenance.storage.origin_get(org_urls) == provenance_storage.origin_get(
        org_urls
    )
    assert provenance.storage.entity_get_all(
        EntityType.ORIGIN
    ) == provenance_storage.entity_get_all(EntityType.ORIGIN)

    # Test content-in-revision relation.
    # Create flat models of every root directory for the revisions in the dataset.
    cnt_in_rev: Set[RelationData] = set()
    for rev in data["revision"]:
        root = next(
            subdir for subdir in data["directory"] if subdir["id"] == rev["directory"]
        )
        cnt_in_rev.update(dircontent(data, rev["id"], root))

    relation_add_and_compare_result(
        RelationType.CNT_EARLY_IN_REV,
        cnt_in_rev,
        provenance.storage,
        provenance_storage,
        provenance_storage.with_path(),
    )

    # Test content-in-directory relation.
    # Create flat models for every directory in the dataset.
    cnt_in_dir: Set[RelationData] = set()
    for dir in data["directory"]:
        cnt_in_dir.update(dircontent(data, dir["id"], dir))

    relation_add_and_compare_result(
        RelationType.CNT_IN_DIR,
        cnt_in_dir,
        provenance.storage,
        provenance_storage,
        provenance_storage.with_path(),
    )

    # Test content-in-directory relation.
    # Add root directories to their correspondent revision in the dataset.
    dir_in_rev = {
        RelationData(rev["directory"], rev["id"], b".") for rev in data["revision"]
    }

    relation_add_and_compare_result(
        RelationType.DIR_IN_REV,
        dir_in_rev,
        provenance.storage,
        provenance_storage,
        provenance_storage.with_path(),
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

    relation_add_and_compare_result(
        RelationType.REV_IN_ORG,
        rev_in_org,
        provenance.storage,
        provenance_storage,
    )

    # Test revision-before-revision relation.
    # For each revision in the data set add an entry for each parent to the relation.
    rev_before_rev = {
        RelationData(parent, rev["id"], None)
        for rev in data["revision"]
        for parent in rev["parents"]
    }

    relation_add_and_compare_result(
        RelationType.REV_BEFORE_REV,
        rev_before_rev,
        provenance.storage,
        provenance_storage,
    )

    # Test content methods.
    # Add all content present in the current repo to both storages, just assigning their
    # creation dates. Then check that the inserted content is the same in both cases.
    cnt_dates = {cnt["sha1_git"]: cnt["ctime"] for cnt in data["content"]}
    assert cnt_dates
    assert provenance.storage.content_set_date(
        cnt_dates
    ) == provenance_storage.content_set_date(cnt_dates)

    assert provenance.storage.content_get(cnt_dates) == provenance_storage.content_get(
        cnt_dates
    )
    assert provenance.storage.entity_get_all(
        EntityType.CONTENT
    ) == provenance_storage.entity_get_all(EntityType.CONTENT)

    # Test directory methods.
    # Of all directories present in the current repo, only assign a date to those
    # containing blobs (picking the max date among the available ones). Then check that
    # the inserted data is the same in both storages.
    def getmaxdate(
        dir: Dict[str, Any], cnt_dates: Dict[Sha1Git, datetime]
    ) -> Optional[datetime]:
        dates = [
            cnt_dates[entry["target"]]
            for entry in dir["entries"]
            if entry["type"] == "file"
        ]
        return max(dates) if dates else None

    dir_dates = {dir["id"]: getmaxdate(dir, cnt_dates) for dir in data["directory"]}
    assert dir_dates
    assert provenance.storage.directory_set_date(
        {sha1: date for sha1, date in dir_dates.items() if date is not None}
    ) == provenance_storage.directory_set_date(
        {sha1: date for sha1, date in dir_dates.items() if date is not None}
    )
    assert provenance.storage.directory_get(
        dir_dates
    ) == provenance_storage.directory_get(dir_dates)
    assert provenance.storage.entity_get_all(
        EntityType.DIRECTORY
    ) == provenance_storage.entity_get_all(EntityType.DIRECTORY)

    # Test revision methods.
    # Add all revisions present in the current repo to both storages, assigning their
    # dataes and an arbitrary origin to each one. Then check that the inserted data is
    # the same in both cases.
    rev_dates = {rev["id"]: ts2dt(rev["date"]) for rev in data["revision"]}
    assert rev_dates
    assert provenance.storage.revision_set_date(
        rev_dates
    ) == provenance_storage.revision_set_date(rev_dates)

    rev_origins = {
        rev["id"]: next(iter(org_urls))  # any arbitrary origin will do
        for rev in data["revision"]
    }
    assert rev_origins
    assert provenance.storage.revision_set_origin(
        rev_origins
    ) == provenance_storage.revision_set_origin(rev_origins)

    assert provenance.storage.revision_get(
        rev_dates
    ) == provenance_storage.revision_get(rev_dates)
    assert provenance.storage.entity_get_all(
        EntityType.REVISION
    ) == provenance_storage.entity_get_all(EntityType.REVISION)

    # Test location_get.
    if provenance_storage.with_path():
        assert provenance.storage.location_get() == provenance_storage.location_get()

    # Test content_find_first and content_find_all.
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

    for cnt in cnt_dates:
        assert adapt_result(
            provenance.storage.content_find_first(cnt), provenance_storage.with_path()
        ) == provenance_storage.content_find_first(cnt)

        assert {
            adapt_result(occur, provenance_storage.with_path())
            for occur in provenance.storage.content_find_all(cnt)
        } == set(provenance_storage.content_find_all(cnt))


def test_types(provenance: ProvenanceInterface) -> None:
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
            concrete_meth = getattr(provenance.storage, meth_name)
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
    assert isinstance(provenance.storage, ProvenanceStorageInterface)
