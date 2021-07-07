# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, List, Optional, Set, Tuple

import pytest

from swh.model.hashutil import hash_to_bytes
from swh.provenance.archive import ArchiveInterface
from swh.provenance.interface import EntityType, ProvenanceInterface, RelationType
from swh.provenance.model import RevisionEntry
from swh.provenance.revision import revision_add
from swh.provenance.tests.conftest import (
    fill_storage,
    get_datafile,
    load_repo_data,
    synthetic_result,
)
from swh.provenance.tests.test_provenance_db import ts2dt
from swh.storage.postgresql.storage import Storage


@pytest.mark.parametrize(
    "repo, lower, mindepth",
    (
        ("cmdbts2", True, 1),
        ("cmdbts2", False, 1),
        ("cmdbts2", True, 2),
        ("cmdbts2", False, 2),
        ("out-of-order", True, 1),
    ),
)
def test_provenance_heuristics(
    provenance: ProvenanceInterface,
    swh_storage: Storage,
    archive: ArchiveInterface,
    repo: str,
    lower: bool,
    mindepth: int,
) -> None:
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data(repo)
    fill_storage(swh_storage, data)
    syntheticfile = get_datafile(
        f"synthetic_{repo}_{'lower' if lower else 'upper'}_{mindepth}.txt"
    )

    revisions = {rev["id"]: rev for rev in data["revision"]}

    rows: Dict[str, Set[Any]] = {
        "content": set(),
        "content_in_directory": set(),
        "content_in_revision": set(),
        "directory": set(),
        "directory_in_revision": set(),
        "location": set(),
        "revision": set(),
    }

    def maybe_path(path: str) -> Optional[bytes]:
        if provenance.storage.with_path():
            return path.encode("utf-8")
        return None

    for synth_rev in synthetic_result(syntheticfile):
        revision = revisions[synth_rev["sha1"]]
        entry = RevisionEntry(
            id=revision["id"],
            date=ts2dt(revision["date"]),
            root=revision["directory"],
        )
        revision_add(provenance, archive, [entry], lower=lower, mindepth=mindepth)

        # each "entry" in the synth file is one new revision
        rows["revision"].add(synth_rev["sha1"])
        assert rows["revision"] == provenance.storage.entity_get_all(
            EntityType.REVISION
        ), synth_rev["msg"]
        # check the timestamp of the revision
        rev_ts = synth_rev["date"]
        rev_data = provenance.storage.revision_get([synth_rev["sha1"]])[
            synth_rev["sha1"]
        ]
        assert (
            rev_data.date is not None and rev_ts == rev_data.date.timestamp()
        ), synth_rev["msg"]

        # this revision might have added new content objects
        rows["content"] |= set(x["dst"] for x in synth_rev["R_C"])
        rows["content"] |= set(x["dst"] for x in synth_rev["D_C"])
        assert rows["content"] == provenance.storage.entity_get_all(
            EntityType.CONTENT
        ), synth_rev["msg"]

        # check for R-C (direct) entries
        # these are added directly in the content_early_in_rev table
        rows["content_in_revision"] |= set(
            (x["dst"], x["src"], maybe_path(x["path"])) for x in synth_rev["R_C"]
        )
        assert rows["content_in_revision"] == {
            (rel.src, rel.dst, rel.path)
            for rel in provenance.storage.relation_get_all(
                RelationType.CNT_EARLY_IN_REV
            )
        }, synth_rev["msg"]
        # check timestamps
        for rc in synth_rev["R_C"]:
            assert (
                rev_ts + rc["rel_ts"]
                == provenance.storage.content_get([rc["dst"]])[rc["dst"]].timestamp()
            ), synth_rev["msg"]

        # check directories
        # each directory stored in the provenance index is an entry
        #      in the "directory" table...
        rows["directory"] |= set(x["dst"] for x in synth_rev["R_D"])
        assert rows["directory"] == provenance.storage.entity_get_all(
            EntityType.DIRECTORY
        ), synth_rev["msg"]

        # ... + a number of rows in the "directory_in_rev" table...
        # check for R-D entries
        rows["directory_in_revision"] |= set(
            (x["dst"], x["src"], maybe_path(x["path"])) for x in synth_rev["R_D"]
        )
        assert rows["directory_in_revision"] == {
            (rel.src, rel.dst, rel.path)
            for rel in provenance.storage.relation_get_all(RelationType.DIR_IN_REV)
        }, synth_rev["msg"]
        # check timestamps
        for rd in synth_rev["R_D"]:
            assert (
                rev_ts + rd["rel_ts"]
                == provenance.storage.directory_get([rd["dst"]])[rd["dst"]].timestamp()
            ), synth_rev["msg"]

        # ... + a number of rows in the "content_in_dir" table
        #     for content of the directory.
        # check for D-C entries
        rows["content_in_directory"] |= set(
            (x["dst"], x["src"], maybe_path(x["path"])) for x in synth_rev["D_C"]
        )
        assert rows["content_in_directory"] == {
            (rel.src, rel.dst, rel.path)
            for rel in provenance.storage.relation_get_all(RelationType.CNT_IN_DIR)
        }, synth_rev["msg"]
        # check timestamps
        for dc in synth_rev["D_C"]:
            assert (
                rev_ts + dc["rel_ts"]
                == provenance.storage.content_get([dc["dst"]])[dc["dst"]].timestamp()
            ), synth_rev["msg"]

        if provenance.storage.with_path():
            # check for location entries
            rows["location"] |= set(x["path"] for x in synth_rev["R_C"])
            rows["location"] |= set(x["path"] for x in synth_rev["D_C"])
            rows["location"] |= set(x["path"] for x in synth_rev["R_D"])
            assert rows["location"] == provenance.storage.location_get(), synth_rev[
                "msg"
            ]


@pytest.mark.parametrize(
    "repo, lower, mindepth",
    (
        ("cmdbts2", True, 1),
        ("cmdbts2", False, 1),
        ("cmdbts2", True, 2),
        ("cmdbts2", False, 2),
        ("out-of-order", True, 1),
    ),
)
@pytest.mark.parametrize("batch", (True, False))
def test_provenance_heuristics_content_find_all(
    provenance: ProvenanceInterface,
    swh_storage: Storage,
    archive: ArchiveInterface,
    repo: str,
    lower: bool,
    mindepth: int,
    batch: bool,
) -> None:
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data(repo)
    fill_storage(swh_storage, data)
    revisions = [
        RevisionEntry(
            id=revision["id"],
            date=ts2dt(revision["date"]),
            root=revision["directory"],
        )
        for revision in data["revision"]
    ]

    def maybe_path(path: str) -> str:
        if provenance.storage.with_path():
            return path
        return ""

    if batch:
        revision_add(provenance, archive, revisions, lower=lower, mindepth=mindepth)
    else:
        for revision in revisions:
            revision_add(
                provenance, archive, [revision], lower=lower, mindepth=mindepth
            )

    syntheticfile = get_datafile(
        f"synthetic_{repo}_{'lower' if lower else 'upper'}_{mindepth}.txt"
    )
    expected_occurrences: Dict[str, List[Tuple[str, float, Optional[str], str]]] = {}
    for synth_rev in synthetic_result(syntheticfile):
        rev_id = synth_rev["sha1"].hex()
        rev_ts = synth_rev["date"]

        for rc in synth_rev["R_C"]:
            expected_occurrences.setdefault(rc["dst"].hex(), []).append(
                (rev_id, rev_ts, None, maybe_path(rc["path"]))
            )
        for dc in synth_rev["D_C"]:
            assert dc["prefix"] is not None  # to please mypy
            expected_occurrences.setdefault(dc["dst"].hex(), []).append(
                (rev_id, rev_ts, None, maybe_path(dc["prefix"] + "/" + dc["path"]))
            )

    for content_id, results in expected_occurrences.items():
        expected = [(content_id, *result) for result in results]
        db_occurrences = [
            (
                occur.content.hex(),
                occur.revision.hex(),
                occur.date.timestamp(),
                occur.origin,
                occur.path.decode(),
            )
            for occur in provenance.content_find_all(hash_to_bytes(content_id))
        ]
        if provenance.storage.with_path():
            # this is not true if the db stores no path, because a same content
            # that appears several times in a given revision may be reported
            # only once by content_find_all()
            assert len(db_occurrences) == len(expected)
        assert set(db_occurrences) == set(expected)


@pytest.mark.parametrize(
    "repo, lower, mindepth",
    (
        ("cmdbts2", True, 1),
        ("cmdbts2", False, 1),
        ("cmdbts2", True, 2),
        ("cmdbts2", False, 2),
        ("out-of-order", True, 1),
    ),
)
@pytest.mark.parametrize("batch", (True, False))
def test_provenance_heuristics_content_find_first(
    provenance: ProvenanceInterface,
    swh_storage: Storage,
    archive: ArchiveInterface,
    repo: str,
    lower: bool,
    mindepth: int,
    batch: bool,
) -> None:
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data(repo)
    fill_storage(swh_storage, data)
    revisions = [
        RevisionEntry(
            id=revision["id"],
            date=ts2dt(revision["date"]),
            root=revision["directory"],
        )
        for revision in data["revision"]
    ]

    if batch:
        revision_add(provenance, archive, revisions, lower=lower, mindepth=mindepth)
    else:
        for revision in revisions:
            revision_add(
                provenance, archive, [revision], lower=lower, mindepth=mindepth
            )

    syntheticfile = get_datafile(
        f"synthetic_{repo}_{'lower' if lower else 'upper'}_{mindepth}.txt"
    )
    expected_first: Dict[str, Tuple[str, float, List[str]]] = {}
    # dict of tuples (blob_id, rev_id, [path, ...]) the third element for path
    # is a list because a content can be added at several places in a single
    # revision, in which case the result of content_find_first() is one of
    # those path, but we have no guarantee which one it will return.
    for synth_rev in synthetic_result(syntheticfile):
        rev_id = synth_rev["sha1"].hex()
        rev_ts = synth_rev["date"]

        for rc in synth_rev["R_C"]:
            sha1 = rc["dst"].hex()
            if sha1 not in expected_first:
                assert rc["rel_ts"] == 0
                expected_first[sha1] = (rev_id, rev_ts, [rc["path"]])
            else:
                if rev_ts == expected_first[sha1][1]:
                    expected_first[sha1][2].append(rc["path"])
                elif rev_ts < expected_first[sha1][1]:
                    expected_first[sha1] = (rev_id, rev_ts, [rc["path"]])

        for dc in synth_rev["D_C"]:
            sha1 = rc["dst"].hex()
            assert sha1 in expected_first
            # nothing to do there, this content cannot be a "first seen file"

    for content_id, (rev_id, ts, paths) in expected_first.items():
        occur = provenance.content_find_first(hash_to_bytes(content_id))
        assert occur is not None
        assert occur.content.hex() == content_id
        assert occur.revision.hex() == rev_id
        assert occur.date.timestamp() == ts
        assert occur.origin is None
        if provenance.storage.with_path():
            assert occur.path.decode() in paths
