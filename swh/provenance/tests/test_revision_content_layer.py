# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

import pytest
from typing_extensions import TypedDict

from swh.model.hashutil import hash_to_bytes
from swh.model.model import Sha1Git
from swh.provenance.algos.directory import directory_add
from swh.provenance.algos.revision import revision_add
from swh.provenance.archive import ArchiveInterface
from swh.provenance.interface import ProvenanceInterface
from swh.provenance.model import DirectoryEntry, RevisionEntry
from swh.provenance.storage.interface import EntityType, RelationType

from .utils import fill_storage, get_datafile, load_repo_data, ts2dt


class SynthRelation(TypedDict):
    prefix: Optional[str]
    path: str
    src: Sha1Git
    dst: Sha1Git
    rel_ts: float


class SynthRevision(TypedDict):
    sha1: Sha1Git
    date: float
    msg: str
    R_C: List[SynthRelation]
    R_D: List[SynthRelation]
    D_C: List[SynthRelation]


def synthetic_revision_content_result(filename: str) -> Iterator[SynthRevision]:
    """Generates dict representations of synthetic revisions found in the synthetic
    file (from the data/ directory) given as argument of the generator.

    Generated SynthRevision (typed dict) with the following elements:

      "sha1": (Sha1Git) sha1 of the revision,
      "date": (float) timestamp of the revision,
      "msg": (str) commit message of the revision,
      "R_C": (list) new R---C relations added by this revision
      "R_D": (list) new R-D   relations added by this revision
      "D_C": (list) new   D-C relations added by this revision

    Each relation above is a SynthRelation typed dict with:

      "path": (str) location
      "src": (Sha1Git) sha1 of the source of the relation
      "dst": (Sha1Git) sha1 of the destination of the relation
      "rel_ts": (float) timestamp of the target of the relation
                (related to the timestamp of the revision)

    """

    with open(get_datafile(filename), "r") as fobj:
        yield from _parse_synthetic_revision_content_file(fobj)


def _parse_synthetic_revision_content_file(
    fobj: Iterable[str],
) -> Iterator[SynthRevision]:
    """Read a 'synthetic' file and generate a dict representation of the synthetic
    revision for each revision listed in the synthetic file.
    """
    regs = [
        "(?P<revname>R[0-9]{2,4})?",
        "(?P<reltype>[^| ]*)",
        "([+] )?(?P<path>[^| +]*?)[/]?",
        "(?P<type>[RDC]) (?P<sha1>[0-9a-f]{40})",
        "(?P<ts>-?[0-9]+(.[0-9]+)?)",
    ]
    regex = re.compile("^ *" + r" *[|] *".join(regs) + r" *(#.*)?$")
    current_rev: List[dict] = []
    for m in (regex.match(line) for line in fobj):
        if m:
            d = m.groupdict()
            if d["revname"]:
                if current_rev:
                    yield _mk_synth_rev(current_rev)
                current_rev.clear()
            current_rev.append(d)
    if current_rev:
        yield _mk_synth_rev(current_rev)


def _mk_synth_rev(synth_rev: List[Dict[str, str]]) -> SynthRevision:
    assert synth_rev[0]["type"] == "R"
    rev = SynthRevision(
        sha1=hash_to_bytes(synth_rev[0]["sha1"]),
        date=float(synth_rev[0]["ts"]),
        msg=synth_rev[0]["revname"],
        R_C=[],
        R_D=[],
        D_C=[],
    )
    current_path = None
    # path of the last R-D relation we parsed, used a prefix for next D-C
    # relations

    for row in synth_rev[1:]:
        if row["reltype"] == "R---C":
            assert row["type"] == "C"
            rev["R_C"].append(
                SynthRelation(
                    prefix=None,
                    path=row["path"],
                    src=rev["sha1"],
                    dst=hash_to_bytes(row["sha1"]),
                    rel_ts=float(row["ts"]),
                )
            )
            current_path = None
        elif row["reltype"] == "R-D":
            assert row["type"] == "D"
            rev["R_D"].append(
                SynthRelation(
                    prefix=None,
                    path=row["path"],
                    src=rev["sha1"],
                    dst=hash_to_bytes(row["sha1"]),
                    rel_ts=float(row["ts"]),
                )
            )
            current_path = row["path"]
        elif row["reltype"] == "D-C":
            assert row["type"] == "C"
            rev["D_C"].append(
                SynthRelation(
                    prefix=current_path,
                    path=row["path"],
                    src=rev["R_D"][-1]["dst"],
                    dst=hash_to_bytes(row["sha1"]),
                    rel_ts=float(row["ts"]),
                )
            )
    return rev


@pytest.mark.parametrize(
    "repo, lower, mindepth, flatten",
    (
        ("cmdbts2", True, 1, True),
        ("cmdbts2", True, 1, False),
        ("cmdbts2", False, 1, True),
        ("cmdbts2", False, 1, False),
        ("cmdbts2", True, 2, True),
        ("cmdbts2", True, 2, False),
        ("cmdbts2", False, 2, True),
        ("cmdbts2", False, 2, False),
        ("out-of-order", True, 1, True),
        ("out-of-order", True, 1, False),
    ),
)
def test_revision_content_result(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    repo: str,
    lower: bool,
    mindepth: int,
    flatten: bool,
) -> None:
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data(repo)
    fill_storage(archive.storage, data)
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

    for synth_rev in synthetic_revision_content_result(syntheticfile):
        revision = revisions[synth_rev["sha1"]]
        entry = RevisionEntry(
            id=revision["id"],
            date=ts2dt(revision["date"]),
            root=revision["directory"],
        )

        if flatten:
            revision_add(provenance, archive, [entry], lower=lower, mindepth=mindepth)
        else:
            prev_directories = provenance.storage.entity_get_all(EntityType.DIRECTORY)
            revision_add(
                provenance,
                archive,
                [entry],
                lower=lower,
                mindepth=mindepth,
                flatten=False,
            )
            directories = [
                DirectoryEntry(id=sha1)
                for sha1 in provenance.storage.entity_get_all(
                    EntityType.DIRECTORY
                ).difference(prev_directories)
            ]
            for directory in directories:
                assert not provenance.directory_already_flattened(directory)
            directory_add(provenance, archive, directories)

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
            (x["dst"], x["src"], x["path"].encode()) for x in synth_rev["R_C"]
        )
        assert rows["content_in_revision"] == {
            (src, rel.dst, rel.path)
            for src, rels in provenance.storage.relation_get_all(
                RelationType.CNT_EARLY_IN_REV
            ).items()
            for rel in rels
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
            (x["dst"], x["src"], x["path"].encode()) for x in synth_rev["R_D"]
        )
        assert rows["directory_in_revision"] == {
            (src, rel.dst, rel.path)
            for src, rels in provenance.storage.relation_get_all(
                RelationType.DIR_IN_REV
            ).items()
            for rel in rels
        }, synth_rev["msg"]
        # check timestamps
        for rd in synth_rev["R_D"]:
            dir_data = provenance.storage.directory_get([rd["dst"]])[rd["dst"]]
            assert dir_data.date is not None
            assert rev_ts + rd["rel_ts"] == dir_data.date.timestamp(), synth_rev["msg"]
            assert dir_data.flat, synth_rev["msg"]

        # ... + a number of rows in the "content_in_dir" table
        #     for content of the directory.
        # check for D-C entries
        rows["content_in_directory"] |= set(
            (x["dst"], x["src"], x["path"].encode()) for x in synth_rev["D_C"]
        )
        assert rows["content_in_directory"] == {
            (src, rel.dst, rel.path)
            for src, rels in provenance.storage.relation_get_all(
                RelationType.CNT_IN_DIR
            ).items()
            for rel in rels
        }, synth_rev["msg"]
        # check timestamps
        for dc in synth_rev["D_C"]:
            assert (
                rev_ts + dc["rel_ts"]
                == provenance.storage.content_get([dc["dst"]])[dc["dst"]].timestamp()
            ), synth_rev["msg"]

        # check for location entries
        rows["location"] |= set(x["path"].encode() for x in synth_rev["R_C"])
        rows["location"] |= set(x["path"].encode() for x in synth_rev["D_C"])
        rows["location"] |= set(x["path"].encode() for x in synth_rev["R_D"])
        assert rows["location"] == set(
            provenance.storage.location_get_all().values()
        ), synth_rev["msg"]


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
    archive: ArchiveInterface,
    repo: str,
    lower: bool,
    mindepth: int,
    batch: bool,
) -> None:
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data(repo)
    fill_storage(archive.storage, data)
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
    expected_occurrences: Dict[str, List[Tuple[str, float, Optional[str], str]]] = {}
    for synth_rev in synthetic_revision_content_result(syntheticfile):
        rev_id = synth_rev["sha1"].hex()
        rev_ts = synth_rev["date"]

        for rc in synth_rev["R_C"]:
            expected_occurrences.setdefault(rc["dst"].hex(), []).append(
                (rev_id, rev_ts, None, rc["path"])
            )
        for dc in synth_rev["D_C"]:
            assert dc["prefix"] is not None  # to please mypy
            expected_occurrences.setdefault(dc["dst"].hex(), []).append(
                (rev_id, rev_ts, None, dc["prefix"] + "/" + dc["path"])
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
    archive: ArchiveInterface,
    repo: str,
    lower: bool,
    mindepth: int,
    batch: bool,
) -> None:
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data(repo)
    fill_storage(archive.storage, data)
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
    for synth_rev in synthetic_revision_content_result(syntheticfile):
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
        assert occur.path.decode() in paths
