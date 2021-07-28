# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re
from typing import Any, Dict, Iterable, Iterator, List, Set

import pytest
from typing_extensions import TypedDict

from swh.model.hashutil import hash_to_bytes
from swh.model.model import Sha1Git
from swh.provenance.archive import ArchiveInterface
from swh.provenance.interface import EntityType, ProvenanceInterface, RelationType
from swh.provenance.model import OriginEntry
from swh.provenance.origin import origin_add
from swh.provenance.tests.conftest import fill_storage, get_datafile, load_repo_data
from swh.storage.postgresql.storage import Storage


class SynthRelation(TypedDict):
    src: Sha1Git
    dst: Sha1Git
    name: str


class SynthOrigin(TypedDict):
    sha1: Sha1Git
    url: str
    snap: Sha1Git
    O_R: List[SynthRelation]
    R_R: List[SynthRelation]


def synthetic_origin_revision_result(filename: str) -> Iterator[SynthOrigin]:
    """Generates dict representations of synthetic origin visits found in the
    synthetic file (from the data/ directory) given as argument of the generator.

    Generated SynthOrigin (typed dict) with the following elements:

      "sha1": (Sha1Git) sha1 of the origin,
      "url": (str) url of the origin,
      "snap": (Sha1Git) sha1 of the visit's snapshot,
      "O_R": (list) new O-R   relations added by this origin visit
      "R_R": (list) new   R-R relations added by this origin visit

    Each relation above is a SynthRelation typed dict with:

      "src": (Sha1Git) sha1 of the source of the relation
      "dst": (Sha1Git) sha1 of the destination of the relation

    """

    with open(get_datafile(filename), "r") as fobj:
        yield from _parse_synthetic_origin_revision_file(fobj)


def _parse_synthetic_origin_revision_file(fobj: Iterable[str]) -> Iterator[SynthOrigin]:
    """Read a 'synthetic' file and generate a dict representation of the synthetic
    origin visit for each snapshot listed in the synthetic file.
    """
    regs = [
        "(?P<url>[^ ]+)?",
        "(?P<reltype>[^| ]*)",
        "(?P<revname>R[0-9]{2,4})?",
        "(?P<type>[ORS]) (?P<sha1>[0-9a-f]{40})",
    ]
    regex = re.compile("^ *" + r" *[|] *".join(regs) + r" *(#.*)?$")
    current_org: List[dict] = []
    for m in (regex.match(line) for line in fobj):
        if m:
            d = m.groupdict()
            if d["url"]:
                if current_org:
                    yield _mk_synth_org(current_org)
                current_org.clear()
            current_org.append(d)
    if current_org:
        yield _mk_synth_org(current_org)


def _mk_synth_org(synth_org: List[Dict[str, str]]) -> SynthOrigin:
    assert synth_org[0]["type"] == "O"
    assert synth_org[1]["type"] == "S"
    org = SynthOrigin(
        sha1=hash_to_bytes(synth_org[0]["sha1"]),
        url=synth_org[0]["url"],
        snap=hash_to_bytes(synth_org[1]["sha1"]),
        O_R=[],
        R_R=[],
    )

    for row in synth_org[2:]:
        if row["reltype"] == "O-R":
            assert row["type"] == "R"
            org["O_R"].append(
                SynthRelation(
                    src=org["sha1"],
                    dst=hash_to_bytes(row["sha1"]),
                    name=row["revname"],
                )
            )
        elif row["reltype"] == "R-R":
            assert row["type"] == "R"
            org["R_R"].append(
                SynthRelation(
                    src=org["O_R"][-1]["dst"],
                    dst=hash_to_bytes(row["sha1"]),
                    name=row["revname"],
                )
            )
    return org


@pytest.mark.parametrize(
    "repo, visit",
    (("with-merges", "visits-01"),),
)
def test_origin_revision_layer(
    provenance: ProvenanceInterface,
    swh_storage: Storage,
    archive: ArchiveInterface,
    repo: str,
    visit: str,
) -> None:
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data(repo)
    fill_storage(swh_storage, data)
    syntheticfile = get_datafile(f"origin-revision_{repo}_{visit}.txt")

    origins = [
        {"url": status["origin"], "snap": status["snapshot"]}
        for status in data["origin_visit_status"]
        if status["snapshot"] is not None
    ]

    rows: Dict[str, Set[Any]] = {
        "origin": set(),
        "revision_in_origin": set(),
        "revision_before_revision": set(),
        "revision": set(),
    }

    for synth_org in synthetic_origin_revision_result(syntheticfile):
        for origin in (
            org
            for org in origins
            if org["url"] == synth_org["url"] and org["snap"] == synth_org["snap"]
        ):
            entry = OriginEntry(url=origin["url"], snapshot=origin["snap"])
            origin_add(provenance, archive, [entry])

            # each "entry" in the synth file is one new origin visit
            rows["origin"].add(synth_org["sha1"])
            assert rows["origin"] == provenance.storage.entity_get_all(
                EntityType.ORIGIN
            ), synth_org["url"]
            # check the url of the origin
            assert (
                provenance.storage.origin_get([synth_org["sha1"]])[synth_org["sha1"]]
                == synth_org["url"]
            ), synth_org["snap"]

            # this origin visit might have added new revision objects
            rows["revision"] |= set(x["dst"] for x in synth_org["O_R"])
            rows["revision"] |= set(x["dst"] for x in synth_org["R_R"])
            assert rows["revision"] == provenance.storage.entity_get_all(
                EntityType.REVISION
            ), synth_org["snap"]

            # check for O-R (head) entries
            # these are added in the revision_in_origin relation
            rows["revision_in_origin"] |= set(
                (x["dst"], x["src"], None) for x in synth_org["O_R"]
            )
            assert rows["revision_in_origin"] == {
                (rel.src, rel.dst, rel.path)
                for rel in provenance.storage.relation_get_all(RelationType.REV_IN_ORG)
            }, synth_org["snap"]

            # check for R-R entries
            # these are added in the revision_before_revision relation
            rows["revision_before_revision"] |= set(
                (x["dst"], x["src"], None) for x in synth_org["R_R"]
            )
            assert rows["revision_before_revision"] == {
                (rel.src, rel.dst, rel.path)
                for rel in provenance.storage.relation_get_all(
                    RelationType.REV_BEFORE_REV
                )
            }, synth_org["snap"]
