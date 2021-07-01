# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os import path
import re
from typing import Any, Dict, Iterable, Iterator, List, Optional

import msgpack
import psycopg2
import pytest
from typing_extensions import TypedDict

from swh.core.db import BaseDb
from swh.journal.serializers import msgpack_ext_hook
from swh.model.hashutil import hash_to_bytes
from swh.model.model import Sha1Git
from swh.model.tests.swh_model_data import TEST_OBJECTS
from swh.provenance import get_provenance
from swh.provenance.archive import ArchiveInterface
from swh.provenance.postgresql.archive import ArchivePostgreSQL
from swh.provenance.postgresql.provenancedb_base import ProvenanceDBBase
from swh.provenance.provenance import ProvenanceInterface
from swh.provenance.storage.archive import ArchiveStorage
from swh.storage.postgresql.storage import Storage
from swh.storage.replay import process_replay_objects


@pytest.fixture(params=["with-path", "without-path"])
def provenance(
    request,  # TODO: add proper type annotation
    postgresql: psycopg2.extensions.connection,
) -> ProvenanceInterface:
    """return a working and initialized provenance db"""
    from swh.core.cli.db import populate_database_for_package

    flavor = request.param
    populate_database_for_package("swh.provenance", postgresql.dsn, flavor=flavor)

    BaseDb.adapt_conn(postgresql)

    args: Dict[str, str] = {
        item.split("=")[0]: item.split("=")[1]
        for item in postgresql.dsn.split()
        if item.split("=")[0] != "options"
    }
    prov = get_provenance(cls="local", db=args)
    assert isinstance(prov.storage, ProvenanceDBBase)
    assert prov.storage.flavor == flavor
    # in test sessions, we DO want to raise any exception occurring at commit time
    prov.storage.raise_on_commit = True
    return prov


@pytest.fixture
def swh_storage_with_objects(swh_storage: Storage) -> Storage:
    """return a Storage object (postgresql-based by default) with a few of each
    object type in it

    The inserted content comes from swh.model.tests.swh_model_data.
    """
    for obj_type in (
        "content",
        "skipped_content",
        "directory",
        "revision",
        "release",
        "snapshot",
        "origin",
        "origin_visit",
        "origin_visit_status",
    ):
        getattr(swh_storage, f"{obj_type}_add")(TEST_OBJECTS[obj_type])
    return swh_storage


@pytest.fixture
def archive_direct(swh_storage_with_objects: Storage) -> ArchiveInterface:
    return ArchivePostgreSQL(swh_storage_with_objects.get_db().conn)


@pytest.fixture
def archive_api(swh_storage_with_objects: Storage) -> ArchiveInterface:
    return ArchiveStorage(swh_storage_with_objects)


@pytest.fixture(params=["archive", "db"])
def archive(request, swh_storage_with_objects: Storage) -> Iterator[ArchiveInterface]:
    """Return a ArchivePostgreSQL based StorageInterface object"""
    # this is a workaround to prevent tests from hanging because of an unclosed
    # transaction.
    # TODO: refactor the ArchivePostgreSQL to properly deal with
    # transactions and get rid of this fixture
    if request.param == "db":
        archive = ArchivePostgreSQL(conn=swh_storage_with_objects.get_db().conn)
        yield archive
        archive.conn.rollback()
    else:
        yield ArchiveStorage(swh_storage_with_objects)


def get_datafile(fname: str) -> str:
    return path.join(path.dirname(__file__), "data", fname)


def load_repo_data(repo: str) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    with open(get_datafile(f"{repo}.msgpack"), "rb") as fobj:
        unpacker = msgpack.Unpacker(
            fobj,
            raw=False,
            ext_hook=msgpack_ext_hook,
            strict_map_key=False,
            timestamp=3,  # convert Timestamp in datetime objects (tz UTC)
        )
        for objtype, objd in unpacker:
            data.setdefault(objtype, []).append(objd)
    return data


def filter_dict(d: Dict[Any, Any], keys: Iterable[Any]) -> Dict[Any, Any]:
    return {k: v for (k, v) in d.items() if k in keys}


def fill_storage(storage: Storage, data: Dict[str, Any]) -> None:
    process_replay_objects(data, storage=storage)


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


def synthetic_result(filename: str) -> Iterator[SynthRevision]:
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
        yield from _parse_synthetic_file(fobj)


def _parse_synthetic_file(fobj: Iterable[str]) -> Iterator[SynthRevision]:
    """Read a 'synthetic' file and generate a dict representation of the synthetic
    revision for each revision listed in the synthetic file.
    """
    regs = [
        "(?P<revname>R[0-9]{2,4})?",
        "(?P<reltype>[^| ]*)",
        "([+] )?(?P<path>[^| +]*?)[/]?",
        "(?P<type>[RDC]) (?P<sha1>[0-9a-z]{40})",
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
