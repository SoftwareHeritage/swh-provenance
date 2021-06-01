# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.provenance.model import RevisionEntry
from swh.provenance.provenance import revision_add
from swh.provenance.tests.conftest import (
    fill_storage,
    get_datafile,
    load_repo_data,
    synthetic_result,
)
from swh.provenance.tests.test_provenance_db import ts2dt


def sha1s(cur, table):
    """return the 'sha1' column from the DB 'table' (as hex)

    'cur' is a cursor to the provenance index DB.
    """
    cur.execute(f"SELECT sha1 FROM {table}")
    return set(sha1.hex() for (sha1,) in cur.fetchall())


def locations(cur):
    """return the 'path' column from the DB location table

    'cur' is a cursor to the provenance index DB.
    """
    cur.execute("SELECT encode(location.path::bytea, 'escape') FROM location")
    return set(x for (x,) in cur.fetchall())


def relations(cur, src, dst):
    """return the triplets ('sha1', 'sha1', 'path') from the DB

    for the relation between 'src' table and 'dst' table
    (i.e. for C-R, C-D and D-R relations).

    'cur' is a cursor to the provenance index DB.
    """
    relation = {
        ("content", "revision"): "content_early_in_rev",
        ("content", "directory"): "content_in_dir",
        ("directory", "revision"): "directory_in_rev",
    }[(src, dst)]

    srccol = {"content": "blob", "directory": "dir"}[src]
    dstcol = {"directory": "dir", "revision": "rev"}[dst]

    cur.execute(
        f"SELECT encode(src.sha1::bytea, 'hex'),"
        f"       encode(dst.sha1::bytea, 'hex'),"
        f"       encode(location.path::bytea, 'escape') "
        f"FROM {relation} as rel, "
        f"     {src} as src, {dst} as dst, location "
        f"WHERE rel.{srccol}=src.id AND rel.{dstcol}=dst.id AND rel.loc=location.id"
    )
    return set(cur.fetchall())


def get_timestamp(cur, table, sha1):
    """return the date for the 'sha1' from the DB 'table' (as hex)

    'cur' is a cursor to the provenance index DB.
    """
    if isinstance(sha1, str):
        sha1 = bytes.fromhex(sha1)
    cur.execute(f"SELECT date FROM {table} WHERE sha1=%s", (sha1,))
    return [date.timestamp() for (date,) in cur.fetchall()]


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
def test_provenance_heuristics(provenance, swh_storage, archive, repo, lower, mindepth):
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data(repo)
    fill_storage(swh_storage, data)
    syntheticfile = get_datafile(
        f"synthetic_{repo}_{'lower' if lower else 'upper'}_{mindepth}.txt"
    )

    revisions = {rev["id"]: rev for rev in data["revision"]}

    rows = {
        "content": set(),
        "content_in_dir": set(),
        "content_early_in_rev": set(),
        "directory": set(),
        "directory_in_rev": set(),
        "location": set(),
        "revision": set(),
    }

    for synth_rev in synthetic_result(syntheticfile):
        revision = revisions[synth_rev["sha1"]]
        entry = RevisionEntry(
            id=revision["id"],
            date=ts2dt(revision["date"]),
            root=revision["directory"],
        )
        revision_add(provenance, archive, [entry], lower=lower, mindepth=mindepth)

        # each "entry" in the synth file is one new revision
        rows["revision"].add(synth_rev["sha1"].hex())
        assert rows["revision"] == sha1s(provenance.cursor, "revision"), synth_rev[
            "msg"
        ]
        # check the timestamp of the revision
        rev_ts = synth_rev["date"]
        assert get_timestamp(
            provenance.cursor, "revision", synth_rev["sha1"].hex()
        ) == [rev_ts], synth_rev["msg"]

        # this revision might have added new content objects
        rows["content"] |= set(x["dst"].hex() for x in synth_rev["R_C"])
        rows["content"] |= set(x["dst"].hex() for x in synth_rev["D_C"])
        assert rows["content"] == sha1s(provenance.cursor, "content"), synth_rev["msg"]

        # check for R-C (direct) entries
        # these are added directly in the content_early_in_rev table
        rows["content_early_in_rev"] |= set(
            (x["dst"].hex(), x["src"].hex(), x["path"]) for x in synth_rev["R_C"]
        )
        assert rows["content_early_in_rev"] == relations(
            provenance.cursor, "content", "revision"
        ), synth_rev["msg"]
        # check timestamps
        for rc in synth_rev["R_C"]:
            assert get_timestamp(provenance.cursor, "content", rc["dst"]) == [
                rev_ts + rc["rel_ts"]
            ], synth_rev["msg"]

        # check directories
        # each directory stored in the provenance index is an entry
        #      in the "directory" table...
        rows["directory"] |= set(x["dst"].hex() for x in synth_rev["R_D"])
        assert rows["directory"] == sha1s(provenance.cursor, "directory"), synth_rev[
            "msg"
        ]

        # ... + a number of rows in the "directory_in_rev" table...
        # check for R-D entries
        rows["directory_in_rev"] |= set(
            (x["dst"].hex(), x["src"].hex(), x["path"]) for x in synth_rev["R_D"]
        )
        assert rows["directory_in_rev"] == relations(
            provenance.cursor, "directory", "revision"
        ), synth_rev["msg"]
        # check timestamps
        for rd in synth_rev["R_D"]:
            assert get_timestamp(provenance.cursor, "directory", rd["dst"]) == [
                rev_ts + rd["rel_ts"]
            ], synth_rev["msg"]

        # ... + a number of rows in the "content_in_dir" table
        #     for content of the directory.
        # check for D-C entries
        rows["content_in_dir"] |= set(
            (x["dst"].hex(), x["src"].hex(), x["path"]) for x in synth_rev["D_C"]
        )
        assert rows["content_in_dir"] == relations(
            provenance.cursor, "content", "directory"
        ), synth_rev["msg"]
        # check timestamps
        for dc in synth_rev["D_C"]:
            assert get_timestamp(provenance.cursor, "content", dc["dst"]) == [
                rev_ts + dc["rel_ts"]
            ], synth_rev["msg"]

        # check for location entries
        rows["location"] |= set(x["path"] for x in synth_rev["R_C"])
        rows["location"] |= set(x["path"] for x in synth_rev["D_C"])
        rows["location"] |= set(x["path"] for x in synth_rev["R_D"])
        assert rows["location"] == locations(provenance.cursor), synth_rev["msg"]
