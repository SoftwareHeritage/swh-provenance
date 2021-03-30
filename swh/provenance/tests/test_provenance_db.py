# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

import pytest

from swh.model.tests.swh_model_data import TEST_OBJECTS
from swh.provenance.model import RevisionEntry
from swh.provenance.origin import OriginEntry
from swh.provenance.provenance import origin_add, revision_add
from swh.provenance.storage.archive import ArchiveStorage
from swh.provenance.tests.conftest import synthetic_result


def ts2dt(ts: dict) -> datetime.datetime:
    timestamp = datetime.datetime.fromtimestamp(
        ts["timestamp"]["seconds"],
        datetime.timezone(datetime.timedelta(minutes=ts["offset"])),
    )
    return timestamp.replace(microsecond=ts["timestamp"]["microseconds"])


def test_provenance_origin_add(provenance, swh_storage_with_objects):
    """Test the ProvenanceDB.origin_add() method"""
    for origin in TEST_OBJECTS["origin"]:
        entry = OriginEntry(url=origin.url, revisions=[])
        origin_add(ArchiveStorage(swh_storage_with_objects), provenance, entry)
    # TODO: check some facts here


def test_provenance_add_revision(provenance, storage_and_CMDBTS, archive_pg):

    storage, data = storage_and_CMDBTS
    for i in range(2):
        # do it twice, there should be no change in results
        for revision in data["revision"]:
            entry = RevisionEntry(
                id=revision["id"],
                date=ts2dt(revision["date"]),
                root=revision["directory"],
            )
            revision_add(provenance, archive_pg, entry)

        # there should be as many entries in 'revision' as revisions from the
        # test dataset
        provenance.cursor.execute("SELECT count(*) FROM revision")
        assert provenance.cursor.fetchone()[0] == len(data["revision"])

        # there should be no 'location' for the empty path
        provenance.cursor.execute("SELECT count(*) FROM location WHERE path=''")
        assert provenance.cursor.fetchone()[0] == 0

        # there should be 32 'location' for non-empty path
        provenance.cursor.execute("SELECT count(*) FROM location WHERE path!=''")
        assert provenance.cursor.fetchone()[0] == 32

        # there should be as many entries in 'revision' as revisions from the
        # test dataset
        provenance.cursor.execute("SELECT count(*) FROM revision")
        assert provenance.cursor.fetchone()[0] == len(data["revision"])

        # 7 directories
        provenance.cursor.execute("SELECT count(*) FROM directory")
        assert provenance.cursor.fetchone()[0] == 7

        # 12 D-R entries
        provenance.cursor.execute("SELECT count(*) FROM directory_in_rev")
        assert provenance.cursor.fetchone()[0] == 12

        provenance.cursor.execute("SELECT count(*) FROM content")
        assert provenance.cursor.fetchone()[0] == len(data["content"])
        provenance.cursor.execute("SELECT count(*) FROM content_in_dir")
        assert provenance.cursor.fetchone()[0] == 16
        provenance.cursor.execute("SELECT count(*) FROM content_early_in_rev")
        assert provenance.cursor.fetchone()[0] == 13


def test_provenance_content_find_first(provenance, storage_and_CMDBTS, archive_pg):
    storage, data = storage_and_CMDBTS
    for revision in data["revision"]:
        entry = RevisionEntry(
            id=revision["id"], date=ts2dt(revision["date"]), root=revision["directory"],
        )
        revision_add(provenance, archive_pg, entry)

    first_expected_content = [
        {
            "content": "43f3c871310a8e524004e91f033e7fb3b0bc8475",
            "rev": "35ccb8dd1b53d2d8a5c1375eb513ef2beaa79ae5",
            "date": 1609757158,
            "path": "README.md",
        },
        {
            "content": "6dc7e44ead5c0e300fe94448c3e046dfe33ad4d1",
            "rev": "9e36e095b79e36a3da104ce272989b39cd68aefd",
            "date": 1610644094,
            "path": "Red/Blue/Green/a",
        },
        {
            "content": "9f6e04be05297905f1275d3f4e0bb0583458b2e8",
            "rev": "bfbfcc72ae7fc35d6941386c36280512e6b38440",
            "date": 1610644097,
            "path": "Red/Blue/Green/b",
        },
        {
            "content": "a28fa70e725ebda781e772795ca080cd737b823c",
            "rev": "0a31c9d509783abfd08f9fdfcd3acae20f17dfd0",
            "date": 1610644099,
            "path": "Red/Blue/c",
        },
        {
            "content": "c0229d305adf3edf49f031269a70e3e87665fe88",
            "rev": "1d1fcf1816a8a2a77f9b1f342ba11d0fe9fd7f17",
            "date": 1610644105,
            "path": "Purple/d",
        },
        {
            "content": "94ba40161084e8b80943accd9d24e1f9dd47189b",
            "rev": "55d4dc9471de6144f935daf3c38878155ca274d5",
            "date": 1610644113,
            "path": ("Dark/Brown/Purple/f", "Dark/Brown/Purple/g", "Dark/h"),  # XXX
        },
        {
            "content": "5e8f9ceaee9dafae2e3210e254fdf170295f8b5b",
            "rev": "a8939755d0be76cfea136e9e5ebce9bc51c49fef",
            "date": 1610644116,
            "path": "Dark/h",
        },
        {
            "content": "bbd54b961764094b13f10cef733e3725d0a834c3",
            "rev": "ca1774a07b6e02c1caa7ae678924efa9259ee7c6",
            "date": 1610644118,
            "path": "Paris/i",
        },
        {
            "content": "7ce4fe9a22f589fa1656a752ea371b0ebc2106b1",
            "rev": "611fe71d75b6ea151b06e3845c09777acc783d82",
            "date": 1610644120,
            "path": "Paris/j",
        },
        {
            "content": "cb79b39935c9392fa5193d9f84a6c35dc9c22c75",
            "rev": "4c5551b4969eb2160824494d40b8e1f6187fc01e",
            "date": 1610644122,
            "path": "Paris/k",
        },
    ]

    for expected in first_expected_content:
        contentid = bytes.fromhex(expected["content"])
        (blob, rev, date, path) = provenance.content_find_first(contentid)
        if isinstance(expected["path"], tuple):
            assert bytes(path).decode() in expected["path"]
        else:
            assert bytes(path).decode() == expected["path"]
        assert bytes(blob) == contentid
        assert bytes(rev).hex() == expected["rev"]
        assert int(date.timestamp()) == expected["date"]


@pytest.mark.parametrize(
    "syntheticfile, args",
    (
        ("synthetic_noroot_lower.txt", {"lower": True, "mindepth": 1}),
        ("synthetic_noroot_upper.txt", {"lower": False, "mindepth": 1}),
    ),
)
def test_provenance_db(provenance, storage_and_CMDBTS, archive_pg, syntheticfile, args):
    storage, data = storage_and_CMDBTS

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

    def db_count(table):
        provenance.cursor.execute(f"SELECT count(*) FROM {table}")
        return provenance.cursor.fetchone()[0]

    for synth_rev in synthetic_result(syntheticfile):
        revision = revisions[synth_rev["sha1"]]
        entry = RevisionEntry(
            id=revision["id"], date=ts2dt(revision["date"]), root=revision["directory"],
        )
        revision_add(provenance, archive_pg, entry, **args)

        # import pdb; pdb.set_trace()
        # each "entry" in the synth file is one new revision
        rows["revision"].add(synth_rev["sha1"])
        assert len(rows["revision"]) == db_count("revision")

        # this revision might have added new content objects
        rows["content"] |= set(x["dst"] for x in synth_rev["R_C"])
        rows["content"] |= set(x["dst"] for x in synth_rev["D_C"])
        assert len(rows["content"]) == db_count("content")

        # check for R-C (direct) entries
        rows["content_early_in_rev"] |= set(
            (x["src"], x["dst"], x["path"]) for x in synth_rev["R_C"]
        )
        assert len(rows["content_early_in_rev"]) == db_count("content_early_in_rev")

        # check directories
        rows["directory"] |= set(x["dst"] for x in synth_rev["R_D"])
        assert len(rows["directory"]) == db_count("directory")

        # check for R-D entries
        rows["directory_in_rev"] |= set(
            (x["src"], x["dst"], x["path"]) for x in synth_rev["R_D"]
        )
        assert len(rows["directory_in_rev"]) == db_count("directory_in_rev")

        # check for D-C entries
        rows["content_in_dir"] |= set(
            (x["src"], x["dst"], x["path"]) for x in synth_rev["D_C"]
        )
        assert len(rows["content_in_dir"]) == db_count("content_in_dir")

        # check for location entries
        rows["location"] |= set(x["path"] for x in synth_rev["R_C"])
        rows["location"] |= set(x["path"] for x in synth_rev["D_C"])
        rows["location"] |= set(x["path"] for x in synth_rev["R_D"])
        assert len(rows["location"]) == db_count("location")
