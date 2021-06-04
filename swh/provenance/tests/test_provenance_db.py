# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from swh.model.tests.swh_model_data import TEST_OBJECTS
from swh.provenance.model import RevisionEntry
from swh.provenance.origin import OriginEntry
from swh.provenance.provenance import origin_add, revision_add
from swh.provenance.storage.archive import ArchiveStorage


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


def test_provenance_content_find_first(provenance, storage_and_CMDBTS, archive):
    storage, data = storage_and_CMDBTS
    for revision in data["revision"]:
        entry = RevisionEntry(
            id=revision["id"],
            date=ts2dt(revision["date"]),
            root=revision["directory"],
        )
        revision_add(provenance, archive, [entry])

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
