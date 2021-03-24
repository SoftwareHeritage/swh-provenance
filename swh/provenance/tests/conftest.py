# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob
from os import path

import pytest

from swh.core.api.serializers import msgpack_loads
from swh.core.db import BaseDb
from swh.core.db.pytest_plugin import postgresql_fact
from swh.core.utils import numfile_sortkey as sortkey
from swh.model.model import Content, Directory, DirectoryEntry, Revision
from swh.model.tests.swh_model_data import TEST_OBJECTS
import swh.provenance
from swh.provenance.postgresql.archive import ArchivePostgreSQL
from swh.provenance.storage.archive import ArchiveStorage

SQL_DIR = path.join(path.dirname(swh.provenance.__file__), "sql")
SQL_FILES = [
    sqlfile
    for sqlfile in sorted(glob.glob(path.join(SQL_DIR, "*.sql")), key=sortkey)
    if "-without-path-" not in sqlfile
]

provenance_db = postgresql_fact(
    "postgresql_proc", db_name="provenance", dump_files=SQL_FILES
)


@pytest.fixture
def provenance(provenance_db):
    """return a working and initialized provenance db"""
    from swh.provenance.postgresql.provenancedb_with_path import (
        ProvenanceWithPathDB as ProvenanceDB,
    )

    BaseDb.adapt_conn(provenance_db)
    return ProvenanceDB(provenance_db)


@pytest.fixture
def swh_storage_with_objects(swh_storage):
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
def archive_direct(swh_storage_with_objects):
    return ArchivePostgreSQL(swh_storage_with_objects.get_db().conn)


@pytest.fixture
def archive_api(swh_storage_with_objects):
    return ArchiveStorage(swh_storage_with_objects)


@pytest.fixture
def archive_pg(swh_storage_with_objects):
    # this is a workaround to prevent tests from hanging because of an unclosed
    # transaction.
    # TODO: refactor the ArchivePostgreSQL to properly deal with
    # transactions and get rif of this fixture
    archive = ArchivePostgreSQL(conn=swh_storage_with_objects.get_db().conn)
    yield archive
    archive.conn.rollback()


@pytest.fixture
def CMDBTS_data():
    # imported git tree is https://github.com/grouss/CMDBTS rev 4c5551b496
    # ([xxx] is the timestamp):
    # o - [1609757158] first commit            35ccb8dd1b53d2d8a5c1375eb513ef2beaa79ae5
    # |    `- README.md                      * 43f3c871310a8e524004e91f033e7fb3b0bc8475
    # o - [1610644094] Reset Empty repository  840b91df68e9549c156942ddd5002111efa15604
    # |
    # o - [1610644094] R0000                   9e36e095b79e36a3da104ce272989b39cd68aefd
    # |    `- Red/Blue/Green/a               * 6dc7e44ead5c0e300fe94448c3e046dfe33ad4d1
    # o - [1610644097] R0001                   bfbfcc72ae7fc35d6941386c36280512e6b38440
    # |    |- Red/Blue/Green/a                 6dc7e44ead5c0e300fe94448c3e046dfe33ad4d1
    # |    `- Red/Blue/Green/b               * 9f6e04be05297905f1275d3f4e0bb0583458b2e8
    # o - [1610644099] R0002                   0a31c9d509783abfd08f9fdfcd3acae20f17dfd0
    # |    |- Red/Blue/Green/a                 6dc7e44ead5c0e300fe94448c3e046dfe33ad4d1
    # |    |- Red/Blue/Green/b                 9f6e04be05297905f1275d3f4e0bb0583458b2e8
    # |    `- Red/Blue/c                     * a28fa70e725ebda781e772795ca080cd737b823c
    # o - [1610644101] R0003                   ca6ec564c69efd2e5c70fb05486fd3f794765a04
    # |    |- Red/Green/a                      6dc7e44ead5c0e300fe94448c3e046dfe33ad4d1
    # |    |- Red/Green/b                      9f6e04be05297905f1275d3f4e0bb0583458b2e8
    # |    `- Red/a                            6dc7e44ead5c0e300fe94448c3e046dfe33ad4d1
    # o - [1610644103] R0004                   fc6e10b7d41b1d56a94091134e3683ce91e80d91
    # |    |- Red/Blue/Green/a                 6dc7e44ead5c0e300fe94448c3e046dfe33ad4d1
    # |    |- Red/Blue/Green/b                 9f6e04be05297905f1275d3f4e0bb0583458b2e8
    # |    `- Red/Blue/c                       a28fa70e725ebda781e772795ca080cd737b823c
    # o - [1610644105] R0005                   1d1fcf1816a8a2a77f9b1f342ba11d0fe9fd7f17
    # |    `- Purple/d                       * c0229d305adf3edf49f031269a70e3e87665fe88
    # o - [1610644107] R0006                   9a71f967ae1a125be9b6569cc4eccec0aecabb7c
    # |    `- Purple/Brown/Purple/d            c0229d305adf3edf49f031269a70e3e87665fe88
    # o - [1610644109] R0007                   4fde4ea4494a630030a4bda99d03961d9add00c7
    # |    |- Dark/Brown/Purple/d              c0229d305adf3edf49f031269a70e3e87665fe88
    # |    `- Dark/d                           c0229d305adf3edf49f031269a70e3e87665fe88
    # o - [1610644111] R0008                   ba00e89d47dc820bb32c783af7123ffc6e58b56d
    # |    |- Dark/Brown/Purple/d              c0229d305adf3edf49f031269a70e3e87665fe88
    # |    |- Dark/Brown/Purple/e              c0229d305adf3edf49f031269a70e3e87665fe88
    # |    `- Dark/a                           6dc7e44ead5c0e300fe94448c3e046dfe33ad4d1
    # o - [1610644113] R0009                   55d4dc9471de6144f935daf3c38878155ca274d5
    # |    |- Dark/Brown/Purple/f            * 94ba40161084e8b80943accd9d24e1f9dd47189b
    # |    |- Dark/Brown/Purple/g              94ba40161084e8b80943accd9d24e1f9dd47189b
    # |    `- Dark/f                           94ba40161084e8b80943accd9d24e1f9dd47189b
    # o - [1610644116] R0010                   a8939755d0be76cfea136e9e5ebce9bc51c49fef
    # |    |- Dark/Brown/Purple/f              94ba40161084e8b80943accd9d24e1f9dd47189b
    # |    |- Dark/Brown/Purple/g              94ba40161084e8b80943accd9d24e1f9dd47189b
    # |    `- Dark/h                         * 5e8f9ceaee9dafae2e3210e254fdf170295f8b5b
    # o - [1610644118] R0011                   ca1774a07b6e02c1caa7ae678924efa9259ee7c6
    # |    |- Paris/Brown/Purple/f             94ba40161084e8b80943accd9d24e1f9dd47189b
    # |    |- Paris/Brown/Purple/g             94ba40161084e8b80943accd9d24e1f9dd47189b
    # |    `- Paris/i                        * bbd54b961764094b13f10cef733e3725d0a834c3
    # o - [1610644120] R0012                   611fe71d75b6ea151b06e3845c09777acc783d82
    # |    |- Paris/Berlin/Purple/f            94ba40161084e8b80943accd9d24e1f9dd47189b
    # |    |- Paris/Berlin/Purple/g            94ba40161084e8b80943accd9d24e1f9dd47189b
    # |    `- Paris/j                        * 7ce4fe9a22f589fa1656a752ea371b0ebc2106b1
    # o - [1610644122] R0013                   4c5551b4969eb2160824494d40b8e1f6187fc01e
    #      |- Paris/Berlin/Purple/f            94ba40161084e8b80943accd9d24e1f9dd47189b
    #      |- Paris/Berlin/Purple/g            94ba40161084e8b80943accd9d24e1f9dd47189b
    #      |- Paris/Munich/Purple/f            94ba40161084e8b80943accd9d24e1f9dd47189b
    #      |- Paris/Munich/Purple/g            94ba40161084e8b80943accd9d24e1f9dd47189b
    #      |- Paris/Purple/f                   94ba40161084e8b80943accd9d24e1f9dd47189b
    #      |- Paris/Purple/g                   94ba40161084e8b80943accd9d24e1f9dd47189b
    #      `- Paris/k                        * cb79b39935c9392fa5193d9f84a6c35dc9c22c75
    data = {"revision": [], "directory": [], "content": []}
    with open(
        path.join(path.dirname(__file__), "data", "CMDBTS.msgpack"), "rb"
    ) as fobj:
        for etype, value in msgpack_loads(fobj.read()):
            data[etype].append(value)
    return data


def filter_dict(d, keys):
    return {k: v for (k, v) in d.items() if k in keys}


@pytest.fixture
def storage_and_CMDBTS(swh_storage, CMDBTS_data):
    swh_storage.content_add_metadata(
        Content.from_dict(content) for content in CMDBTS_data["content"]
    )
    swh_storage.directory_add(
        [
            Directory(
                entries=tuple(
                    [
                        DirectoryEntry.from_dict(
                            filter_dict(entry, ("name", "type", "target", "perms"))
                        )
                        for entry in dir["entries"]
                    ]
                )
            )
            for dir in CMDBTS_data["directory"]
        ]
    )
    swh_storage.revision_add(
        Revision.from_dict(revision) for revision in CMDBTS_data["revision"]
    )
    return swh_storage, CMDBTS_data
