# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
from typing import List, Optional, Tuple, Union

from swh.model.hashutil import hash_to_bytes
from swh.model.model import Sha1Git
from swh.provenance.storage.interface import DirectoryData, RelationData, RevisionData
from swh.provenance.storage.rabbitmq.server import (
    resolve_dates,
    resolve_directory,
    resolve_relation,
    resolve_revision,
)


def test_resolve_dates() -> None:
    items: List[Tuple[Sha1Git, datetime]] = [
        (
            hash_to_bytes("20329687bb9c1231a7e05afe86160343ad49b494"),
            datetime.fromtimestamp(1000000001),
        ),
        (
            hash_to_bytes("20329687bb9c1231a7e05afe86160343ad49b494"),
            datetime.fromtimestamp(1000000000),
        ),
    ]
    assert resolve_dates(items) == {
        hash_to_bytes(
            "20329687bb9c1231a7e05afe86160343ad49b494"
        ): datetime.fromtimestamp(1000000000)
    }


def test_resolve_directory() -> None:
    items: List[Tuple[Sha1Git, DirectoryData]] = [
        (
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            DirectoryData(date=datetime.fromtimestamp(1000000002), flat=False),
        ),
        (
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            DirectoryData(date=datetime.fromtimestamp(1000000001), flat=True),
        ),
        (
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            DirectoryData(date=datetime.fromtimestamp(1000000000), flat=False),
        ),
    ]
    assert resolve_directory(items) == {
        hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"): DirectoryData(
            date=datetime.fromtimestamp(1000000000), flat=True
        )
    }


def test_resolve_revision_without_date() -> None:
    items: List[Union[Tuple[Sha1Git, RevisionData], Tuple[Sha1Git]]] = [
        (hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),),
        (
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            RevisionData(
                date=None,
                origin=hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8175"),
            ),
        ),
    ]
    assert resolve_revision(items) == {
        hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"): RevisionData(
            date=None,
            origin=hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8175"),
        )
    }


def test_resolve_revision_without_origin() -> None:
    items: List[Union[Tuple[Sha1Git, RevisionData], Tuple[Sha1Git]]] = [
        (hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),),
        (
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            RevisionData(date=datetime.fromtimestamp(1000000000), origin=None),
        ),
    ]
    assert resolve_revision(items) == {
        hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"): RevisionData(
            date=datetime.fromtimestamp(1000000000),
            origin=None,
        )
    }


def test_resolve_revision_merge() -> None:
    items: List[Union[Tuple[Sha1Git, RevisionData], Tuple[Sha1Git]]] = [
        (
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            RevisionData(date=datetime.fromtimestamp(1000000000), origin=None),
        ),
        (
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            RevisionData(
                date=None,
                origin=hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8175"),
            ),
        ),
    ]
    assert resolve_revision(items) == {
        hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"): RevisionData(
            date=datetime.fromtimestamp(1000000000),
            origin=hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8175"),
        )
    }


def test_resolve_revision_keep_min_date() -> None:
    items: List[Union[Tuple[Sha1Git, RevisionData], Tuple[Sha1Git]]] = [
        (
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            RevisionData(
                date=datetime.fromtimestamp(1000000000),
                origin=hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"),
            ),
        ),
        (
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            RevisionData(
                date=datetime.fromtimestamp(1000000001),
                origin=hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8175"),
            ),
        ),
    ]
    assert resolve_revision(items) == {
        hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"): RevisionData(
            date=datetime.fromtimestamp(1000000000),
            origin=hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8175"),
        )
    }


def test_resolve_relation() -> None:
    items: List[Tuple[Sha1Git, Sha1Git, Optional[bytes], Optional[datetime]]] = [
        (
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"),
            b"/path/1",
            datetime.fromtimestamp(1000000001),
        ),
        (
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"),
            b"/path/2",
            None,
        ),
        (
            hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"),
            hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"),
            b"/path/1",
            datetime.fromtimestamp(1000000001),
        ),
    ]
    assert resolve_relation(items) == {
        hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4"): {
            RelationData(
                hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"),
                b"/path/1",
                dst_date=datetime.fromtimestamp(1000000001),
            ),
            RelationData(
                hash_to_bytes("3acef14580ea7fd42840ee905c5ce2b0ef9e8174"),
                b"/path/2",
                dst_date=None,
            ),
        }
    }
