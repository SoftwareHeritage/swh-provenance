# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model.hashutil import hash_to_bytes
from swh.provenance.algos.revision import revision_add
from swh.provenance.archive import ArchiveInterface
from swh.provenance.interface import ProvenanceInterface
from swh.provenance.model import RevisionEntry
from swh.provenance.storage.interface import DirectoryData, ProvenanceResult

from .utils import fill_storage, load_repo_data, ts2dt


def test_consistency(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
) -> None:
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data("cmdbts2")
    fill_storage(archive.storage, data)

    revisions = {rev["id"]: rev for rev in data["revision"]}

    # Process R00 first as expected
    rev_00 = revisions[hash_to_bytes("c0d8929936631ecbcf9147be6b8aa13b13b014e4")]
    r00 = RevisionEntry(
        id=rev_00["id"],
        date=ts2dt(rev_00["date"]),
        root=rev_00["directory"],
    )
    revision_add(provenance, archive, [r00])

    # Register contents A/B/C/b from R01 in the storage to simulate a crash
    rev_01 = revisions[hash_to_bytes("1444db96cbd8cd791abe83527becee73d3c64e86")]
    r01 = RevisionEntry(
        id=rev_01["id"],
        date=ts2dt(rev_01["date"]),
        root=rev_01["directory"],
    )
    assert r01.date is not None  # for mypy
    cnt_b_sha1 = hash_to_bytes("50e9cdb03f9719261dd39d7f2920b906db3711a3")
    provenance.storage.content_add({cnt_b_sha1: r01.date})

    # Process R02 (this should set a frontier in directory C)
    rev_02 = revisions[hash_to_bytes("0d45f1ee524db8f6f0b5a267afac4e733b4b2cee")]
    r02 = RevisionEntry(
        id=rev_02["id"],
        date=ts2dt(rev_02["date"]),
        root=rev_02["directory"],
    )
    assert r02.date is not None  # for mypy
    revision_add(provenance, archive, [r02])

    dir_C_sha1 = hash_to_bytes("c9cabe7f49012e3fdef6ac6b929efb5654f583cf")
    assert provenance.storage.directory_get([dir_C_sha1]) == {
        dir_C_sha1: DirectoryData(r01.date, True)
    }
    assert provenance.content_find_first(cnt_b_sha1) is None  # No first occurrence
    assert set(provenance.content_find_all(cnt_b_sha1)) == {
        ProvenanceResult(
            content=cnt_b_sha1,
            revision=r02.id,
            date=r02.date,
            origin=None,
            path=b"A/B/C/b",
        )
    }

    # Process R01 out of order (frontier in C should not be reused to guarantee that the
    # first occurrence of A/B/C/b is in the CNT_EARLY_IN_REV relation)
    revision_add(provenance, archive, [r01])

    assert provenance.content_find_first(cnt_b_sha1) == ProvenanceResult(
        content=cnt_b_sha1, revision=r01.id, date=r01.date, origin=None, path=b"A/B/C/b"
    )
    assert set(provenance.content_find_all(cnt_b_sha1)) == {
        ProvenanceResult(
            content=cnt_b_sha1,
            revision=r01.id,
            date=r01.date,
            origin=None,
            path=b"A/B/C/b",
        ),
        ProvenanceResult(
            content=cnt_b_sha1,
            revision=r02.id,
            date=r02.date,
            origin=None,
            path=b"A/B/C/b",
        ),
    }
