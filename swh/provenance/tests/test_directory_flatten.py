# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from datetime import datetime, timezone

from swh.model.hashutil import hash_to_bytes
from swh.provenance.archive import ArchiveInterface
from swh.provenance.directory import directory_add
from swh.provenance.interface import (
    DirectoryData,
    ProvenanceInterface,
    RelationData,
    RelationType,
)
from swh.provenance.model import DirectoryEntry, FileEntry
from swh.provenance.tests.conftest import fill_storage, load_repo_data


def test_directory_add(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
) -> None:
    # read data/README.md for more details on how these datasets are generated
    data = load_repo_data("cmdbts2")
    fill_storage(archive.storage, data)

    # just take a directory that is known to exists in cmdbts2
    directory = DirectoryEntry(
        id=hash_to_bytes("48007c961cc734d1f63886d0413a6dc605e3e2ea")
    )
    content1 = FileEntry(
        id=hash_to_bytes("20329687bb9c1231a7e05afe86160343ad49b494"), name=b"a"
    )
    content2 = FileEntry(
        id=hash_to_bytes("50e9cdb03f9719261dd39d7f2920b906db3711a3"), name=b"b"
    )
    date = datetime.fromtimestamp(1000000010, timezone.utc)

    # directory_add and the internal directory_flatten require the directory and its
    # content to be known by the provenance object. Otherwise, they do nothing
    provenance.directory_set_date_in_isochrone_frontier(directory, date)
    provenance.content_set_early_date(content1, date)
    provenance.content_set_early_date(content2, date)
    provenance.flush()
    assert provenance.storage.directory_get([directory.id]) == {
        directory.id: DirectoryData(date=date, flat=False)
    }
    assert provenance.storage.content_get([content1.id, content2.id]) == {
        content1.id: date,
        content2.id: date,
    }

    # this query forces the directory date to be retrieved from the storage and cached
    # (otherwise, the flush below won't update the directory flatten flag)
    flattenned = provenance.directory_already_flattenned(directory)
    assert flattenned is not None and not flattenned

    # flatten the directory and check the expected result
    directory_add(provenance, archive, [directory])
    assert provenance.storage.directory_get([directory.id]) == {
        directory.id: DirectoryData(date=date, flat=True)
    }
    assert provenance.storage.relation_get_all(RelationType.CNT_IN_DIR) == {
        content1.id: {
            RelationData(dst=directory.id, path=b"a"),
            RelationData(dst=directory.id, path=b"C/a"),
        },
        content2.id: {RelationData(dst=directory.id, path=b"C/b")},
    }
