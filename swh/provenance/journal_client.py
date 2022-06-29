# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.provenance.interface import ProvenanceInterface
from swh.provenance.model import OriginEntry
from swh.provenance.origin import origin_add
from swh.storage.interface import StorageInterface


def process_journal_objects(
    messages, *, provenance: ProvenanceInterface, archive: StorageInterface
) -> None:
    """Worker function for `JournalClient.process(worker_fn)`."""
    assert set(messages) == {"origin_visit_status"}, set(messages)
    origin_entries = [
        OriginEntry(url=visit["origin"], snapshot=visit["snapshot"])
        for visit in messages["origin_visit_status"]
        if visit["snapshot"] is not None
    ]
    with provenance:
        origin_add(provenance, archive, origin_entries)
