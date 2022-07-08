# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model.model import TimestampWithTimezone
from swh.provenance.interface import ProvenanceInterface
from swh.provenance.model import OriginEntry, RevisionEntry
from swh.provenance.origin import origin_add
from swh.provenance.revision import revision_add
from swh.storage.interface import StorageInterface


def process_journal_origins(
    messages, *, provenance: ProvenanceInterface, archive: StorageInterface, **cfg
) -> None:
    """Worker function for `JournalClient.process(worker_fn)`."""
    assert set(messages) == {"origin_visit_status"}, set(messages)
    origin_entries = [
        OriginEntry(url=visit["origin"], snapshot=visit["snapshot"])
        for visit in messages["origin_visit_status"]
        if visit["snapshot"] is not None
    ]
    if origin_entries:
        with provenance:
            origin_add(provenance, archive, origin_entries, **cfg)


def process_journal_revisions(
    messages, *, provenance: ProvenanceInterface, archive: StorageInterface, **cfg
) -> None:
    """Worker function for `JournalClient.process(worker_fn)`."""
    assert set(messages) == {"revision"}, set(messages)
    revisions = [
        RevisionEntry(
            id=rev["id"],
            date=TimestampWithTimezone.from_dict(rev["date"]).to_datetime(),
            root=rev["directory"],
            parents=rev["parents"],
        )
        for rev in messages["revision"]
        if rev["date"] is not None
    ]
    if revisions:
        with provenance:
            revision_add(provenance, archive, revisions, **cfg)
