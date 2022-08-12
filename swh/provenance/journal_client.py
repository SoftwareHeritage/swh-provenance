# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

try:
    from systemd.daemon import notify
except ImportError:
    notify = None

import sentry_sdk

from swh.model.model import TimestampWithTimezone
from swh.provenance.archive import ArchiveInterface
from swh.provenance.interface import ProvenanceInterface
from swh.provenance.model import OriginEntry, RevisionEntry
from swh.provenance.origin import origin_add
from swh.provenance.revision import revision_add

EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def process_journal_origins(
    messages, *, provenance: ProvenanceInterface, archive: ArchiveInterface, **cfg
) -> None:
    """Worker function for `JournalClient.process(worker_fn)`."""
    assert set(messages) == {"origin_visit_status"}, set(messages)
    origin_entries = [
        OriginEntry(url=visit["origin"], snapshot=visit["snapshot"])
        for visit in messages["origin_visit_status"]
        if visit["snapshot"] is not None
    ]
    if origin_entries:
        origin_add(provenance, archive, origin_entries, **cfg)
    if notify:
        notify("WATCHDOG=1")


def process_journal_revisions(
    messages, *, provenance: ProvenanceInterface, archive: ArchiveInterface, **cfg
) -> None:
    """Worker function for `JournalClient.process(worker_fn)`."""
    assert set(messages) == {"revision"}, set(messages)
    revisions = []
    for rev in messages["revision"]:
        if rev["date"] is None:
            continue
        try:
            date = TimestampWithTimezone.from_dict(rev["date"]).to_datetime()
        except Exception:
            sentry_sdk.capture_exception()
            continue

        if date <= EPOCH:
            continue

        revisions.append(
            RevisionEntry(
                id=rev["id"],
                root=rev["directory"],
                date=date,
            )
        )
    if revisions:
        revision_add(provenance, archive, revisions, **cfg)
    if notify:
        notify("WATCHDOG=1")
