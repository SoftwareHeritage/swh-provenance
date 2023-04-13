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

from swh.provenance.algos.origin import origin_add
from swh.provenance.algos.revision import revision_add
from swh.provenance.archive import ArchiveInterface
from swh.provenance.interface import ProvenanceInterface
from swh.provenance.model import OriginEntry, RevisionEntry

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
        if not rev["date"]:
            continue
        try:
            reventry = RevisionEntry.from_revision_dict(rev)
        except Exception:
            sentry_sdk.capture_exception()
            continue

        if reventry.date <= EPOCH:
            continue

        revisions.append(reventry)

    if revisions:
        revision_add(provenance, archive, revisions, **cfg)
    if notify:
        notify("WATCHDOG=1")


def process_journal_releases(
    messages, *, provenance: ProvenanceInterface, archive: ArchiveInterface, **cfg
) -> None:
    """Worker function for `JournalClient.process(worker_fn)`."""
    assert set(messages) == {"release"}, set(messages)
    rev_ids = []
    for rel in messages["release"]:
        if rel["target"] is None:
            continue

        if rel["target_type"] == "revision":
            rev_ids.append(rel["target"])

    revisions = []
    for (rev_id, directory, date_d) in archive.revisions_get(rev_ids):
        rev = {"id": rev_id, "directory": directory, "date": date_d}
        if not rev["date"]:
            continue
        try:
            reventry = RevisionEntry.from_revision_dict(rev)
        except Exception:
            sentry_sdk.capture_exception()
            continue

        if reventry.date <= EPOCH:
            continue

        revisions.append(reventry)

    if revisions:
        revision_add(provenance, archive, revisions, **cfg)
    if notify:
        notify("WATCHDOG=1")
