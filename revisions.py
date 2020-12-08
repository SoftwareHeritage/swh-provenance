import io
import random
import pytz

from datetime import datetime

from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.storage import get_storage
from swh.provenance.revision import RevisionEntry


def rev_to_csv(revision: RevisionEntry):
    return (
        ",".join(
            [
                hash_to_hex(revision.id),
                str(pytz.utc.localize(revision.date)),
                hash_to_hex(revision.root),
            ]
        )
        + "\n"
    )


if __name__ == "__main__":
    conninfo = {
        "cls": "remote",
        "url": "http://uffizi.internal.softwareheritage.org:5002",
    }
    storage = get_storage(**conninfo)

    revisions = [
        # '6eec5815ef8fc88d9fc5bcc91c6465a8899c1445',
        # 'd1468bb5f06ca44cc42c43fbd011c5dcbdc262c6',
        # '6a45ebb887d87ee53f359aaeba8a9840576c907b'
        "02f95c0a1868cbef82ff73fc1b903183a579c7de",
        "da061f1caf293a5da00bff6a45abcf4d7ae54c50",
        "e3bfd73a9fd8ef3dd4c5b05a927de485f9871323",
    ]
    print(revisions)

    revisions = list(map(hash_to_bytes, revisions))
    print(revisions)

    entries = []
    for revision in storage.revision_get(revisions):
        if revision is not None:
            print(revision)
            entries.append(
                RevisionEntry(
                    storage,
                    revision.id,
                    datetime.fromtimestamp(revision.date.timestamp.seconds),
                    revision.directory,
                )
            )

    random.shuffle(entries)
    with io.open("random.csv", "w") as outfile:
        for revision in entries:
            outfile.write(rev_to_csv(revision))

    with io.open("ordered.csv", "w") as outfile:
        for revision in sorted(entries, key=lambda rev: rev.date):
            outfile.write(rev_to_csv(revision))

    with io.open("reverse.csv", "w") as outfile:
        for revision in sorted(entries, key=lambda rev: rev.date, reverse=True):
            outfile.write(rev_to_csv(revision))
