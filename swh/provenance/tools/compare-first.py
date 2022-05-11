#!/usr/bin/env python

import glob
import io
import logging
import os
from typing import Iterable

from swh.model.hashutil import hash_to_hex
from swh.model.model import Sha1Git
from swh.provenance import get_provenance
from swh.provenance.interface import EntityType, ProvenanceResult

# TODO: take conninfo as command line arguments.
conninfo1 = {
    "cls": "local",
    "db": {"host": "/var/run/postgresql", "port": "5436", "dbname": "old"},
}
conninfo2 = {
    "cls": "local",
    "db": {"host": "/var/run/postgresql", "port": "5436", "dbname": "provenance"},
}


# Write log file with occurrence detail.
def logdiff(filename: str, occurrence: ProvenanceResult) -> None:
    with io.open(filename, "a") as outfile:
        try:
            # Try to decode path.
            path = os.fsdecode(occurrence.path).decode("utf-8", "replace")
        except Exception:
            # Use its raw value if not possible
            path = occurrence.path
        outfile.write(
            "{blob},{rev},{date},{path}\n".format(
                blob=hash_to_hex(occurrence.content),
                rev=hash_to_hex(occurrence.revision),
                date=occurrence.date,
                path=path,
            )
        )


# Write log file with list of occurrences.
def loglist(filename: str, occurrences: Iterable[Sha1Git]) -> None:
    with io.open(filename, "a") as outfile:
        for sha1 in occurrences:
            outfile.write("{blob}\n".format(blob=hash_to_hex(sha1)))


# Output log file name.
nextidx = None


def outfilename(suffix: str) -> str:
    global nextidx
    basename, _ = os.path.splitext(os.path.basename(os.path.abspath(__file__)))
    prefix = os.path.join(os.getcwd(), basename + "-")
    if nextidx is None:
        nextidx = 0
        for filename in glob.glob(f"{prefix}*.log"):
            try:
                lastidx = int(filename.strip(prefix).split("-")[0])
                nextidx = max(nextidx, lastidx + 1)
            except Exception:
                continue
    return f"{prefix}{nextidx:02}-{suffix}.log"


# Print iterations progress.
# TODO: move to utils module.
def progress(
    iteration: int,
    total: int,
    prefix: str = "Progress:",
    suffix: str = "Complete",
    decimals: int = 1,
    length: int = 50,
    fill: str = "█",
    printEnd: str = "\r",
):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + "-" * (length - filledLength)
    print(f"\r{prefix} |{bar}| {percent}% {suffix}", end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()


if __name__ == "__main__":
    # Set minimum logging level to INFO.
    logging.getLogger().setLevel(logging.INFO)

    # Get provenance object for both databases and query its lists of content.
    with get_provenance(**conninfo1) as provenance1:
        with get_provenance(**conninfo2) as provenance2:
            content1 = provenance1.storage.entity_get_all(EntityType.CONTENT)
            content2 = provenance2.storage.entity_get_all(EntityType.CONTENT)

            if content1 == content2:
                # If lists of content match, we check that occurrences does as well.
                total = len(content1)
                progress(0, total)

                mismatch = False
                # Iterate over all content querying its first occurrences
                # on both databases.
                for i, sha1 in enumerate(content1):
                    occurrence1 = provenance1.content_find_first(sha1)
                    occurrence2 = provenance2.content_find_first(sha1)

                    # If there is a mismatch log it to file. We can only compare the
                    # timestamp as the same blob might be seen for the first time in
                    # different locations.
                    if occurrence1.date != occurrence2.date:
                        mismatch = True
                        logging.warning(
                            f"Occurrencies mismatch for {hash_to_hex(sha1)}"
                        )
                        logdiff(outfilename(conninfo1["db"]["dbname"]), occurrence1)
                        logdiff(outfilename(conninfo2["db"]["dbname"]), occurrence2)

                    progress(i + 1, total)

                if not mismatch:
                    logging.info("Databases are equivalent!")

            else:
                # If lists of content don't match, we are done.
                loglist(outfilename(conninfo1["db"]["dbname"]), content1)
                loglist(outfilename(conninfo2["db"]["dbname"]), content2)
                logging.warning("Content lists are different")
