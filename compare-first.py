#!/usr/bin/env python

import glob
import io
import logging
import os

from swh.model.hashutil import hash_to_hex
from swh.provenance import get_provenance
from swh.provenance.interface import EntityType

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
def logdiff(filename, occurrence):
    with io.open(filename, "a") as outfile:
        try:
            # Try to decode path.
            path = os.fsdecode(occurrence[3]).decode("utf-8", "replace")
        except:
            # Use its raw value if not possible
            path = occurrence[3]
        outfile.write(
            "{blob},{rev},{date},{path}\n".format(
                blob=hash_to_hex(occurrence[0]),
                rev=hash_to_hex(occurrence[1]),
                date=occurrence[2],
                path=path,
            )
        )


# Write log file with list of occurrences.
def loglist(filename, occurrences):
    with io.open(filename, "a") as outfile:
        for blobid in occurrences:
            outfile.write("{blob}\n".format(blob=hash_to_hex(blobid)))


# Output log file name.
nextidx = None


def outfilename(suffix):
    global nextidx
    basename, _ = os.path.splitext(os.path.basename(os.path.abspath(__file__)))
    prefix = os.path.join(os.getcwd(), basename + "-")
    if nextidx is None:
        nextidx = 0
        for filename in glob.glob(f"{prefix}*.log"):
            try:
                lastidx = int(filename.strip(prefix).split("-")[0])
                nextidx = max(nextidx, lastidx + 1)
            except:
                continue
    return f"{prefix}{nextidx:02}-{suffix}.log"


# Print iterations progress.
# TODO: move to utils module.
def progress(
    iteration,
    total,
    prefix="Progress:",
    suffix="Complete",
    decimals=1,
    length=50,
    fill="█",
    printEnd="\r",
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
    provenance1 = get_provenance(**conninfo1)
    provenance2 = get_provenance(**conninfo2)

    content1 = provenance1.storage.entity_get_all(EntityType.CONTENT)
    content2 = provenance2.storage.entity_get_all(EntityType.CONTENT)

    if content1 == content2:
        # If lists of content match, we check that occurrences does as well.
        total = len(content1)
        progress(0, total)

        mismatch = False
        # Iterate over all content querying all its occurrences on both databases.
        for i, blobid in enumerate(content1):
            occurrence1 = provenance1.content_find_first(blobid)
            occurrence2 = provenance2.content_find_first(blobid)

            # If there is a mismatch log it to file. We can only compare the timestamp
            # as the same blob might be seen for the first time in different locations.
            if occurrence1[2] != occurrence2[2]:
                mismatch = True
                logging.warning(f"Occurrencies mismatch for {hash_to_hex(blobid)}")
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
