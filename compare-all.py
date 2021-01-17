#!/usr/bin/env python

import glob
import io
import logging
import os
import psycopg2

from swh.model.hashutil import hash_to_hex
from swh.provenance import get_provenance


# TODO: take conninfo as command line arguments.
conninfo1 = {
    "cls": "ps",
    "db": {"host": "/var/run/postgresql", "port": "5436", "dbname": "old"},
}
conninfo2 = {
    "cls": "ps",
    "db": {"host": "/var/run/postgresql", "port": "5436", "dbname": "withids"},
}


# Write log file with occurrence detail.
def logdiff(filename, occurrences):
    with io.open(filename, "a") as outfile:
        for row in occurrences:
            try:
                # Try to decode path.
                path = os.fsdecode(row[3]).decode("utf-8", "replace")
            except:
                # Use its raw value if not possible
                path = row[3]
            outfile.write(
                "{blob},{rev},{date},{path}\n".format(
                    blob=hash_to_hex(row[0]),
                    rev=hash_to_hex(row[1]),
                    date=row[2],
                    path=path,
                )
            )


# Write log file with list of occurrences.
def loglist(filename, occurrences):
    with io.open(filename, "a") as outfile:
        for blobid in occurrences:
            outfile.write(
                "{blob}\n".format(
                    blob=hash_to_hex(blobid)
                )
            )


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

    provenance1.cursor.execute("""SELECT id FROM content ORDER BY id""")
    content1 = set(map(lambda row: row[0], provenance1.cursor.fetchall()))

    provenance2.cursor.execute("""SELECT sha1 FROM content ORDER BY sha1""")
    content2 = set(map(lambda row: row[0], provenance2.cursor.fetchall()))

    if content1 == content2:
        # If lists of content match, we check that occurrences does as well.
        total = len(content1)
        progress(0, total)

        mismatch = False
        # Iterate over all content querying all its occurrences on both databases.
        for i, blobid in enumerate(content1):
            provenance1.cursor.execute(
	            """(SELECT content_early_in_rev.blob,
                           content_early_in_rev.rev, 
                           revision.date,
                           content_early_in_rev.path
                     FROM content_early_in_rev
                     JOIN revision
                       ON revision.id=content_early_in_rev.rev
                     WHERE content_early_in_rev.blob=%s
                   )
                   UNION
                   (SELECT content_in_rev.blob,
                           content_in_rev.rev,
                           revision.date,
                           content_in_rev.path
                     FROM (SELECT content_in_dir.blob,
                                  directory_in_rev.rev,
                                  CASE directory_in_rev.path
                                    WHEN '' THEN content_in_dir.path
                                    WHEN '.' THEN content_in_dir.path
                                    ELSE (directory_in_rev.path || '/' ||
                                             content_in_dir.path)::unix_path
                                  END AS path
                            FROM content_in_dir
                            JOIN directory_in_rev
                              ON content_in_dir.dir=directory_in_rev.dir
                            WHERE content_in_dir.blob=%s
                          ) AS content_in_rev
                     JOIN revision
                       ON revision.id=content_in_rev.rev
                   )
                   ORDER BY date, rev, path""",
                (blobid, blobid),
            )
            occurrences1 = list(provenance1.cursor.fetchall())
            occurrences2 = list(provenance2.content_find_all(blobid))

            # If there is a mismatch log it to file.
            if (
                len(occurrences1) != len(occurrences2) or
                set(occurrences1) != set(occurrences2)
            ):
                mismatch = True
                logging.warning(f"Occurrencies mismatch for {hash_to_hex(blobid)}")
                logdiff(outfilename(conninfo1["db"]["dbname"]), occurrences1)
                logdiff(outfilename(conninfo2["db"]["dbname"]), occurrences2)

            progress(i + 1, total)

        if not mismatch:
            logging.info("Databases are equivalent!")

    else:
        # If lists of content don't match, we are done.
        loglist(outfilename(conninfo1["db"]["dbname"]), content1)
        loglist(outfilename(conninfo2["db"]["dbname"]), content2)
        logging.warning("Content lists are different")
