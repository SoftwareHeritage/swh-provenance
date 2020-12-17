#!/usr/bin/env python

import io
import logging
import os
import psycopg2

from swh.model.hashutil import hash_to_hex
from swh.provenance import get_provenance


conninfo1 = {
    "cls": "ps",
    "db":
    {
        "host": "/var/run/postgresql",
        "port": "5436",
        "dbname": "old"
    }
}
conninfo2 = {
    "cls": "ps",
    "db":
    {
        "host": "/var/run/postgresql",
        "port": "5436",
        "dbname": "test"
    }
}


# Print iterations progress.
def printProgressBar(
    iteration,
    total,
    prefix = 'Progress:',
    suffix = 'Complete',
    decimals = 1,
    length = 50,
    fill = '█',
    printEnd = "\r"
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
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()


# Output log file name.
def outfilename(suffix):
    basename, _ = os.path.splitext(os.path.basename(os.path.abspath(__file__)))
    return os.path.join(os.getcwd(), basename + '-' + suffix + '.log')


# Write log file.
def writeLogFile(filename, occurrences):
    with io.open(filename, 'a') as outfile:
        for row in occurrences:
            try:
                # Try to decode path.
                path = os.fsdecode(row[3]).decode('utf-8', 'replace')
            except:
                # Use its raw value if not possible
                path = row[3]

            outfile.write(
                "{blob}, {rev}, {date}, {path}\n".format(
                    blob=hash_to_hex(row[0]),
                    rev=hash_to_hex(row[1]),
                    date=row[2],
                    path=path,
                )
            )


if __name__ == "__main__":
    # Clear output from previous executions.
    outfile1 = outfilename(conninfo1['db']['dbname'])
    outfile2 = outfilename(conninfo2['db']['dbname'])

    if os.path.exists(outfile1): os.remove(outfile1)
    if os.path.exists(outfile2): os.remove(outfile2)

    # Get provenance object for both databases and query its lists of content.
    provenance1 = get_provenance(**conninfo1)
    provenance2 = get_provenance(**conninfo2)

    provenance1.cursor.execute('''SELECT id FROM content ORDER BY id''')
    content1 = set(map(lambda row: row[0], provenance1.cursor.fetchall()))

    provenance2.cursor.execute('''SELECT id FROM content ORDER BY id''')
    content2 = set(map(lambda row: row[0], provenance2.cursor.fetchall()))

    if content1 == content2:
        # If lists of content match, we check that occurrences does as well.
        total = len(content1)
        printProgressBar(0, total)

        # Iterate over all content querying all its occurrences on both databases.
        for i, blob in enumerate(content1):
            occurrences1 = list(provenance1.content_find_all(blob))
            occurrences2 = list(provenance2.content_find_all(blob))

            # If there is a mismatch log it to file.
            if (
                len(occurrences1) != len(occurrences2) or
                set(occurrences1) != set(occurrences2)
            ):
                writeLogFile(outfile1, occurrences1)
                writeLogFile(outfile2, occurrences2)

            printProgressBar(i + 1, total)

    else:
        # If lists of content don't match, we are done.
        # TODO: maybe log difference?
        logging.warning("Content lists are different")
