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
        "dbname": "revisited"
    }
}


# Print iterations progress
def printProgressBar(iteration, total, prefix = 'Progress:', suffix = 'Complete', decimals = 1, length = 50, fill = '█', printEnd = "\r"):
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


if __name__ == "__main__":
    provenance1 = get_provenance(**conninfo1)
    provenance2 = get_provenance(**conninfo2)

    provenance1.cursor.execute('''SELECT id FROM content ORDER BY id''')
    content1 = set(map(lambda row: row[0], provenance1.cursor.fetchall()))

    provenance2.cursor.execute('''SELECT id FROM content ORDER BY id''')
    content2 = set(map(lambda row: row[0], provenance2.cursor.fetchall()))

    if content1 == content2:
        total = len(content1)
        printProgressBar(0, total)

        for i, blob in enumerate(content1):
            occurrences1 = set(provenance1.content_find_all(blob))
            occurrences2 = set(provenance2.content_find_all(blob))

            if occurrences1 != occurrences2:
                with io.open(conninfo1['db']['dbname'] + '.log', 'a') as outfile:
                    for row in occurrences1:
                        outfile.write(
                            "{blob}, {rev}, {date}, {path}\n".format(
                                blob=hash_to_hex(row[0]),
                                rev=hash_to_hex(row[1]),
                                date=row[2],
                                path=os.fsdecode(row[3]),
                            )
                        )
                with io.open(conninfo2['db']['dbname'] + '.log', 'a') as outfile:
                    for row in occurrences2:
                        outfile.write(
                            "{blob}, {rev}, {date}, {path}\n".format(
                                blob=hash_to_hex(row[0]),
                                rev=hash_to_hex(row[1]),
                                date=row[2],
                                path=os.fsdecode(row[3]),
                            )
                        )

            printProgressBar(i + 1, total)

    else:
        logging.warning("Content lists are different")
