#!/usr/bin/env python

import gzip
import sys
from datetime import datetime

from swh.model.hashutil import hash_to_bytes, hash_to_hex

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: revisions_sort <infile> <outfile>")
        exit(-1)

    infilename = sys.argv[1]
    outfilename = sys.argv[2]

    with gzip.open(infilename, "rt") as infile:
        revisions = []
        sort = False
        for idx, line in enumerate(infile.readlines(), start=1):
            if line.strip():
                splitted = line.split(",")
                revision = hash_to_bytes(splitted[0])
                date = datetime.fromisoformat(splitted[1])
                root = hash_to_bytes(splitted[2])

                assert date is not None

                if revisions:
                    last = revisions[-1]
                    if date < last[1]:
                        print("Out of order", last, f"({revision},{date},{root})")
                        sort = True

                revisions.append((revision, date, root))

        if sort:
            revisions = sorted(revisions, key=lambda rev: rev[1])

            date = None
            with gzip.open(outfilename, "wt") as outfile:
                for rev in revisions:
                    assert date == None or date <= rev[1]
                    date = rev[1]
                    outfile.write(
                        f"{hash_to_hex(rev[0])},{rev[1]},{hash_to_hex(rev[2])}\n"
                    )
