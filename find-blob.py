#!/usr/bin/env python

import logging
import os
import sys

from swh.model.cli import identify_object
from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.provenance import get_provenance


# TODO: take conninfo as command line arguments.
conninfo = {
    "cls": "ps",
    "db": {"host": "/var/run/postgresql", "port": "5436", "dbname": "lower1m"},
}


if __name__ == "__main__":
    # Set minimum logging level to INFO.
    logging.getLogger().setLevel(logging.INFO)

    if len(sys.argv) != 2:
        print("usage: find-blob <filename>")
        exit(-1)

    # Get provenance object for both databases and query its lists of content.
    provenance = get_provenance(**conninfo)

    obj, swhid = identify_object("content", True, True, sys.argv[1])
    sha1 = hash_to_bytes(swhid.split(":")[-1])
    print(f"Identifier of object {obj}: {swhid}")

    first = provenance.content_find_first(sha1)

    if first is not None:
        print("===============================================================================")
        print(f"First occurrence of {obj}:")
        print(
            "   content: {blob}, revision: {rev}, date: {date}, location: {path}".format(
                blob=hash_to_hex(first[0]),
                rev=hash_to_hex(first[1]),
                date=first[2],
                path=os.fsdecode(first[3]),
            )
        )

        print("===============================================================================")
        print(f"All occurrences of {obj}:")
        for occur in provenance.content_find_all(sha1):
            print(
                "   content: {blob}, revision: {rev}, date: {date}, location: {path}".format(
                    blob=hash_to_hex(occur[0]),
                    rev=hash_to_hex(occur[1]),
                    date=occur[2],
                    path=os.fsdecode(occur[3]),
                )
            )

    else:
        logging.warning("Requested content not available in the provenance database.")
