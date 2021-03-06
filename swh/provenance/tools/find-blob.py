#!/usr/bin/env python

import logging
import os
import sys

from swh.model.cli import identify_object
from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.provenance import get_provenance

# TODO: take conninfo as command line arguments.
conninfo = {
    "cls": "local",
    "db": {"host": "/var/run/postgresql", "port": "5436", "dbname": "provenance"},
}


if __name__ == "__main__":
    # Set minimum logging level to INFO.
    logging.getLogger().setLevel(logging.INFO)

    if len(sys.argv) < 2:
        print("usage: find-blob <filename> [limit]")
        exit(-1)

    obj, swhid = identify_object("content", True, True, sys.argv[1])
    sha1 = hash_to_bytes(swhid.split(":")[-1])
    print(f"Identifier of object {obj}: {swhid}")

    limit = sys.argv[2] if len(sys.argv) > 2 else None

    # Get provenance object.
    with get_provenance(**conninfo) as provenance:
        first = provenance.content_find_first(sha1)

        if first is not None:
            print(
                "======================================================================"
            )
            print(f"First occurrence of {obj}:")
            print(
                f" content: swh:1:cnt:{hash_to_hex(first[0])},"
                f" revision: swh:1:rev:{hash_to_hex(first[1])},"
                f" date: {first[2]},"
                f" location: {os.fsdecode(first[3])}"
            )

            print(
                "======================================================================"
            )
            if limit is None:
                print(f"All occurrences of {obj}:")
            else:
                print(f"First {limit} occurrences of {obj}:")
            for occur in provenance.content_find_all(sha1, limit=limit):
                print(
                    f" content: swh:1:cnt:{hash_to_hex(occur[0])},"
                    f" revision: swh:1:rev:{hash_to_hex(occur[1])},"
                    f" date: {occur[2]},"
                    f" location: {os.fsdecode(occur[3])}"
                )

        else:
            logging.warning(
                "Requested content not available in the provenance database."
            )
