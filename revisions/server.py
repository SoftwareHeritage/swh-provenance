#!/usr/bin/env python

import sys
import zmq

from swh.model.hashutil import hash_to_hex
from swh.provenance import get_archive
from swh.provenance.revision import FileRevisionIterator


# TODO: take this from a configuration file
conninfo = {
    "archive": {
        "cls": "ps",
        "db": {
            "host": "somerset.internal.softwareheritage.org",
            "port": "5433",
            "dbname": "softwareheritage",
            "user": "guest"
        }
    }
}

if __name__ == "__main__":
    # Set minimum logging level to INFO.
    logging.getLogger().setLevel(logging.INFO)

    if len(sys.argv) < 2:
        print("usage: server <filename> [limit]")
        print("where")
        print("    filename     : csv file containing the list of revisions to be iterated (one per")
        print("                   line): revision sha1, date in ISO format, root directory sha1.")
        print("    limit        : max number of revisions to be retrieved from the file.")
        exit(-1)

    filename = sys.arv[1]
    limit = int(sys.arv[2]) if len(sys.argv) > 2 else None
    port = 5556
    
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{port}")

    archive = get_archive(**conninfo["archive"])
    revisions = FileRevisionIterator(filename, archive, limit=limit)
    while True:
        revision = revisions.next()
        if revision is None:
            break

        # Wait for next request from client
        message = socket.recv()
        message = {
            "rev" : hash_to_hex(revision.id),
            "date" : str(revision.date),
            "root" : hash_to_hex(revision.root)
        }
        socket.send_json(message)

    while True:
        # Force all clients to exit
        message = socket.recv()
        socket.send_json(None)
