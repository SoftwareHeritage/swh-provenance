#!/usr/bin/env python

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
    # TODO: make this a command line parameter
    filename = "../../swh-provenance/data/ordered.csv"
    limit = None
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
