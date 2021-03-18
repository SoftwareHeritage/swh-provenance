#!/usr/bin/env python

import glob
import io
import logging
import os
import psycopg2

from swh.model.hashutil import hash_to_hex
from swh.provenance import get_provenance


# TODO: take conninfo as command line arguments.
conninfo = {
    "cls": "local",
    "db": {"host": "/var/run/postgresql", "port": "5436", "dbname": "provenance"},
}


def dump(type, hash, time, path="", header="", table=""):
    return f"{str(header).ljust(5)} | {str(table).ljust(5)} | {str(path).ljust(30)} | {type} {hash_to_hex(hash)} | {str(time).rjust(10)}"


if __name__ == "__main__":
    # Set minimum logging level to INFO.
    logging.getLogger().setLevel(logging.INFO)

    # Get provenance object for both databases and query its lists of content.
    provenance = get_provenance(**conninfo)

    provenance.cursor.execute("""SELECT sha1, date FROM revision ORDER BY date""")
    revisions = list(provenance.cursor.fetchall())

    for idx, (revision, date) in enumerate(revisions):
        # Display current revision information.
        header = f"R{idx:04}"
        timestamp = date.timestamp()
        print(f"{timestamp} {hash_to_hex(revision)} {header}")
        print(dump("R", revision, timestamp, header=header))

        # Display content found early in current revision.
        provenance.cursor.execute(
            """SELECT content.sha1 AS blob,
                      content.date AS date,
                      content_location.path AS path
                 FROM (SELECT content_in_rev.blob,
                              location.path
                        FROM (SELECT content_early_in_rev.blob,
                                     content_early_in_rev.loc
                               FROM content_early_in_rev
                               JOIN revision
                                 ON revision.id=content_early_in_rev.rev
                               WHERE revision.sha1=%s
                             ) AS content_in_rev
                        JOIN location
                          ON location.id=content_in_rev.loc
                      ) AS content_location
                 JOIN content
                   ON content.id=content_location.blob
                 ORDER BY path""",
            (revision,)
        )
        content = list(provenance.cursor.fetchall())

        for blob, date, path in content:
            delta = date.timestamp() - timestamp
            location = os.fsdecode(path)
            print(dump("C", blob, delta, path=location, table="R---C"))

        # Display isochrone frontiers found in current revision.
        provenance.cursor.execute(
            """SELECT directory.sha1 AS dir,
                      directory.date AS date,
                      directory_location.path AS path
                 FROM (SELECT isochrone_frontier.dir,
                              location.path
                        FROM (SELECT directory_in_rev.dir,
                                     directory_in_rev.loc
                               FROM directory_in_rev
                               JOIN revision
                                 ON revision.id=directory_in_rev.rev
                               WHERE revision.sha1=%s
                             ) AS isochrone_frontier
                        JOIN location
                          ON location.id=isochrone_frontier.loc
                      ) AS directory_location
                 JOIN directory
                   ON directory.id=directory_location.dir
                 ORDER BY path""",
            (revision,)
        )
        directories = list(provenance.cursor.fetchall())

        for directory, date, path in directories:
            delta = date.timestamp() - timestamp
            location = os.fsdecode(path) + "/"
            if location == "/": location = "./"
            print(dump("D", directory, delta, path=location, table="R-D  "))

            # Display content found outside the current isochrone frontier.
            provenance.cursor.execute(
                """SELECT content.sha1 AS blob,
                          content.date AS date,
                          content_location.path AS path
                     FROM (SELECT content_outside.blob,
                                  location.path
                            FROM (SELECT content_in_dir.blob,
                                         content_in_dir.loc
                                   FROM content_in_dir
                                   JOIN directory
                                     ON directory.id=content_in_dir.dir
                                   WHERE directory.sha1=%s
                                 ) AS content_outside
                            JOIN location
                              ON location.id=content_outside.loc
                          ) AS content_location
                     JOIN content
                       ON content.id=content_location.blob
                     ORDER BY path""",
                (directory,)
            )
            content = list(provenance.cursor.fetchall())

            for blob, date, path in content:
                delta = date.timestamp() - timestamp
                location = " + " + os.fsdecode(path)
                print(dump("C", blob, delta, path=location, table="  D-C"))

        print("")
