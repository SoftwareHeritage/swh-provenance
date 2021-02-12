import itertools
import logging
import operator
import os
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from ..model import DirectoryEntry, FileEntry
from ..origin import OriginEntry
from ..provenance import ProvenanceInterface
from ..revision import RevisionEntry
from .db_utils import connect, execute_sql


def normalize(path: bytes) -> bytes:
    return path[2:] if path.startswith(bytes("." + os.path.sep, "utf-8")) else path


def create_database(conn: psycopg2.extensions.connection, conninfo: dict, name: str):
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    # Normalize dbname to avoid issues when reconnecting below
    name = name.casefold()

    # Create new database dropping previous one if exists
    cursor = conn.cursor()
    cursor.execute(f"""DROP DATABASE IF EXISTS {name}""")
    cursor.execute(f"""CREATE DATABASE {name}""")
    conn.close()

    # Reconnect to server selecting newly created database to add tables
    conninfo["dbname"] = name
    conn = connect(conninfo)

    sqldir = os.path.dirname(os.path.realpath(__file__))
    execute_sql(conn, os.path.join(sqldir, "provenance.sql"))


########################################################################################
########################################################################################
########################################################################################


class ProvenancePostgreSQL(ProvenanceInterface):
    def __init__(self, conn: psycopg2.extensions.connection):
        # TODO: consider adding a mutex for thread safety
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.insert_cache: Dict[str, Any] = {}
        self.remove_cache: Dict[str, Any] = {}
        self.select_cache: Dict[str, Any] = {}
        self.clear_caches()

    def clear_caches(self):
        self.insert_cache = {
            "content": dict(),
            "content_early_in_rev": list(),
            "content_in_dir": list(),
            "directory": dict(),
            "directory_in_rev": list(),
            "revision": dict(),
            "revision_before_rev": list(),
            "revision_in_org": list(),
        }
        self.remove_cache = {"directory": dict()}
        self.select_cache = {"content": dict(), "directory": dict(), "revision": dict()}

    def commit(self):
        result = False
        try:
            self.insert_all()
            self.clear_caches()
            result = True

        except Exception as error:
            # Unexpected error occurred, rollback all changes and log message
            logging.error(f"Unexpected error: {error}")

        return result

    def content_add_to_directory(
        self, directory: DirectoryEntry, blob: FileEntry, prefix: bytes
    ):
        self.insert_cache["content_in_dir"].append(
            (blob.id, directory.id, normalize(os.path.join(prefix, blob.name)))
        )

    def content_add_to_revision(
        self, revision: RevisionEntry, blob: FileEntry, prefix: bytes
    ):
        self.insert_cache["content_early_in_rev"].append(
            (blob.id, revision.id, normalize(os.path.join(prefix, blob.name)))
        )

    def content_find_first(
        self, blobid: bytes
    ) -> Optional[Tuple[bytes, bytes, datetime, bytes]]:
        self.cursor.execute(
            """SELECT content_location.sha1 AS blob,
                      revision.sha1 AS rev,
                      revision.date AS date,
                      content_location.path AS path
                 FROM (SELECT content_hex.sha1,
                              content_hex.rev,
                              location.path
                        FROM (SELECT content.sha1,
                                     content_early_in_rev.rev,
                                     content_early_in_rev.loc
                               FROM content_early_in_rev
                               JOIN content
                                 ON content.id=content_early_in_rev.blob
                               WHERE content.sha1=%s
                             ) AS content_hex
                        JOIN location
                            ON location.id=content_hex.loc
                      ) AS content_location
                 JOIN revision
                   ON revision.id=content_location.rev
                 ORDER BY date, rev, path ASC LIMIT 1""",
            (blobid,),
        )
        return self.cursor.fetchone()

    def content_find_all(
        self, blobid: bytes
    ) -> Generator[Tuple[bytes, bytes, datetime, bytes], None, None]:
        self.cursor.execute(
            """(SELECT content_location.sha1 AS blob,
                       revision.sha1 AS rev,
                       revision.date AS date,
                       content_location.path AS path
                 FROM (SELECT content_hex.sha1,
                              content_hex.rev,
                              location.path
                        FROM (SELECT content.sha1,
                                     content_early_in_rev.rev,
                                     content_early_in_rev.loc
                               FROM content_early_in_rev
                               JOIN content
                                 ON content.id=content_early_in_rev.blob
                               WHERE content.sha1=%s
                             ) AS content_hex
                        JOIN location
                          ON location.id=content_hex.loc
                      ) AS content_location
                 JOIN revision
                   ON revision.id=content_location.rev
                 )
               UNION
               (SELECT content_prefix.sha1 AS blob,
                       revision.sha1 AS rev,
                       revision.date AS date,
                       content_prefix.path AS path
                 FROM (SELECT content_in_rev.sha1,
                              content_in_rev.rev,
                              CASE location.path
                                WHEN '' THEN content_in_rev.suffix
                                WHEN '.' THEN content_in_rev.suffix
                                ELSE (location.path || '/' ||
                                         content_in_rev.suffix)::unix_path
                              END AS path
                        FROM (SELECT content_suffix.sha1,
                                     directory_in_rev.rev,
                                     directory_in_rev.loc,
                                     content_suffix.path AS suffix
                               FROM (SELECT content_hex.sha1,
                                            content_hex.dir,
                                            location.path
                                      FROM (SELECT content.sha1,
                                                   content_in_dir.dir,
                                                   content_in_dir.loc
                                             FROM content_in_dir
                                             JOIN content
                                               ON content_in_dir.blob=content.id
                                             WHERE content.sha1=%s
                                           ) AS content_hex
                                      JOIN location
                                        ON location.id=content_hex.loc
                                    ) AS content_suffix
                               JOIN directory_in_rev
                                 ON directory_in_rev.dir=content_suffix.dir
                             ) AS content_in_rev
                        JOIN location
                          ON location.id=content_in_rev.loc
                      ) AS content_prefix
                 JOIN revision
                   ON revision.id=content_prefix.rev
               )
               ORDER BY date, rev, path""",
            (blobid, blobid),
        )
        # TODO: use POSTGRESQL EXPLAIN looking for query optimizations.
        yield from self.cursor.fetchall()

    def content_get_early_date(self, blob: FileEntry) -> Optional[datetime]:
        # First check if the date is being modified by current transection.
        date = self.insert_cache["content"].get(blob.id, None)
        if date is None:
            # If not, check whether it's been query before
            date = self.select_cache["content"].get(blob.id, None)
            if date is None:
                # Otherwise, query the database and cache the value
                self.cursor.execute(
                    """SELECT date FROM content WHERE sha1=%s""", (blob.id,)
                )
                row = self.cursor.fetchone()
                date = row[0] if row is not None else None
                self.select_cache["content"][blob.id] = date
        return date

    def content_get_early_dates(self, blobs: List[FileEntry]) -> Dict[bytes, datetime]:
        dates = {}
        pending = []
        for blob in blobs:
            # First check if the date is being modified by current transection.
            date = self.insert_cache["content"].get(blob.id, None)
            if date is not None:
                dates[blob.id] = date
            else:
                # If not, check whether it's been query before
                date = self.select_cache["content"].get(blob.id, None)
                if date is not None:
                    dates[blob.id] = date
                else:
                    pending.append(blob.id)
        if pending:
            # Otherwise, query the database and cache the values
            values = ", ".join(itertools.repeat("%s", len(pending)))
            self.cursor.execute(
                f"""SELECT sha1, date FROM content WHERE sha1 IN ({values})""",
                tuple(pending),
            )
            for row in self.cursor.fetchall():
                dates[row[0]] = row[1]
                self.select_cache["content"][row[0]] = row[1]
        return dates

    def content_set_early_date(self, blob: FileEntry, date: datetime):
        self.insert_cache["content"][blob.id] = date

    def directory_add_to_revision(
        self, revision: RevisionEntry, directory: DirectoryEntry, path: bytes
    ):
        self.insert_cache["directory_in_rev"].append(
            (directory.id, revision.id, normalize(path))
        )

    def directory_get_date_in_isochrone_frontier(
        self, directory: DirectoryEntry
    ) -> Optional[datetime]:
        # First check if the date is being modified by current transection.
        date = self.insert_cache["directory"].get(directory.id, None)
        if date is None and directory.id not in self.remove_cache["directory"]:
            # If not, check whether it's been query before
            date = self.select_cache["directory"].get(directory.id, None)
            if date is None:
                # Otherwise, query the database and cache the value
                self.cursor.execute(
                    """SELECT date FROM directory WHERE sha1=%s""", (directory.id,)
                )
                row = self.cursor.fetchone()
                date = row[0] if row is not None else None
                self.select_cache["directory"][directory.id] = date
        return date

    def directory_get_dates_in_isochrone_frontier(
        self, dirs: List[DirectoryEntry]
    ) -> Dict[bytes, datetime]:
        dates = {}
        pending = []
        for directory in dirs:
            # First check if the date is being modified by current transection.
            date = self.insert_cache["directory"].get(directory.id, None)
            if date is not None:
                dates[directory.id] = date
            elif directory.id not in self.remove_cache["directory"]:
                # If not, check whether it's been query before
                date = self.select_cache["directory"].get(directory.id, None)
                if date is not None:
                    dates[directory.id] = date
                else:
                    pending.append(directory.id)
        if pending:
            # Otherwise, query the database and cache the values
            values = ", ".join(itertools.repeat("%s", len(pending)))
            self.cursor.execute(
                f"""SELECT sha1, date FROM directory WHERE sha1 IN ({values})""",
                tuple(pending),
            )
            for row in self.cursor.fetchall():
                dates[row[0]] = row[1]
                self.select_cache["directory"][row[0]] = row[1]
        return dates

    def directory_invalidate_in_isochrone_frontier(self, directory: DirectoryEntry):
        self.remove_cache["directory"][directory.id] = None
        self.insert_cache["directory"].pop(directory.id, None)

    def directory_set_date_in_isochrone_frontier(
        self, directory: DirectoryEntry, date: datetime
    ):
        self.insert_cache["directory"][directory.id] = date
        self.remove_cache["directory"].pop(directory.id, None)

    def insert_all(self):
        # Performe insertions with cached information
        if self.insert_cache["content"]:
            psycopg2.extras.execute_values(
                self.cursor,
                """LOCK TABLE ONLY content;
                   INSERT INTO content(sha1, date) VALUES %s
                     ON CONFLICT (sha1) DO
                       UPDATE SET date=LEAST(EXCLUDED.date,content.date)""",
                self.insert_cache["content"].items(),
            )
            self.insert_cache["content"].clear()

        if self.insert_cache["directory"]:
            psycopg2.extras.execute_values(
                self.cursor,
                """LOCK TABLE ONLY directory;
                   INSERT INTO directory(sha1, date) VALUES %s
                     ON CONFLICT (sha1) DO
                       UPDATE SET date=LEAST(EXCLUDED.date,directory.date)""",
                self.insert_cache["directory"].items(),
            )
            self.insert_cache["directory"].clear()

        if self.insert_cache["revision"]:
            psycopg2.extras.execute_values(
                self.cursor,
                """LOCK TABLE ONLY revision;
                   INSERT INTO revision(sha1, date) VALUES %s
                     ON CONFLICT (sha1) DO
                       UPDATE SET date=LEAST(EXCLUDED.date,revision.date)""",
                self.insert_cache["revision"].items(),
            )
            self.insert_cache["revision"].clear()

        # Relations should come after ids for elements were resolved
        if self.insert_cache["content_early_in_rev"]:
            self.insert_location("content", "revision", "content_early_in_rev")

        if self.insert_cache["content_in_dir"]:
            self.insert_location("content", "directory", "content_in_dir")

        if self.insert_cache["directory_in_rev"]:
            self.insert_location("directory", "revision", "directory_in_rev")

        # if self.insert_cache["revision_before_rev"]:
        #     psycopg2.extras.execute_values(
        #         self.cursor,
        #         """INSERT INTO revision_before_rev VALUES %s
        #            ON CONFLICT DO NOTHING""",
        #         self.insert_cache["revision_before_rev"],
        #     )
        #     self.insert_cache["revision_before_rev"].clear()

        # if self.insert_cache["revision_in_org"]:
        #     psycopg2.extras.execute_values(
        #         self.cursor,
        #         """INSERT INTO revision_in_org VALUES %s
        #            ON CONFLICT DO NOTHING""",
        #         self.insert_cache["revision_in_org"],
        #     )
        #     self.insert_cache["revision_in_org"].clear()

    def insert_location(self, src0_table, src1_table, dst_table):
        # Resolve src0 ids
        src0_values = dict().fromkeys(
            map(operator.itemgetter(0), self.insert_cache[dst_table])
        )
        values = ", ".join(itertools.repeat("%s", len(src0_values)))
        self.cursor.execute(
            f"""SELECT sha1, id FROM {src0_table} WHERE sha1 IN ({values})""",
            tuple(src0_values),
        )
        src0_values = dict(self.cursor.fetchall())

        # Resolve src1 ids
        src1_values = dict().fromkeys(
            map(operator.itemgetter(1), self.insert_cache[dst_table])
        )
        values = ", ".join(itertools.repeat("%s", len(src1_values)))
        self.cursor.execute(
            f"""SELECT sha1, id FROM {src1_table} WHERE sha1 IN ({values})""",
            tuple(src1_values),
        )
        src1_values = dict(self.cursor.fetchall())

        # Resolve location ids
        location = dict().fromkeys(
            map(operator.itemgetter(2), self.insert_cache[dst_table])
        )
        location = dict(
            psycopg2.extras.execute_values(
                self.cursor,
                """LOCK TABLE ONLY location;
                   INSERT INTO location(path) VALUES %s
                     ON CONFLICT (path) DO
                       UPDATE SET path=EXCLUDED.path
                     RETURNING path, id""",
                map(lambda path: (path,), location.keys()),
                fetch=True,
            )
        )

        # Insert values in dst_table
        rows = map(
            lambda row: (src0_values[row[0]], src1_values[row[1]], location[row[2]]),
            self.insert_cache[dst_table],
        )
        psycopg2.extras.execute_values(
            self.cursor,
            f"""INSERT INTO {dst_table} VALUES %s
                  ON CONFLICT DO NOTHING""",
            rows,
        )
        self.insert_cache[dst_table].clear()

    def origin_get_id(self, origin: OriginEntry) -> int:
        if origin.id is None:
            # Insert origin in the DB and return the assigned id
            self.cursor.execute(
                """INSERT INTO origin (url) VALUES (%s)
                     ON CONFLICT DO NOTHING
                     RETURNING id""",
                (origin.url,),
            )
            return self.cursor.fetchone()[0]
        else:
            return origin.id

    def revision_add(self, revision: RevisionEntry):
        # Add current revision to the compact DB
        self.insert_cache["revision"][revision.id] = revision.date

    def revision_add_before_revision(
        self, relative: RevisionEntry, revision: RevisionEntry
    ):
        self.insert_cache["revision_before_rev"].append((revision.id, relative.id))

    def revision_add_to_origin(self, origin: OriginEntry, revision: RevisionEntry):
        self.insert_cache["revision_in_org"].append((revision.id, origin.id))

    def revision_get_early_date(self, revision: RevisionEntry) -> Optional[datetime]:
        date = self.insert_cache["revision"].get(revision.id, None)
        if date is None:
            # If not, check whether it's been query before
            date = self.select_cache["revision"].get(revision.id, None)
            if date is None:
                # Otherwise, query the database and cache the value
                self.cursor.execute(
                    """SELECT date FROM revision WHERE sha1=%s""", (revision.id,)
                )
                row = self.cursor.fetchone()
                date = row[0] if row is not None else None
                self.select_cache["revision"][revision.id] = date
        return date

    def revision_get_preferred_origin(self, revision: RevisionEntry) -> int:
        # TODO: adapt this method to consider cached values
        self.cursor.execute(
            """SELECT COALESCE(org,0) FROM revision WHERE sha1=%s""", (revision.id,)
        )
        row = self.cursor.fetchone()
        # None means revision is not in database;
        # 0 means revision has no preferred origin
        return row[0] if row is not None and row[0] != 0 else None

    def revision_in_history(self, revision: RevisionEntry) -> bool:
        # TODO: adapt this method to consider cached values
        self.cursor.execute(
            """SELECT 1
                 FROM revision_before_rev
                 JOIN revision
                   ON revision.id=revision_before_rev.prev
                 WHERE revision.sha1=%s""",
            (revision.id,),
        )
        return self.cursor.fetchone() is not None

    def revision_set_preferred_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ):
        # TODO: adapt this method to consider cached values
        self.cursor.execute(
            """UPDATE revision SET org=%s WHERE sha1=%s""", (origin.id, revision.id)
        )

    def revision_visited(self, revision: RevisionEntry) -> bool:
        # TODO: adapt this method to consider cached values
        self.cursor.execute(
            """SELECT 1
                 FROM revision_in_org
                 JOIN revision
                   ON revision.id=revision_in_org.rev
                 WHERE revision.sha1=%s""",
            (revision.id,),
        )
        return self.cursor.fetchone() is not None
