from datetime import datetime
import itertools
import logging
from typing import Any, Dict, Iterable, List, Optional

import psycopg2
import psycopg2.extras

from ..model import DirectoryEntry, FileEntry
from ..origin import OriginEntry
from ..revision import RevisionEntry


class ProvenanceDBBase:
    raise_on_commit: bool = False

    def __init__(self, conn: psycopg2.extensions.connection):
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn.set_session(autocommit=True)
        self.conn = conn
        self.cursor = self.conn.cursor()
        # XXX: not sure this is the best place to do it!
        self.cursor.execute("SET timezone TO 'UTC'")
        self.write_cache: Dict[str, Any] = {}
        self.read_cache: Dict[str, Any] = {}
        self.clear_caches()

    def clear_caches(self):
        self.write_cache = {
            "content": dict(),
            "content_early_in_rev": set(),
            "content_in_dir": set(),
            "directory": dict(),
            "directory_in_rev": set(),
            "revision": dict(),
            "revision_before_rev": list(),
            "revision_in_org": list(),
        }
        self.read_cache = {"content": dict(), "directory": dict(), "revision": dict()}

    def commit(self):
        try:
            self.insert_all()
            self.clear_caches()
            return True
        except:  # noqa: E722
            # Unexpected error occurred, rollback all changes and log message
            logging.exception("Unexpected error")
            if self.raise_on_commit:
                raise
        return False

    def content_get_early_date(self, blob: FileEntry) -> Optional[datetime]:
        return self.get_dates("content", [blob.id]).get(blob.id, None)

    def content_get_early_dates(
        self, blobs: Iterable[FileEntry]
    ) -> Dict[bytes, datetime]:
        return self.get_dates("content", [blob.id for blob in blobs])

    def content_set_early_date(self, blob: FileEntry, date: datetime):
        self.write_cache["content"][blob.id] = date
        # update read cache as well
        self.read_cache["content"][blob.id] = date

    def directory_get_date_in_isochrone_frontier(
        self, directory: DirectoryEntry
    ) -> Optional[datetime]:
        return self.get_dates("directory", [directory.id]).get(directory.id, None)

    def directory_get_dates_in_isochrone_frontier(
        self, dirs: Iterable[DirectoryEntry]
    ) -> Dict[bytes, datetime]:
        return self.get_dates("directory", [directory.id for directory in dirs])

    def directory_set_date_in_isochrone_frontier(
        self, directory: DirectoryEntry, date: datetime
    ):
        self.write_cache["directory"][directory.id] = date
        # update read cache as well
        self.read_cache["directory"][directory.id] = date

    def get_dates(self, table: str, ids: List[bytes]) -> Dict[bytes, datetime]:
        dates = {}
        pending = []
        for sha1 in ids:
            # Check whether the date has been queried before
            date = self.read_cache[table].get(sha1, None)
            if date is not None:
                dates[sha1] = date
            else:
                pending.append(sha1)
        if pending:
            # Otherwise, query the database and cache the values
            values = ", ".join(itertools.repeat("%s", len(pending)))
            self.cursor.execute(
                f"""SELECT sha1, date FROM {table} WHERE sha1 IN ({values})""",
                tuple(pending),
            )
            for sha1, date in self.cursor.fetchall():
                dates[sha1] = date
                self.read_cache[table][sha1] = date
        return dates

    def insert_entity(self, entity):
        # Perform insertions with cached information
        if self.write_cache[entity]:
            psycopg2.extras.execute_values(
                self.cursor,
                f"""
                LOCK TABLE ONLY {entity};
                INSERT INTO {entity}(sha1, date) VALUES %s
                    ON CONFLICT (sha1) DO
                    UPDATE SET date=LEAST(EXCLUDED.date,{entity}.date)
                """,
                self.write_cache[entity].items(),
            )
            self.write_cache[entity].clear()

    def insert_all(self):
        # First insert entities
        self.insert_entity("content")
        self.insert_entity("directory")
        self.insert_entity("revision")

        # Relations should come after ids for entities were resolved
        self.insert_relation("content", "revision", "content_early_in_rev")
        self.insert_relation("content", "directory", "content_in_dir")
        self.insert_relation("directory", "revision", "directory_in_rev")

        # TODO: this should be updated when origin-revision layer gets properly updated.
        # if self.write_cache["revision_before_rev"]:
        #     psycopg2.extras.execute_values(
        #         self.cursor,
        #         """
        #         LOCK TABLE ONLY revision_before_rev;
        #         INSERT INTO revision_before_rev VALUES %s
        #           ON CONFLICT DO NOTHING
        #         """,
        #         self.write_cache["revision_before_rev"],
        #     )
        #     self.write_cache["revision_before_rev"].clear()
        #
        # if self.write_cache["revision_in_org"]:
        #     psycopg2.extras.execute_values(
        #         self.cursor,
        #         """
        #         LOCK TABLE ONLY revision_in_org;
        #         INSERT INTO revision_in_org VALUES %s
        #           ON CONFLICT DO NOTHING
        #         """,
        #         self.write_cache["revision_in_org"],
        #     )
        #     self.write_cache["revision_in_org"].clear()

    def origin_get_id(self, origin: OriginEntry) -> int:
        if origin.id is None:
            # Insert origin in the DB and return the assigned id
            self.cursor.execute(
                """
                LOCK TABLE ONLY origin;
                INSERT INTO origin(url) VALUES (%s)
                  ON CONFLICT DO NOTHING
                  RETURNING id
                """,
                (origin.url,),
            )
            return self.cursor.fetchone()[0]
        else:
            return origin.id

    def revision_add(self, revision: RevisionEntry):
        # Add current revision to the compact DB
        self.write_cache["revision"][revision.id] = revision.date
        # update read cache as well
        self.read_cache["revision"][revision.id] = revision.date

    def revision_add_before_revision(
        self, relative: RevisionEntry, revision: RevisionEntry
    ):
        self.write_cache["revision_before_rev"].append((revision.id, relative.id))

    def revision_add_to_origin(self, origin: OriginEntry, revision: RevisionEntry):
        self.write_cache["revision_in_org"].append((revision.id, origin.id))

    def revision_get_early_date(self, revision: RevisionEntry) -> Optional[datetime]:
        return self.get_dates("revision", [revision.id]).get(revision.id, None)

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
            """
            SELECT 1
              FROM revision_before_rev
              JOIN revision
                ON revision.id=revision_before_rev.prev
              WHERE revision.sha1=%s
            """,
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
            """
            SELECT 1
              FROM revision_in_org
              JOIN revision
                ON revision.id=revision_in_org.rev
              WHERE revision.sha1=%s
            """,
            (revision.id,),
        )
        return self.cursor.fetchone() is not None
