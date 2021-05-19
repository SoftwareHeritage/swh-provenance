from datetime import datetime
import itertools
import logging
from typing import Any, Dict, Iterable, Optional, Set

import psycopg2
import psycopg2.extras

from ..model import DirectoryEntry, FileEntry
from ..origin import OriginEntry
from ..revision import RevisionEntry


class ProvenanceDBBase:
    raise_on_commit: bool = False

    def __init__(self, conn: psycopg2.extensions.connection):
        # TODO: consider adding a mutex for thread safety
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn.set_session(autocommit=True)
        self.conn = conn
        self.cursor = self.conn.cursor()
        # XXX: not sure this is the best place to do it!
        self.cursor.execute("SET timezone TO 'UTC'")
        self.insert_cache: Dict[str, Any] = {}
        self.remove_cache: Dict[str, Set[bytes]] = {}
        self.select_cache: Dict[str, Any] = {}
        self.clear_caches()

    def clear_caches(self):
        self.insert_cache = {
            "content": dict(),
            "content_early_in_rev": set(),
            "content_in_dir": set(),
            "directory": dict(),
            "directory_in_rev": set(),
            "revision": dict(),
            "revision_before_rev": list(),
            "revision_in_org": list(),
        }
        self.remove_cache = {"directory": set()}
        self.select_cache = {"content": dict(), "directory": dict(), "revision": dict()}

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

    def content_get_early_dates(
        self, blobs: Iterable[FileEntry]
    ) -> Dict[bytes, datetime]:
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
            for sha1, date in self.cursor.fetchall():
                dates[sha1] = date
                self.select_cache["content"][sha1] = date
        return dates

    def content_set_early_date(self, blob: FileEntry, date: datetime):
        self.insert_cache["content"][blob.id] = date

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
        self, dirs: Iterable[DirectoryEntry]
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
            for sha1, date in self.cursor.fetchall():
                dates[sha1] = date
                self.select_cache["directory"][sha1] = date
        return dates

    def directory_invalidate_in_isochrone_frontier(self, directory: DirectoryEntry):
        self.remove_cache["directory"].add(directory.id)
        self.insert_cache["directory"].pop(directory.id, None)

    def directory_set_date_in_isochrone_frontier(
        self, directory: DirectoryEntry, date: datetime
    ):
        self.insert_cache["directory"][directory.id] = date
        self.remove_cache["directory"].discard(directory.id)

    def insert_all(self):
        # Performe insertions with cached information
        if self.insert_cache["content"]:
            psycopg2.extras.execute_values(
                self.cursor,
                """
                LOCK TABLE ONLY content;
                INSERT INTO content(sha1, date) VALUES %s
                  ON CONFLICT (sha1) DO
                    UPDATE SET date=LEAST(EXCLUDED.date,content.date)
                """,
                self.insert_cache["content"].items(),
            )
            self.insert_cache["content"].clear()

        if self.insert_cache["directory"]:
            psycopg2.extras.execute_values(
                self.cursor,
                """
                LOCK TABLE ONLY directory;
                INSERT INTO directory(sha1, date) VALUES %s
                  ON CONFLICT (sha1) DO
                    UPDATE SET date=LEAST(EXCLUDED.date,directory.date)
                """,
                self.insert_cache["directory"].items(),
            )
            self.insert_cache["directory"].clear()

        if self.insert_cache["revision"]:
            psycopg2.extras.execute_values(
                self.cursor,
                """
                LOCK TABLE ONLY revision;
                INSERT INTO revision(sha1, date) VALUES %s
                  ON CONFLICT (sha1) DO
                    UPDATE SET date=LEAST(EXCLUDED.date,revision.date)
                """,
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
        #         """
        #         LOCK TABLE ONLY revision_before_rev;
        #         INSERT INTO revision_before_rev VALUES %s
        #           ON CONFLICT DO NOTHING
        #         """,
        #         self.insert_cache["revision_before_rev"],
        #     )
        #     self.insert_cache["revision_before_rev"].clear()

        # if self.insert_cache["revision_in_org"]:
        #     psycopg2.extras.execute_values(
        #         self.cursor,
        #         """
        #         LOCK TABLE ONLY revision_in_org;
        #         INSERT INTO revision_in_org VALUES %s
        #           ON CONFLICT DO NOTHING
        #         """,
        #         self.insert_cache["revision_in_org"],
        #     )
        #     self.insert_cache["revision_in_org"].clear()

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
