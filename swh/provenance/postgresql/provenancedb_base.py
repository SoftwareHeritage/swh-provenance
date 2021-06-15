from datetime import datetime
import itertools
import logging
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

import psycopg2
import psycopg2.extras


class ProvenanceDBBase:
    def __init__(self, conn: psycopg2.extensions.connection):
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn.set_session(autocommit=True)
        self.conn = conn
        self.cursor = self.conn.cursor()
        # XXX: not sure this is the best place to do it!
        self.cursor.execute("SET timezone TO 'UTC'")

    def commit(self, data: Dict[str, Any], raise_on_commit: bool = False) -> bool:
        try:
            # First insert entities
            for entity in ("content", "directory", "revision"):

                self.insert_entity(
                    entity,
                    {
                        sha1: data[entity]["data"][sha1]
                        for sha1 in data[entity]["added"]
                    },
                )

            # Relations should come after ids for entities were resolved
            for rel_table in (
                "content_in_revision",
                "content_in_directory",
                "directory_in_revision",
            ):
                self.insert_relation(rel_table, data[rel_table])

            # TODO: this should be updated when origin-revision layer gets properly
            #       updated.
            # if data["revision_before_revision"]:
            #     psycopg2.extras.execute_values(
            #         self.cursor,
            #         """
            #         LOCK TABLE ONLY revision_before_revision;
            #         INSERT INTO revision_before_revision VALUES %s
            #           ON CONFLICT DO NOTHING
            #         """,
            #         data["revision_before_revision"],
            #     )
            #     data["revision_before_revision"].clear()
            #
            # if data["revision_in_origin"]:
            #     psycopg2.extras.execute_values(
            #         self.cursor,
            #         """
            #         LOCK TABLE ONLY revision_in_origin;
            #         INSERT INTO revision_in_origin VALUES %s
            #           ON CONFLICT DO NOTHING
            #         """,
            #         data["revision_in_origin"],
            #     )
            #     data["revision_in_origin"].clear()

            return True
        except:  # noqa: E722
            # Unexpected error occurred, rollback all changes and log message
            logging.exception("Unexpected error")
            if raise_on_commit:
                raise
        return False

    def get_dates(self, entity: str, ids: List[bytes]) -> Dict[bytes, datetime]:
        dates = {}
        if ids:
            values = ", ".join(itertools.repeat("%s", len(ids)))
            self.cursor.execute(
                f"""SELECT sha1, date FROM {entity} WHERE sha1 IN ({values})""",
                tuple(ids),
            )
            dates.update(self.cursor.fetchall())
        return dates

    def insert_entity(self, entity: str, data: Dict[bytes, datetime]):
        if data:
            psycopg2.extras.execute_values(
                self.cursor,
                f"""
                LOCK TABLE ONLY {entity};
                INSERT INTO {entity}(sha1, date) VALUES %s
                    ON CONFLICT (sha1) DO
                    UPDATE SET date=LEAST(EXCLUDED.date,{entity}.date)
                """,
                data.items(),
            )
            # XXX: not sure if Python takes a reference or a copy.
            #      This might be useless!
            data.clear()

    def insert_relation(self, relation: str, data: Set[Tuple[bytes, bytes, bytes]]):
        ...

    def content_find_first(
        self, blob: bytes
    ) -> Optional[Tuple[bytes, bytes, datetime, bytes]]:
        ...

    def content_find_all(
        self, blob: bytes, limit: Optional[int] = None
    ) -> Generator[Tuple[bytes, bytes, datetime, bytes], None, None]:
        ...

    def origin_get_id(self, url: str) -> int:
        # Insert origin in the DB and return the assigned id
        self.cursor.execute(
            """
            LOCK TABLE ONLY origin;
            INSERT INTO origin(url) VALUES (%s)
                ON CONFLICT DO NOTHING
                RETURNING id
            """,
            (url,),
        )
        return self.cursor.fetchone()[0]

    def revision_get_preferred_origin(self, revision: bytes) -> int:
        self.cursor.execute(
            """SELECT COALESCE(origin, 0) FROM revision WHERE sha1=%s""", (revision,)
        )
        row = self.cursor.fetchone()
        # None means revision is not in database;
        # 0 means revision has no preferred origin
        return row[0] if row is not None and row[0] != 0 else None

    def revision_in_history(self, revision: bytes) -> bool:
        self.cursor.execute(
            """
            SELECT 1
              FROM revision_before_revision
              JOIN revision
                ON revision.id=revision_before_revision.prev
              WHERE revision.sha1=%s
            """,
            (revision,),
        )
        return self.cursor.fetchone() is not None

    def revision_set_preferred_origin(self, origin: int, revision: bytes):
        self.cursor.execute(
            """UPDATE revision SET origin=%s WHERE sha1=%s""", (origin, revision)
        )

    def revision_visited(self, revision: bytes) -> bool:
        self.cursor.execute(
            """
            SELECT 1
              FROM revision_in_origin
              JOIN revision
                ON revision.id=revision_in_origin.revision
              WHERE revision.sha1=%s
            """,
            (revision,),
        )
        return self.cursor.fetchone() is not None
