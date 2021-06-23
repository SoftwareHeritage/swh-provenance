from datetime import datetime
import itertools
import logging
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

import psycopg2
import psycopg2.extras

from swh.model.model import Sha1Git


class ProvenanceDBBase:
    def __init__(self, conn: psycopg2.extensions.connection):
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn.set_session(autocommit=True)
        self.conn = conn
        self.cursor = self.conn.cursor()
        # XXX: not sure this is the best place to do it!
        self.cursor.execute("SET timezone TO 'UTC'")
        self._flavor: Optional[str] = None

    @property
    def flavor(self) -> str:
        if self._flavor is None:
            self.cursor.execute("select swh_get_dbflavor()")
            self._flavor = self.cursor.fetchone()[0]
        assert self._flavor is not None
        return self._flavor

    @property
    def with_path(self) -> bool:
        return self.flavor == "with-path"

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
                data[entity]["data"].clear()
                data[entity]["added"].clear()

            # Relations should come after ids for entities were resolved
            for relation in (
                "content_in_revision",
                "content_in_directory",
                "directory_in_revision",
            ):
                self.insert_relation(relation, data[relation])

            # Insert origins
            self.insert_origin(
                {
                    sha1: data["origin"]["data"][sha1]
                    for sha1 in data["origin"]["added"]
                },
            )
            data["origin"]["data"].clear()
            data["origin"]["added"].clear()

            # Insert relations from the origin-revision layer
            self.insert_origin_head(data["revision_in_origin"])
            self.insert_revision_history(data["revision_before_revision"])

            # Update preferred origins
            self.update_preferred_origin(
                {
                    sha1: data["revision_origin"]["data"][sha1]
                    for sha1 in data["revision_origin"]["added"]
                }
            )
            data["revision_origin"]["data"].clear()
            data["revision_origin"]["added"].clear()

            return True

        except:  # noqa: E722
            # Unexpected error occurred, rollback all changes and log message
            logging.exception("Unexpected error")
            if raise_on_commit:
                raise

        return False

    def content_find_first(
        self, blob: bytes
    ) -> Optional[Tuple[bytes, bytes, datetime, bytes]]:
        ...

    def content_find_all(
        self, blob: bytes, limit: Optional[int] = None
    ) -> Generator[Tuple[bytes, bytes, datetime, bytes], None, None]:
        ...

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

    def insert_origin(self, data: Dict[Sha1Git, str]):
        if data:
            psycopg2.extras.execute_values(
                self.cursor,
                """
                LOCK TABLE ONLY origin;
                INSERT INTO origin(sha1, url) VALUES %s
                  ON CONFLICT DO NOTHING
                """,
                data.items(),
            )
            # XXX: not sure if Python takes a reference or a copy.
            #      This might be useless!
            data.clear()

    def insert_origin_head(self, data: Set[Tuple[Sha1Git, Sha1Git]]):
        if data:
            psycopg2.extras.execute_values(
                self.cursor,
                # XXX: not clear how conflicts are handled here!
                """
                LOCK TABLE ONLY revision_in_origin;
                INSERT INTO revision_in_origin
                    SELECT R.id, O.id
                    FROM (VALUES %s) AS V(rev, org)
                    INNER JOIN revision AS R on (R.sha1=V.rev)
                    INNER JOIN origin AS O on (O.sha1=V.org)
                """,
                data,
            )
            data.clear()

    def insert_relation(self, relation: str, data: Set[Tuple[bytes, bytes, bytes]]):
        ...

    def insert_revision_history(self, data: Dict[bytes, bytes]):
        if data:
            values = [[(prev, next) for next in data[prev]] for prev in data]
            psycopg2.extras.execute_values(
                self.cursor,
                # XXX: not clear how conflicts are handled here!
                """
                LOCK TABLE ONLY revision_before_revision;
                INSERT INTO revision_before_revision
                    SELECT P.id, N.id
                    FROM (VALUES %s) AS V(prev, next)
                    INNER JOIN revision AS P on (P.sha1=V.prev)
                    INNER JOIN revision AS N on (N.sha1=V.next)
                """,
                tuple(sum(values, [])),
            )
            data.clear()

    def revision_get_preferred_origin(self, revision: Sha1Git) -> Optional[Sha1Git]:
        self.cursor.execute(
            """
            SELECT O.sha1
              FROM revision AS R
              JOIN origin as O
                ON R.origin=O.id
              WHERE R.sha1=%s""",
            (revision,),
        )
        row = self.cursor.fetchone()
        return row[0] if row is not None else None

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

    def update_preferred_origin(self, data: Dict[Sha1Git, Sha1Git]):
        if data:
            # XXX: this is assuming the revision already exists in the db! It should
            #      be improved by allowing null dates in the revision table.
            psycopg2.extras.execute_values(
                self.cursor,
                """
                UPDATE revision R
                  SET origin=O.id
                  FROM (VALUES %s) AS V(rev, org)
                  INNER JOIN origin AS O on (O.sha1=V.org)
                  WHERE R.sha1=V.rev
                """,
                data.items(),
            )
            data.clear()
