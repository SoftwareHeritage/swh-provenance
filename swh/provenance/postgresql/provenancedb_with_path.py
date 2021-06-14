from datetime import datetime
from typing import Generator, Optional, Set, Tuple

import psycopg2
import psycopg2.extras

from .provenancedb_base import ProvenanceDBBase


class ProvenanceWithPathDB(ProvenanceDBBase):
    def content_find_first(
        self, blob: bytes
    ) -> Optional[Tuple[bytes, bytes, datetime, bytes]]:
        self.cursor.execute(
            """
            SELECT C.sha1 AS blob,
                   R.sha1 AS rev,
                   R.date AS date,
                   L.path AS path
            FROM content AS C
            INNER JOIN content_early_in_rev AS CR ON (CR.blob = C.id)
            INNER JOIN location as L ON (CR.loc = L.id)
            INNER JOIN revision as R ON (CR.rev = R.id)
            WHERE C.sha1=%s
            ORDER BY date, rev, path ASC LIMIT 1
            """,
            (blob,),
        )
        return self.cursor.fetchone()

    def content_find_all(
        self, blob: bytes, limit: Optional[int] = None
    ) -> Generator[Tuple[bytes, bytes, datetime, bytes], None, None]:
        early_cut = f"LIMIT {limit}" if limit is not None else ""
        self.cursor.execute(
            f"""
            (SELECT C.sha1 AS blob,
                    R.sha1 AS rev,
                    R.date AS date,
                    L.path AS path
             FROM content AS C
             INNER JOIN content_early_in_rev AS CR ON (CR.blob = C.id)
             INNER JOIN location AS L ON (CR.loc = L.id)
             INNER JOIN revision AS R ON (CR.rev = R.id)
             WHERE C.sha1=%s)
            UNION
            (SELECT C.sha1 AS blob,
                    R.sha1 AS rev,
                    R.date AS date,
                    CASE DL.path
                      WHEN '' THEN CL.path
                      WHEN '.' THEN CL.path
                      ELSE (DL.path || '/' || CL.path)::unix_path
                    END AS path
             FROM content AS C
             INNER JOIN content_in_dir AS CD ON (C.id = CD.blob)
             INNER JOIN directory_in_rev AS DR ON (CD.dir = DR.dir)
             INNER JOIN revision AS R ON (DR.rev = R.id)
             INNER JOIN location AS CL ON (CD.loc = CL.id)
             INNER JOIN location AS DL ON (DR.loc = DL.id)
             WHERE C.sha1=%s)
            ORDER BY date, rev, path {early_cut}
            """,
            (blob, blob),
        )
        # TODO: use POSTGRESQL EXPLAIN looking for query optimizations.
        yield from self.cursor.fetchall()

    def insert_relation(
        self, src: str, dst: str, relation: str, data: Set[Tuple[bytes, bytes, bytes]]
    ):
        """Insert entries in `relation` from `data`

        Also insert missing location entries in the 'location' table.
        """
        if data:
            # insert missing locations
            locations = tuple(set((loc,) for (_, _, loc) in data))
            psycopg2.extras.execute_values(
                self.cursor,
                """
                LOCK TABLE ONLY location;
                INSERT INTO location(path) VALUES %s
                ON CONFLICT (path) DO NOTHING
                """,
                locations,
            )
            sql = f"""
            LOCK TABLE ONLY {relation};
            INSERT INTO {relation}
              SELECT {src}.id, {dst}.id, location.id
              FROM (VALUES %s) AS V(src, dst, path)
              INNER JOIN {src} on ({src}.sha1=V.src)
              INNER JOIN {dst} on ({dst}.sha1=V.dst)
              INNER JOIN location on (location.path=V.path)
            """
            psycopg2.extras.execute_values(self.cursor, sql, data)
            data.clear()
