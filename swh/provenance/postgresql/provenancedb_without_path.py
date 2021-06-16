from datetime import datetime
from typing import Generator, Optional, Set, Tuple

import psycopg2
import psycopg2.extras

from .provenancedb_base import ProvenanceDBBase

########################################################################################
########################################################################################
########################################################################################


class ProvenanceWithoutPathDB(ProvenanceDBBase):
    def content_find_first(
        self, blob: bytes
    ) -> Optional[Tuple[bytes, bytes, datetime, bytes]]:
        self.cursor.execute(
            """
            SELECT C.sha1 AS blob,
                   R.sha1 AS rev,
                   R.date AS date,
                   '\\x'::bytea as path
            FROM content AS C
            INNER JOIN content_in_revision AS CR ON (CR.content = C.id)
            INNER JOIN revision as R ON (CR.revision = R.id)
            WHERE C.sha1=%s
            ORDER BY date, rev ASC LIMIT 1
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
                    '\\x'::bytea as path
             FROM content AS C
             INNER JOIN content_in_revision AS CR ON (CR.content = C.id)
             INNER JOIN revision AS R ON (CR.revision = R.id)
             WHERE C.sha1=%s)
            UNION
            (SELECT C.sha1 AS content,
                    R.sha1 AS revision,
                    R.date AS date,
                    '\\x'::bytea as path
             FROM content AS C
             INNER JOIN content_in_directory AS CD ON (C.id = CD.content)
             INNER JOIN directory_in_revision AS DR ON (CD.directory = DR.directory)
             INNER JOIN revision AS R ON (DR.revision = R.id)
             WHERE C.sha1=%s)
            ORDER BY date, rev, path {early_cut}
            """,
            (blob, blob),
        )
        yield from self.cursor.fetchall()

    def insert_relation(self, relation: str, data: Set[Tuple[bytes, bytes, bytes]]):
        if data:
            assert relation in (
                "content_in_revision",
                "content_in_directory",
                "directory_in_revision",
            )
            src, dst = relation.split("_in_")

            psycopg2.extras.execute_values(
                self.cursor,
                f"""
                LOCK TABLE ONLY {relation};
                INSERT INTO {relation}
                  SELECT {src}.id, {dst}.id
                  FROM (VALUES %s) AS V(src, dst)
                  INNER JOIN {src} on ({src}.sha1=V.src)
                  INNER JOIN {dst} on ({dst}.sha1=V.dst)
                """,
                data,
            )
            data.clear()
