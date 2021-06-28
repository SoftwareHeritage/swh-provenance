from typing import Generator, Optional, Set, Tuple

import psycopg2
import psycopg2.extras

from swh.model.model import Sha1Git

from ..provenance import ProvenanceResult
from .provenancedb_base import ProvenanceDBBase


class ProvenanceWithoutPathDB(ProvenanceDBBase):
    def content_find_first(self, id: Sha1Git) -> Optional[ProvenanceResult]:
        self.cursor.execute(
            """
            SELECT C.sha1 AS blob,
                   R.sha1 AS rev,
                   R.date AS date,
                   O.url AS url,
                   '\\x'::bytea as path
            FROM content AS C
            INNER JOIN content_in_revision AS CR ON (CR.content=C.id)
            INNER JOIN revision as R ON (CR.revision=R.id)
            LEFT JOIN origin as O ON (R.origin=O.id)
            WHERE C.sha1=%s
            ORDER BY date, rev, url ASC LIMIT 1
            """,
            (id,),
        )
        row = self.cursor.fetchone()
        if row:
            return ProvenanceResult(
                content=row[0], revision=row[1], date=row[2], origin=row[3], path=row[4]
            )
        else:
            return None

    def content_find_all(
        self, id: Sha1Git, limit: Optional[int] = None
    ) -> Generator[ProvenanceResult, None, None]:
        early_cut = f"LIMIT {limit}" if limit is not None else ""
        self.cursor.execute(
            f"""
            (SELECT C.sha1 AS blob,
                    R.sha1 AS rev,
                    R.date AS date,
                    O.url AS url,
                    '\\x'::bytea as path
             FROM content AS C
             INNER JOIN content_in_revision AS CR ON (CR.content=C.id)
             INNER JOIN revision AS R ON (CR.revision=R.id)
             LEFT JOIN origin as O ON (R.origin=O.id)
             WHERE C.sha1=%s)
            UNION
            (SELECT C.sha1 AS content,
                    R.sha1 AS revision,
                    R.date AS date,
                    O.url AS url,
                    '\\x'::bytea as path
             FROM content AS C
             INNER JOIN content_in_directory AS CD ON (C.id=CD.content)
             INNER JOIN directory_in_revision AS DR ON (CD.directory=DR.directory)
             INNER JOIN revision AS R ON (DR.revision=R.id)
             LEFT JOIN origin as O ON (R.origin=O.id)
             WHERE C.sha1=%s)
            ORDER BY date, rev, url {early_cut}
            """,
            (id, id),
        )
        for row in self.cursor.fetchall():
            yield ProvenanceResult(
                content=row[0], revision=row[1], date=row[2], origin=row[3], path=row[4]
            )

    def insert_relation(self, relation: str, data: Set[Tuple[Sha1Git, Sha1Git, bytes]]):
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
