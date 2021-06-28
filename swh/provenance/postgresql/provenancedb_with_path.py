from typing import Generator, Optional, Set, Tuple

import psycopg2
import psycopg2.extras

from swh.model.model import Sha1Git

from ..provenance import ProvenanceResult
from .provenancedb_base import ProvenanceDBBase


class ProvenanceWithPathDB(ProvenanceDBBase):
    def content_find_first(self, id: Sha1Git) -> Optional[ProvenanceResult]:
        self.cursor.execute(
            """
            SELECT C.sha1 AS blob,
                   R.sha1 AS rev,
                   R.date AS date,
                   O.url AS url,
                   L.path AS path
            FROM content AS C
            INNER JOIN content_in_revision AS CR ON (CR.content=C.id)
            INNER JOIN location as L ON (CR.location=L.id)
            INNER JOIN revision as R ON (CR.revision=R.id)
            LEFT JOIN origin as O ON (R.origin=O.id)
            WHERE C.sha1=%s
            ORDER BY date, rev, url, path ASC LIMIT 1
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
                    L.path AS path
             FROM content AS C
             INNER JOIN content_in_revision AS CR ON (CR.content=C.id)
             INNER JOIN location AS L ON (CR.location=L.id)
             INNER JOIN revision AS R ON (CR.revision=R.id)
             LEFT JOIN origin as O ON (R.origin=O.id)
             WHERE C.sha1=%s)
            UNION
            (SELECT C.sha1 AS content,
                    R.sha1 AS revision,
                    R.date AS date,
                    O.url AS url,
                    CASE DL.path
                      WHEN '' THEN CL.path
                      WHEN '.' THEN CL.path
                      ELSE (DL.path || '/' || CL.path)::unix_path
                    END AS path
             FROM content AS C
             INNER JOIN content_in_directory AS CD ON (C.id=CD.content)
             INNER JOIN directory_in_revision AS DR ON (CD.directory=DR.directory)
             INNER JOIN revision AS R ON (DR.revision=R.id)
             INNER JOIN location AS CL ON (CD.location=CL.id)
             INNER JOIN location AS DL ON (DR.location=DL.id)
             LEFT JOIN origin AS O ON (R.origin=O.id)
             WHERE C.sha1=%s)
            ORDER BY date, rev, url, path {early_cut}
            """,
            (id, id),
        )
        for row in self.cursor.fetchall():
            yield ProvenanceResult(
                content=row[0], revision=row[1], date=row[2], origin=row[3], path=row[4]
            )

    def insert_relation(self, relation: str, data: Set[Tuple[Sha1Git, Sha1Git, bytes]]):
        """Insert entries in `relation` from `data`

        Also insert missing location entries in the 'location' table.
        """
        if data:
            assert relation in (
                "content_in_revision",
                "content_in_directory",
                "directory_in_revision",
            )
            src, dst = relation.split("_in_")

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
            psycopg2.extras.execute_values(
                self.cursor,
                f"""
                LOCK TABLE ONLY {relation};
                INSERT INTO {relation}
                  SELECT {src}.id, {dst}.id, location.id
                  FROM (VALUES %s) AS V(src, dst, path)
                  INNER JOIN {src} on ({src}.sha1=V.src)
                  INNER JOIN {dst} on ({dst}.sha1=V.dst)
                  INNER JOIN location on (location.path=V.path)
                """,
                data,
            )
            data.clear()
