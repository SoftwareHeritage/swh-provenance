# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Generator, Optional

from swh.model.model import Sha1Git

from ..provenance import ProvenanceResult, RelationType
from .provenancedb_base import ProvenanceDBBase


class ProvenanceWithoutPathDB(ProvenanceDBBase):
    def content_find_first(self, id: Sha1Git) -> Optional[ProvenanceResult]:
        if self.denormalized:
            sql = """
            SELECT C_L.sha1 AS content,
                   R.sha1 AS revision,
                   R.date AS date,
                   O.url AS origin,
                   '\\x'::bytea AS path
            FROM (
              SELECT C.sha1, UNNEST(revision) AS revision
              FROM content_in_revision AS C_R
              INNER JOIN content AS C ON (C.id=C_R.content)
              WHERE C.sha1=%s
            ) AS C_L
            INNER JOIN revision AS R ON (R.id=C_L.revision)
            LEFT JOIN origin AS O ON (R.origin=O.id)
            ORDER BY date, revision, origin ASC LIMIT 1
            """
        else:
            sql = """
            SELECT C.sha1 AS content,
                   R.sha1 AS revision,
                   R.date AS date,
                   O.url AS origin,
                   '\\x'::bytea AS path
            FROM content AS C
            INNER JOIN content_in_revision AS CR ON (CR.content = C.id)
            INNER JOIN revision AS R ON (CR.revision = R.id)
            LEFT JOIN origin AS O ON (R.origin=O.id)
            WHERE C.sha1=%s
            ORDER BY date, revision, origin ASC LIMIT 1
            """

        self.cursor.execute(sql, (id,))
        row = self.cursor.fetchone()
        return ProvenanceResult(**row) if row is not None else None

    def content_find_all(
        self, id: Sha1Git, limit: Optional[int] = None
    ) -> Generator[ProvenanceResult, None, None]:
        early_cut = f"LIMIT {limit}" if limit is not None else ""
        if self.denormalized:
            sql = f"""
            (SELECT C_L.sha1 AS content,
                    R.sha1 AS revision,
                    R.date AS date,
                    O.url AS origin,
                    '\\x'::bytea as path
             FROM (
              SELECT C.sha1, UNNEST(revision) AS revision
              FROM content_in_revision AS C_R
              INNER JOIN content AS C ON (C.id=C_R.content)
              WHERE C.sha1=%s) AS C_L
             INNER JOIN revision AS R ON (R.id=C_L.revision)
             LEFT JOIN origin AS O ON (R.origin=O.id)
            )
            UNION
            (WITH
             C_D AS (
              SELECT C.sha1 AS content_sha1,
                     unnest(CD.directory) AS directory
              FROM content AS C
              INNER JOIN content_in_directory AS CD ON (CD.content = C.id)
              WHERE C.sha1=%s
              ),
             D_R AS (
              SELECT C_D.content_sha1 AS content_sha1,
                     UNNEST(DR.revision) AS revision
              FROM C_D
              INNER JOIN directory_in_revision AS DR ON (DR.directory = C_D.directory)
              )
            SELECT D_R.content_sha1 AS content,
                   R.sha1 AS revision,
                   R.date AS date,
                   O.url AS origin,
                   '\\x'::bytea AS path
            FROM D_R
            INNER JOIN revision AS R ON (D_R.revision = R.id)
            LEFT JOIN origin AS O ON (R.origin=O.id)
            )
            ORDER BY date, revision, path {early_cut}
            """
        else:
            sql = f"""
            (SELECT C.sha1 AS content,
                    R.sha1 AS revision,
                    R.date AS date,
                    O.url AS origin,
                    '\\x'::bytea as path
             FROM content AS C
             INNER JOIN content_in_revision AS CR ON (CR.content=C.id)
             INNER JOIN revision AS R ON (CR.revision=R.id)
             LEFT JOIN origin AS O ON (R.origin=O.id)
             WHERE C.sha1=%s)
            UNION
            (SELECT C.sha1 AS content,
                    R.sha1 AS revision,
                    R.date AS date,
                    O.url AS origin,
                    '\\x'::bytea AS path
             FROM content AS C
             INNER JOIN content_in_directory AS CD ON (C.id=CD.content)
             INNER JOIN directory_in_revision AS DR ON (CD.directory=DR.directory)
             INNER JOIN revision AS R ON (DR.revision=R.id)
             LEFT JOIN origin AS O ON (R.origin=O.id)
             WHERE C.sha1=%s)
            ORDER BY date, revision, origin {early_cut}
            """
        self.cursor.execute(sql, (id, id))
        yield from (ProvenanceResult(**row) for row in self.cursor.fetchall())

    def _relation_uses_location_table(self, relation: RelationType) -> bool:
        return False
