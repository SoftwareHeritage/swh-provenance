# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Generator, Optional

from swh.model.model import Sha1Git

from ..interface import ProvenanceResult, RelationType
from .provenancedb_base import ProvenanceDBBase


class ProvenanceWithPathDB(ProvenanceDBBase):
    def content_find_first(self, id: Sha1Git) -> Optional[ProvenanceResult]:
        if self.denormalized:
            sql = """
            SELECT C_L.sha1 AS content,
                   R.sha1 AS revision,
                   R.date AS date,
                   O.url AS origin,
                   L.path AS path
            FROM (
              sELECT C.sha1 AS sha1,
                     UNNEST(revision) AS revision,
                     UNNEST(location) AS location
              FROM content_in_revision AS C_R
              INNER JOIN content AS C ON (C.id=C_R.content)
              WHERE C.sha1=%s) AS C_L
            INNER JOIN revision AS R ON (R.id=C_L.revision)
            INNER JOIN location AS L ON (L.id=C_L.location)
            LEFT JOIN origin AS O ON (R.origin=O.id)
            ORDER BY date, revision, origin, path ASC LIMIT 1
            """
        else:
            sql = """
            SELECT C.sha1 AS content,
                   R.sha1 AS revision,
                   R.date AS date,
                   O.url AS origin,
                   L.path AS path
            FROM content AS C
            INNER JOIN content_in_revision AS CR ON (CR.content=C.id)
            INNER JOIN location as L ON (CR.location=L.id)
            INNER JOIN revision as R ON (CR.revision=R.id)
            LEFT JOIN origin AS O ON (R.origin=O.id)
            WHERE C.sha1=%s
            ORDER BY date, revision, origin, path ASC LIMIT 1
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
                    L.path AS path
            FROM (
              SELECT C.sha1 AS sha1,
                     unnest(revision) AS revision,
                     unnest(location) AS location
              FROM content_in_revision AS C_R
              INNER JOIN content AS C ON (C.id = C_R.content)
              WHERE C.sha1=%s) AS C_L
            INNER JOIN revision AS R ON (R.id = C_L.revision)
            INNER JOIN location AS L ON (L.id = C_L.location)
            LEFT JOIN origin AS O ON (R.origin=O.id)
            )
            UNION
            (WITH
             C_D as (
              SELECT C.sha1 AS content_sha1,
                     unnest(CD.directory) AS directory,
                     unnest(CD.location) AS location
              FROM content AS C
              INNER JOIN content_in_directory AS CD ON (CD.content = C.id)
              WHERE C.sha1=%s
              ),
             D_R as (
              SELECT C_D.content_sha1 AS content_sha1,
                     DL.path AS file_path,
                     unnest(DR.revision) AS revision,
                     unnest(DR.location) AS prefix_location
              FROM C_D
              INNER JOIN directory_in_revision AS DR ON (DR.directory = C_D.directory)
              INNER JOIN location AS DL ON (DL.id = C_D.location)
              )
            SELECT D_R.content_sha1 AS sha1,
                   R.sha1 AS revision,
                   R.date AS date,
                   O.url AS origin,
                   CASE DL.path
                      WHEN ''  THEN D_R.file_path
                      WHEN '.' THEN D_R.file_path
                      ELSE (DL.path || '/' || D_R.file_path)::unix_path
                   END AS path
            FROM D_R
            INNER JOIN location AS DL ON (D_R.prefix_location = DL.id)
            INNER JOIN revision AS R ON (D_R.revision = R.id)
            LEFT JOIN origin AS O ON (R.origin=O.id)
            )
            ORDER BY date, revision, origin, path {early_cut}
            """
        else:
            sql = f"""
            (SELECT C.sha1 AS content,
                    R.sha1 AS revision,
                    R.date AS date,
                    O.url AS origin,
                    L.path AS path
             FROM content AS C
             INNER JOIN content_in_revision AS CR ON (CR.content=C.id)
             INNER JOIN location AS L ON (CR.location=L.id)
             INNER JOIN revision AS R ON (CR.revision=R.id)
             LEFT JOIN origin AS O ON (R.origin=O.id)
             WHERE C.sha1=%s)
            UNION
            (SELECT C.sha1 AS content,
                    R.sha1 AS revision,
                    R.date AS date,
                    O.url AS origin,
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
            ORDER BY date, revision, origin, path {early_cut}
            """
        self.cursor.execute(sql, (id, id))
        yield from (ProvenanceResult(**row) for row in self.cursor.fetchall())

    def _relation_uses_location_table(self, relation: RelationType) -> bool:
        src, *_ = relation.value.split("_")
        return src in ("content", "directory")
