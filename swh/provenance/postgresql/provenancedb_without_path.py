from typing import Generator, Optional

from swh.model.model import Sha1Git

from ..provenance import ProvenanceResult, RelationType
from .provenancedb_base import ProvenanceDBBase


class ProvenanceWithoutPathDB(ProvenanceDBBase):
    def content_find_first(self, id: Sha1Git) -> Optional[ProvenanceResult]:
        sql = """
            SELECT C.sha1 AS content,
                   R.sha1 AS revision,
                   R.date AS date,
                   O.url AS origin,
                   '\\x'::bytea as path
            FROM content AS C
            INNER JOIN content_in_revision AS CR ON (CR.content=C.id)
            INNER JOIN revision as R ON (CR.revision=R.id)
            LEFT JOIN origin as O ON (R.origin=O.id)
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
        sql = f"""
            (SELECT C.sha1 AS content,
                    R.sha1 AS revision,
                    R.date AS date,
                    O.url AS origin,
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
                    O.url AS origin,
                    '\\x'::bytea as path
             FROM content AS C
             INNER JOIN content_in_directory AS CD ON (C.id=CD.content)
             INNER JOIN directory_in_revision AS DR ON (CD.directory=DR.directory)
             INNER JOIN revision AS R ON (DR.revision=R.id)
             LEFT JOIN origin as O ON (R.origin=O.id)
             WHERE C.sha1=%s)
            ORDER BY date, revision, origin {early_cut}
            """
        self.cursor.execute(sql, (id, id))
        yield from (ProvenanceResult(**row) for row in self.cursor.fetchall())

    def _relation_uses_location_table(self, relation: RelationType) -> bool:
        return False
