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
            # TODO: find a better way of doing this; might be doable in a couple of
            # SQL queries (one to insert missing entries in the location' table,
            # one to insert entries in the relation)

            # Resolve src ids
            src_sha1s = tuple(set(sha1 for (sha1, _, _) in data))
            fmt = ",".join(["%s"] * len(src_sha1s))
            self.cursor.execute(
                f"""SELECT sha1, id FROM {src} WHERE sha1 IN ({fmt})""",
                src_sha1s,
            )
            src_values = dict(self.cursor.fetchall())

            # Resolve dst ids
            dst_sha1s = tuple(set(sha1 for (_, sha1, _) in data))
            fmt = ",".join(["%s"] * len(dst_sha1s))
            self.cursor.execute(
                f"""SELECT sha1, id FROM {dst} WHERE sha1 IN ({fmt})""",
                dst_sha1s,
            )
            dst_values = dict(self.cursor.fetchall())

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
            # fetch location ids
            fmt = ",".join(["%s"] * len(locations))
            self.cursor.execute(
                f"SELECT path, id FROM location WHERE path IN ({fmt})",
                locations,
            )
            loc_ids = dict(self.cursor.fetchall())

            # Insert values in relation
            rows = [
                (src_values[sha1_src], dst_values[sha1_dst], loc_ids[loc])
                for (sha1_src, sha1_dst, loc) in data
            ]
            psycopg2.extras.execute_values(
                self.cursor,
                f"""
                LOCK TABLE ONLY {relation};
                INSERT INTO {relation} VALUES %s
                ON CONFLICT DO NOTHING
                """,
                rows,
            )
            data.clear()
