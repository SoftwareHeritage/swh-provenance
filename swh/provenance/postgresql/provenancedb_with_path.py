from datetime import datetime
import os
from typing import Generator, Optional, Tuple

import psycopg2
import psycopg2.extras

from ..model import DirectoryEntry, FileEntry
from ..revision import RevisionEntry
from .provenancedb_base import ProvenanceDBBase


def normalize(path: bytes) -> bytes:
    return path[2:] if path.startswith(bytes("." + os.path.sep, "utf-8")) else path


class ProvenanceWithPathDB(ProvenanceDBBase):
    def content_add_to_directory(
        self, directory: DirectoryEntry, blob: FileEntry, prefix: bytes
    ):
        self.write_cache["content_in_dir"].add(
            (blob.id, directory.id, normalize(os.path.join(prefix, blob.name)))
        )

    def content_add_to_revision(
        self, revision: RevisionEntry, blob: FileEntry, prefix: bytes
    ):
        self.write_cache["content_early_in_rev"].add(
            (blob.id, revision.id, normalize(os.path.join(prefix, blob.name)))
        )

    def content_find_first(
        self, blobid: bytes
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
            (blobid,),
        )
        return self.cursor.fetchone()

    def content_find_all(
        self, blobid: bytes, limit: Optional[int] = None
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
            (blobid, blobid),
        )
        # TODO: use POSTGRESQL EXPLAIN looking for query optimizations.
        yield from self.cursor.fetchall()

    def directory_add_to_revision(
        self, revision: RevisionEntry, directory: DirectoryEntry, path: bytes
    ):
        self.write_cache["directory_in_rev"].add(
            (directory.id, revision.id, normalize(path))
        )

    def insert_location(self, src0_table, src1_table, dst_table):
        """Insert location entries in `dst_table` from the write_cache

        Also insert missing location entries in the 'location' table.
        """
        # TODO: find a better way of doing this; might be doable in a coupls of
        # SQL queries (one to insert missing entries in the location' table,
        # one to insert entries in the dst_table)

        # Resolve src0 ids
        src0_sha1s = tuple(set(sha1 for (sha1, _, _) in self.write_cache[dst_table]))
        fmt = ",".join(["%s"] * len(src0_sha1s))
        self.cursor.execute(
            f"""SELECT sha1, id FROM {src0_table} WHERE sha1 IN ({fmt})""",
            src0_sha1s,
        )
        src0_values = dict(self.cursor.fetchall())

        # Resolve src1 ids
        src1_sha1s = tuple(set(sha1 for (_, sha1, _) in self.write_cache[dst_table]))
        fmt = ",".join(["%s"] * len(src1_sha1s))
        self.cursor.execute(
            f"""SELECT sha1, id FROM {src1_table} WHERE sha1 IN ({fmt})""",
            src1_sha1s,
        )
        src1_values = dict(self.cursor.fetchall())

        # insert missing locations
        locations = tuple(set((loc,) for (_, _, loc) in self.write_cache[dst_table]))
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

        # Insert values in dst_table
        rows = [
            (src0_values[sha1_src], src1_values[sha1_dst], loc_ids[loc])
            for (sha1_src, sha1_dst, loc) in self.write_cache[dst_table]
        ]
        psycopg2.extras.execute_values(
            self.cursor,
            f"""
            LOCK TABLE ONLY {dst_table};
            INSERT INTO {dst_table} VALUES %s
              ON CONFLICT DO NOTHING
            """,
            rows,
        )
        self.write_cache[dst_table].clear()
