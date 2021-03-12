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
        self.insert_cache["content_in_dir"].add(
            (blob.id, directory.id, normalize(os.path.join(prefix, blob.name)))
        )

    def content_add_to_revision(
        self, revision: RevisionEntry, blob: FileEntry, prefix: bytes
    ):
        self.insert_cache["content_early_in_rev"].add(
            (blob.id, revision.id, normalize(os.path.join(prefix, blob.name)))
        )

    def content_find_first(
        self, blobid: bytes
    ) -> Optional[Tuple[bytes, bytes, datetime, bytes]]:
        self.cursor.execute(
            """SELECT content_location.sha1 AS blob,
                      revision.sha1 AS rev,
                      revision.date AS date,
                      content_location.path AS path
                 FROM (SELECT content_hex.sha1,
                              content_hex.rev,
                              location.path
                        FROM (SELECT content.sha1,
                                     content_early_in_rev.rev,
                                     content_early_in_rev.loc
                               FROM content_early_in_rev
                               JOIN content
                                 ON content.id=content_early_in_rev.blob
                               WHERE content.sha1=%s
                             ) AS content_hex
                        JOIN location
                            ON location.id=content_hex.loc
                      ) AS content_location
                 JOIN revision
                   ON revision.id=content_location.rev
                 ORDER BY date, rev, path ASC LIMIT 1""",
            (blobid,),
        )
        return self.cursor.fetchone()

    def content_find_all(
        self, blobid: bytes, limit: Optional[int] = None
    ) -> Generator[Tuple[bytes, bytes, datetime, bytes], None, None]:
        early_cut = f"LIMIT {limit}" if limit is not None else ""
        self.cursor.execute(
            f"""(SELECT content_location.sha1 AS blob,
                        revision.sha1 AS rev,
                        revision.date AS date,
                        content_location.path AS path
                  FROM (SELECT content_hex.sha1,
                               content_hex.rev,
                               location.path
                         FROM (SELECT content.sha1,
                                      content_early_in_rev.rev,
                                      content_early_in_rev.loc
                                FROM content_early_in_rev
                                JOIN content
                                  ON content.id=content_early_in_rev.blob
                                WHERE content.sha1=%s
                              ) AS content_hex
                         JOIN location
                           ON location.id=content_hex.loc
                       ) AS content_location
                  JOIN revision
                    ON revision.id=content_location.rev
                  )
                UNION
                (SELECT content_prefix.sha1 AS blob,
                        revision.sha1 AS rev,
                        revision.date AS date,
                        content_prefix.path AS path
                  FROM (SELECT content_in_rev.sha1,
                               content_in_rev.rev,
                               CASE location.path
                                 WHEN '' THEN content_in_rev.suffix
                                 WHEN '.' THEN content_in_rev.suffix
                                 ELSE (location.path || '/' ||
                                          content_in_rev.suffix)::unix_path
                               END AS path
                         FROM (SELECT content_suffix.sha1,
                                      directory_in_rev.rev,
                                      directory_in_rev.loc,
                                      content_suffix.path AS suffix
                                FROM (SELECT content_hex.sha1,
                                             content_hex.dir,
                                             location.path
                                       FROM (SELECT content.sha1,
                                                    content_in_dir.dir,
                                                    content_in_dir.loc
                                              FROM content_in_dir
                                              JOIN content
                                                ON content_in_dir.blob=content.id
                                              WHERE content.sha1=%s
                                            ) AS content_hex
                                       JOIN location
                                         ON location.id=content_hex.loc
                                     ) AS content_suffix
                                JOIN directory_in_rev
                                  ON directory_in_rev.dir=content_suffix.dir
                              ) AS content_in_rev
                         JOIN location
                           ON location.id=content_in_rev.loc
                       ) AS content_prefix
                  JOIN revision
                    ON revision.id=content_prefix.rev
                )
                ORDER BY date, rev, path {early_cut}""",
            (blobid, blobid),
        )
        # TODO: use POSTGRESQL EXPLAIN looking for query optimizations.
        yield from self.cursor.fetchall()

    def directory_add_to_revision(
        self, revision: RevisionEntry, directory: DirectoryEntry, path: bytes
    ):
        self.insert_cache["directory_in_rev"].add(
            (directory.id, revision.id, normalize(path))
        )

    def insert_location(self, src0_table, src1_table, dst_table):
        """Insert location entries in `dst_table` from the insert_cache

        Also insert missing location entries in the 'location' table.
        """
        # TODO: find a better way of doing this; might be doable in a coupls of
        # SQL queries (one to insert missing entries in the location' table,
        # one to insert entries in the dst_table)

        # Resolve src0 ids
        src0_sha1s = tuple(set(sha1 for (sha1, _, _) in self.insert_cache[dst_table]))
        fmt = ",".join(["%s"] * len(src0_sha1s))
        self.cursor.execute(
            f"""SELECT sha1, id FROM {src0_table} WHERE sha1 IN ({fmt})""", src0_sha1s,
        )
        src0_values = dict(self.cursor.fetchall())

        # Resolve src1 ids
        src1_sha1s = tuple(set(sha1 for (_, sha1, _) in self.insert_cache[dst_table]))
        fmt = ",".join(["%s"] * len(src1_sha1s))
        self.cursor.execute(
            f"""SELECT sha1, id FROM {src1_table} WHERE sha1 IN ({fmt})""", src1_sha1s,
        )
        src1_values = dict(self.cursor.fetchall())

        # insert missing locations
        locations = tuple(set((loc,) for (_, _, loc) in self.insert_cache[dst_table]))
        psycopg2.extras.execute_values(
            self.cursor,
            """
            INSERT INTO location(path) VALUES %s
            ON CONFLICT (path) DO NOTHING
            """,
            locations,
        )
        # fetch location ids
        fmt = ",".join(["%s"] * len(locations))
        self.cursor.execute(
            f"SELECT path, id FROM location WHERE path IN ({fmt})", locations,
        )
        loc_ids = dict(self.cursor.fetchall())

        # Insert values in dst_table
        rows = [
            (src0_values[sha1_src], src1_values[sha1_dst], loc_ids[loc])
            for (sha1_src, sha1_dst, loc) in self.insert_cache[dst_table]
        ]
        psycopg2.extras.execute_values(
            self.cursor,
            f"""INSERT INTO {dst_table} VALUES %s
                  ON CONFLICT DO NOTHING""",
            rows,
        )
        self.insert_cache[dst_table].clear()
