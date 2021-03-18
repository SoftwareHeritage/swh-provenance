from datetime import datetime
import itertools
import operator
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
        # Resolve src0 ids
        src0_values = dict().fromkeys(
            map(operator.itemgetter(0), self.insert_cache[dst_table])
        )
        values = ", ".join(itertools.repeat("%s", len(src0_values)))
        self.cursor.execute(
            f"""SELECT sha1, id FROM {src0_table} WHERE sha1 IN ({values})""",
            tuple(src0_values),
        )
        src0_values = dict(self.cursor.fetchall())

        # Resolve src1 ids
        src1_values = dict().fromkeys(
            map(operator.itemgetter(1), self.insert_cache[dst_table])
        )
        values = ", ".join(itertools.repeat("%s", len(src1_values)))
        self.cursor.execute(
            f"""SELECT sha1, id FROM {src1_table} WHERE sha1 IN ({values})""",
            tuple(src1_values),
        )
        src1_values = dict(self.cursor.fetchall())

        # Resolve location ids
        location = dict().fromkeys(
            map(operator.itemgetter(2), self.insert_cache[dst_table])
        )
        location = dict(
            psycopg2.extras.execute_values(
                self.cursor,
                """LOCK TABLE ONLY location;
                   INSERT INTO location(path) VALUES %s
                     ON CONFLICT (path) DO
                       UPDATE SET path=EXCLUDED.path
                     RETURNING path, id""",
                map(lambda path: (path,), location.keys()),
                fetch=True,
            )
        )

        # Insert values in dst_table
        rows = map(
            lambda row: (src0_values[row[0]], src1_values[row[1]], location[row[2]]),
            self.insert_cache[dst_table],
        )
        psycopg2.extras.execute_values(
            self.cursor,
            f"""INSERT INTO {dst_table} VALUES %s
                  ON CONFLICT DO NOTHING""",
            rows,
        )
        self.insert_cache[dst_table].clear()
