from datetime import datetime
import itertools
import operator
from typing import Generator, Optional, Tuple

import psycopg2
import psycopg2.extras

from ..model import DirectoryEntry, FileEntry
from ..revision import RevisionEntry
from .provenancedb_base import ProvenanceDBBase

########################################################################################
########################################################################################
########################################################################################


class ProvenanceWithoutPathDB(ProvenanceDBBase):
    def content_add_to_directory(
        self, directory: DirectoryEntry, blob: FileEntry, prefix: bytes
    ):
        self.insert_cache["content_in_dir"].add((blob.id, directory.id))

    def content_add_to_revision(
        self, revision: RevisionEntry, blob: FileEntry, prefix: bytes
    ):
        self.insert_cache["content_early_in_rev"].add((blob.id, revision.id))

    def content_find_first(
        self, blobid: bytes
    ) -> Optional[Tuple[bytes, bytes, datetime, bytes]]:
        self.cursor.execute(
            """SELECT revision.sha1 AS rev,
                      revision.date AS date
                 FROM (SELECT content_early_in_rev.rev
                          FROM content_early_in_rev
                          JOIN content
                            ON content.id=content_early_in_rev.blob
                          WHERE content.sha1=%s
                      ) AS content_in_rev
                 JOIN revision
                   ON revision.id=content_in_rev.rev
                 ORDER BY date, rev ASC LIMIT 1""",
            (blobid,),
        )
        row = self.cursor.fetchone()
        if row is not None:
            # TODO: query revision from the archive and look for blobid into a
            # recursive directory_ls of the revision's root.
            return blobid, row[0], row[1], b""
        return None

    def content_find_all(
        self, blobid: bytes
    ) -> Generator[Tuple[bytes, bytes, datetime, bytes], None, None]:
        self.cursor.execute(
            """(SELECT revision.sha1 AS rev,
                       revision.date AS date
                 FROM (SELECT content_early_in_rev.rev
                          FROM content_early_in_rev
                          JOIN content
                            ON content.id=content_early_in_rev.blob
                          WHERE content.sha1=%s
                      ) AS content_in_rev
                 JOIN revision
                   ON revision.id=content_in_rev.rev
                 )
               UNION
               (SELECT revision.sha1 AS rev,
                       revision.date AS date
                 FROM (SELECT directory_in_rev.rev
                          FROM (SELECT content_in_dir.dir
                                   FROM content_in_dir
                                   JOIN content
                                     ON content_in_dir.blob=content.id
                                   WHERE content.sha1=%s
                               ) AS content_dir
                          JOIN directory_in_rev
                            ON directory_in_rev.dir=content_dir.dir
                      ) AS content_in_rev
                 JOIN revision
                   ON revision.id=content_in_rev.rev
               )
               ORDER BY date, rev""",
            (blobid, blobid),
        )
        # TODO: use POSTGRESQL EXPLAIN looking for query optimizations.
        for row in self.cursor.fetchall():
            # TODO: query revision from the archive and look for blobid into a
            # recursive directory_ls of the revision's root.
            yield blobid, row[0], row[1], b""

    def directory_add_to_revision(
        self, revision: RevisionEntry, directory: DirectoryEntry, path: bytes
    ):
        self.insert_cache["directory_in_rev"].add((directory.id, revision.id))

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

        # Insert values in dst_table
        rows = map(
            lambda row: (src0_values[row[0]], src1_values[row[1]]),
            self.insert_cache[dst_table],
        )
        psycopg2.extras.execute_values(
            self.cursor,
            f"""INSERT INTO {dst_table} VALUES %s
                  ON CONFLICT DO NOTHING""",
            rows,
        )
        self.insert_cache[dst_table].clear()
