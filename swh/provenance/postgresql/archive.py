# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterable, List

from methodtools import lru_cache
import psycopg2.extensions

from swh.model.model import Sha1Git
from swh.storage import get_storage


class ArchivePostgreSQL:
    def __init__(self, conn: psycopg2.extensions.connection) -> None:
        self.storage = get_storage(
            "postgresql", db=conn.dsn, objstorage={"cls": "memory"}
        )
        self.conn = conn

    def directory_ls(self, id: Sha1Git) -> Iterable[Dict[str, Any]]:
        entries = self._directory_ls(id)
        yield from entries

    @lru_cache(maxsize=100000)
    def _directory_ls(self, id: Sha1Git) -> List[Dict[str, Any]]:
        # TODO: add file size filtering
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                WITH
                dir AS (SELECT id AS dir_id, dir_entries, file_entries, rev_entries
                            FROM directory WHERE id=%s),
                ls_d AS (SELECT dir_id, UNNEST(dir_entries) AS entry_id FROM dir),
                ls_f AS (SELECT dir_id, UNNEST(file_entries) AS entry_id FROM dir),
                ls_r AS (SELECT dir_id, UNNEST(rev_entries) AS entry_id FROM dir)
                (SELECT 'dir'::directory_entry_type AS type, e.target, e.name,
                        NULL::sha1_git
                    FROM ls_d
                    LEFT JOIN directory_entry_dir e ON ls_d.entry_id=e.id)
                UNION
                (WITH known_contents AS
                    (SELECT 'file'::directory_entry_type AS type, e.target, e.name,
                            c.sha1_git
                        FROM ls_f
                        LEFT JOIN directory_entry_file e ON ls_f.entry_id=e.id
                        INNER JOIN content c ON e.target=c.sha1_git)
                    SELECT * FROM known_contents
                    UNION
                    (SELECT 'file'::directory_entry_type AS type, e.target, e.name,
                            c.sha1_git
                        FROM ls_f
                        LEFT JOIN directory_entry_file e ON ls_f.entry_id=e.id
                        LEFT JOIN skipped_content c ON e.target=c.sha1_git
                        WHERE NOT EXISTS (
                            SELECT 1 FROM known_contents
                                WHERE known_contents.sha1_git=e.target
                        )
                    )
                )
                """,
                (id,),
            )
            return [
                {"type": row[0], "target": row[1], "name": row[2]} for row in cursor
            ]

    def revision_get_parents(self, id: Sha1Git) -> Iterable[Sha1Git]:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT RH.parent_id::bytea
                    FROM revision_history AS RH
                    WHERE RH.id=%s
                    ORDER BY RH.parent_rank
                """,
                (id,),
            )
            # There should be at most one row anyway
            yield from (row[0] for row in cursor)

    def snapshot_get_heads(self, id: Sha1Git) -> Iterable[Sha1Git]:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                WITH
                snaps AS (SELECT object_id FROM snapshot WHERE snapshot.id=%s),
                heads AS ((SELECT R.id, R.date
                            FROM snaps
                            JOIN snapshot_branches AS BS
                              ON (snaps.object_id=BS.snapshot_id)
                            JOIN snapshot_branch AS B
                              ON (BS.branch_id=B.object_id)
                            JOIN revision AS R
                              ON (B.target=R.id)
                            WHERE B.target_type='revision'::snapshot_target)
                          UNION
                          (SELECT RV.id, RV.date
                            FROM snaps
                            JOIN snapshot_branches AS BS
                              ON (snaps.object_id=BS.snapshot_id)
                            JOIN snapshot_branch AS B
                              ON (BS.branch_id=B.object_id)
                            JOIN release AS RL
                              ON (B.target=RL.id)
                            JOIN revision AS RV
                              ON (RL.target=RV.id)
                            WHERE B.target_type='release'::snapshot_target
                              AND RL.target_type='revision'::object_type)
                          ORDER BY date, id)
                SELECT id FROM heads
                """,
                (id,),
            )
            yield from (row[0] for row in cursor)
