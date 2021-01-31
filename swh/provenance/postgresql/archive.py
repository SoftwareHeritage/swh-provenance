import psycopg2

# import threading

from ..archive import ArchiveInterface

# from functools import lru_cache
from methodtools import lru_cache
from typing import Any, Dict, List


class ArchivePostgreSQL(ArchiveInterface):
    def __init__(self, conn: psycopg2.extensions.connection):
        self.conn = conn
        # self.mutex = threading.Lock()

    def directory_ls(self, id: bytes) -> List[Dict[str, Any]]:
        # TODO: only call directory_ls_internal if the id is not being queried by
        # someone else. Otherwise wait until results get properly cached.
        # self.mutex.acquire()
        entries = self.directory_ls_internal(id)
        # self.mutex.release()
        return entries

    @lru_cache(maxsize=1000000)
    def directory_ls_internal(self, id: bytes) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute(
            """WITH
            dir  AS (SELECT id AS dir_id, dir_entries, file_entries, rev_entries
                        FROM directory WHERE id=%s),
            ls_d AS (SELECT dir_id, UNNEST(dir_entries)  AS entry_id FROM dir),
            ls_f AS (SELECT dir_id, UNNEST(file_entries) AS entry_id FROM dir),
            ls_r AS (SELECT dir_id, UNNEST(rev_entries)  AS entry_id FROM dir)
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
            ORDER BY name
            """,
            (id,),
        )
        return [
            {"type": row[0], "target": row[1], "name": row[2]}
            for row in cursor.fetchall()
        ]

    def iter_origins(self):
        raise NotImplementedError

    def iter_origin_visits(self, origin: str):
        raise NotImplementedError

    def iter_origin_visit_statuses(self, origin: str, visit: int):
        raise NotImplementedError

    def release_get(self, ids: List[bytes]):
        raise NotImplementedError

    def revision_get(self, ids: List[bytes]):
        raise NotImplementedError

    def snapshot_get_all_branches(self, snapshot: bytes):
        raise NotImplementedError
