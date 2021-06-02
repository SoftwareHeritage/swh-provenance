from typing import Any, Dict, Iterable, List

from methodtools import lru_cache
import psycopg2

from swh.model.model import Revision
from swh.storage.postgresql.storage import Storage


class ArchivePostgreSQL:
    def __init__(self, conn: psycopg2.extensions.connection):
        self.conn = conn
        self.storage = Storage(conn, objstorage={"cls": "memory"})

    def directory_ls(self, id: bytes) -> List[Dict[str, Any]]:
        # TODO: only call directory_ls_internal if the id is not being queried by
        # someone else. Otherwise wait until results get properly cached.
        entries = self.directory_ls_internal(id)
        return entries

    @lru_cache(maxsize=100000)
    def directory_ls_internal(self, id: bytes) -> List[Dict[str, Any]]:
        # TODO: add file size filtering
        with self.conn.cursor() as cursor:
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
        from swh.storage.algos.origin import iter_origins

        yield from iter_origins(self.storage)

    def iter_origin_visits(self, origin: str):
        from swh.storage.algos.origin import iter_origin_visits

        # TODO: filter unused fields
        yield from iter_origin_visits(self.storage, origin)

    def iter_origin_visit_statuses(self, origin: str, visit: int):
        from swh.storage.algos.origin import iter_origin_visit_statuses

        # TODO: filter unused fields
        yield from iter_origin_visit_statuses(self.storage, origin, visit)

    def release_get(self, ids: Iterable[bytes]):
        # TODO: filter unused fields
        yield from self.storage.release_get(list(ids))

    def revision_get(self, ids: Iterable[bytes]):
        with self.conn.cursor() as cursor:
            psycopg2.extras.execute_values(
                cursor,
                """
                SELECT t.id, revision.date, revision.directory,
                    ARRAY(
                        SELECT rh.parent_id::bytea
                            FROM revision_history rh
                            WHERE rh.id = t.id
                            ORDER BY rh.parent_rank
                    )
                    FROM (VALUES %s) as t(sortkey, id)
                    LEFT JOIN revision ON t.id = revision.id
                    LEFT JOIN person author ON revision.author = author.id
                    LEFT JOIN person committer ON revision.committer = committer.id
                    ORDER BY sortkey
                """,
                ((sortkey, id) for sortkey, id in enumerate(ids)),
            )
            for row in cursor.fetchall():
                parents = []
                for parent in row[3]:
                    if parent:
                        parents.append(parent)
                yield Revision.from_dict(
                    {
                        "id": row[0],
                        "date": row[1],
                        "directory": row[2],
                        "parents": tuple(parents),
                    }
                )

    def snapshot_get_all_branches(self, snapshot: bytes):
        from swh.storage.algos.snapshot import snapshot_get_all_branches

        # TODO: filter unused fields
        return snapshot_get_all_branches(self.storage, snapshot)
