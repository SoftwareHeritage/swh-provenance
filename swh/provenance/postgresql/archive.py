from typing import Any, Dict, Iterable, List, Set

from methodtools import lru_cache
import psycopg2

from swh.model.model import ObjectType, Sha1Git, TargetType
from swh.storage.postgresql.storage import Storage


class ArchivePostgreSQL:
    def __init__(self, conn: psycopg2.extensions.connection):
        self.conn = conn
        self.storage = Storage(conn, objstorage={"cls": "memory"})

    def directory_ls(self, id: Sha1Git) -> Iterable[Dict[str, Any]]:
        # TODO: only call directory_ls_internal if the id is not being queried by
        # someone else. Otherwise wait until results get properly cached.
        entries = self.directory_ls_internal(id)
        yield from entries

    @lru_cache(maxsize=100000)
    def directory_ls_internal(self, id: Sha1Git) -> List[Dict[str, Any]]:
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
            yield from (row[0] for row in cursor.fetchall())

    def snapshot_get_heads(self, id: Sha1Git) -> Iterable[Sha1Git]:
        # TODO: this code is duplicated here (same as in swh.provenance.storage.archive)
        # but it's just temporary. This method should actually perform a direct query to
        # the SQL db of the archive.
        from swh.core.utils import grouper
        from swh.storage.algos.snapshot import snapshot_get_all_branches

        snapshot = snapshot_get_all_branches(self.storage, id)
        assert snapshot is not None

        targets_set = set()
        releases_set = set()
        if snapshot is not None:
            for branch in snapshot.branches:
                if snapshot.branches[branch].target_type == TargetType.REVISION:
                    targets_set.add(snapshot.branches[branch].target)
                elif snapshot.branches[branch].target_type == TargetType.RELEASE:
                    releases_set.add(snapshot.branches[branch].target)

        batchsize = 100
        for releases in grouper(releases_set, batchsize):
            targets_set.update(
                release.target
                for release in self.storage.release_get(releases)
                if release is not None and release.target_type == ObjectType.REVISION
            )

        revisions: Set[Sha1Git] = set()
        for targets in grouper(targets_set, batchsize):
            revisions.update(
                revision.id
                for revision in self.storage.revision_get(targets)
                if revision is not None
            )

        yield from revisions
