# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterable, List, Tuple

import psycopg2.extensions

from swh.core.statsd import statsd
from swh.model.model import Sha1Git
from swh.storage import get_storage

ARCHIVE_DURATION_METRIC = "swh_provenance_archive_direct_duration_seconds"


class ArchivePostgreSQL:
    def __init__(self, conn: psycopg2.extensions.connection) -> None:
        self.storage = get_storage(
            "postgresql", db=conn.dsn, objstorage={"cls": "memory"}
        )
        self.conn = conn

    def directory_ls(self, id: Sha1Git, minsize: int = 0) -> Iterable[Dict[str, Any]]:
        yield from self._directory_ls(id, minsize=minsize)

    @statsd.timed(metric=ARCHIVE_DURATION_METRIC, tags={"method": "directory_ls"})
    def _directory_ls(self, id: Sha1Git, minsize: int = 0) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            if minsize > 0:
                cursor.execute(
                    """
                    WITH
                    dir AS (SELECT dir_entries, file_entries
                              FROM directory WHERE id=%s),
                    ls_d AS (SELECT DISTINCT UNNEST(dir_entries) AS entry_id FROM dir),
                    ls_f AS (SELECT DISTINCT UNNEST(file_entries) AS entry_id FROM dir)
                    (SELECT 'dir' AS type, e.target, e.name
                       FROM ls_d
                       LEFT JOIN directory_entry_dir e ON ls_d.entry_id=e.id)
                    UNION ALL
                    (WITH known_contents AS
                       (SELECT 'file' AS type, e.target, e.name
                          FROM ls_f
                          LEFT JOIN directory_entry_file e ON ls_f.entry_id=e.id
                          INNER JOIN content c ON e.target=c.sha1_git
                          WHERE c.length >= %s
                       )
                       SELECT * FROM known_contents
                       UNION ALL
                       (SELECT 'file' AS type, e.target, e.name
                          FROM ls_f
                          LEFT JOIN directory_entry_file e ON ls_f.entry_id=e.id
                          LEFT JOIN skipped_content c ON e.target=c.sha1_git
                          WHERE NOT EXISTS (
                            SELECT 1 FROM known_contents
                              WHERE known_contents.target=e.target
                          )
                          AND c.length >= %s
                       )
                    )
                    """,
                    (id, minsize, minsize),
                )
            else:
                cursor.execute(
                    """
                    WITH
                    dir AS (SELECT dir_entries, file_entries
                              FROM directory WHERE id=%s),
                    ls_d AS (SELECT DISTINCT UNNEST(dir_entries) AS entry_id FROM dir),
                    ls_f AS (SELECT DISTINCT UNNEST(file_entries) AS entry_id FROM dir)
                    (SELECT 'dir' AS type, e.target, e.name
                       FROM ls_d
                       LEFT JOIN directory_entry_dir e ON ls_d.entry_id=e.id)
                    UNION ALL
                    (SELECT 'file' AS type, e.target, e.name
                       FROM ls_f
                       LEFT JOIN directory_entry_file e ON ls_f.entry_id=e.id)
                    """,
                    (id,),
                )

            entries = []
            for entry_type, target, name in cursor:
                if target is None or name is None:
                    # LEFT JOIN returned a NULL on the right hand side:
                    # directory_entry_{dir,file} are not up to date.
                    return []
                entries.append({"type": entry_type, "target": target, "name": name})
            return entries

    def revision_get_some_outbound_edges(
        self, revision_id: Sha1Git
    ) -> Iterable[Tuple[Sha1Git, Sha1Git]]:
        yield from self._revision_get_some_outbound_edges(revision_id)

    @statsd.timed(
        metric=ARCHIVE_DURATION_METRIC,
        tags={"method": "revision_get_some_outbound_edges"},
    )
    def _revision_get_some_outbound_edges(
        self, revision_id: Sha1Git
    ) -> List[Tuple[Sha1Git, Sha1Git]]:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                select
                  id, unnest(parents) as parent_id
                from
                  swh_revision_list(ARRAY[%s::bytea], 1000);
                """,
                (revision_id,),
            )
            return cursor.fetchall()

    @statsd.timed(metric=ARCHIVE_DURATION_METRIC, tags={"method": "snapshot_get_heads"})
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
                         )
                SELECT id FROM heads
                """,
                (id,),
            )
            yield from (row[0] for row in cursor)
