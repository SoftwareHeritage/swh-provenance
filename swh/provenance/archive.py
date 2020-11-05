import psycopg2

from .db_utils import connect

from typing import List

from swh.storage import get_storage


class ArchiveInterface:
    def __init__(self):
        raise NotImplementedError

    def directory_ls(self, id: bytes):
        raise NotImplementedError

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


class ArchiveStorage(ArchiveInterface):
    def __init__(self, cls: str, **kwargs):
        self.storage = get_storage(cls, **kwargs)

    def directory_ls(self, id: bytes):
        # TODO: filter unused fields
        yield from self.storage.directory_ls(id)

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

    def release_get(self, ids: List[bytes]):
        # TODO: filter unused fields
        yield from self.storage.release_get(ids)

    def revision_get(self, ids: List[bytes]):
        # TODO: filter unused fields
        yield from self.storage.revision_get(ids)

    def snapshot_get_all_branches(self, snapshot: bytes):
        from swh.storage.algos.snapshot import snapshot_get_all_branches
        # TODO: filter unused fields
        return snapshot_get_all_branches(self.storage, snapshot)


class Archive(ArchiveInterface):
    def __init__(self, conn: psycopg2.extensions.connection):
        self.conn = conn
        self.cursor = conn.cursor()

    def directory_ls(self, id: bytes):
        self.cursor.execute('''WITH
    dir  AS (SELECT id AS dir_id, dir_entries, file_entries, rev_entries
	         FROM directory WHERE id=%s),
    ls_d AS (SELECT dir_id, unnest(dir_entries) AS entry_id from dir),
    ls_f AS (SELECT dir_id, unnest(file_entries) AS entry_id from dir),
    ls_r AS (SELECT dir_id, unnest(rev_entries) AS entry_id from dir)
    (SELECT 'dir'::directory_entry_type AS type, e.target, e.name, NULL::sha1_git
     FROM ls_d
     LEFT JOIN directory_entry_dir e ON ls_d.entry_id=e.id)
    UNION
    (WITH known_contents AS
	(SELECT 'file'::directory_entry_type AS type, e.target, e.name, c.sha1_git
         FROM ls_f
         LEFT JOIN directory_entry_file e ON ls_f.entry_id=e.id
         INNER JOIN content c ON e.target=c.sha1_git)
    SELECT * FROM known_contents
	UNION
	(SELECT 'file'::directory_entry_type AS type, e.target, e.name, c.sha1_git
         FROM ls_f
         LEFT JOIN directory_entry_file e ON ls_f.entry_id=e.id
         LEFT JOIN skipped_content c ON e.target=c.sha1_git
         WHERE NOT EXISTS (SELECT 1 FROM known_contents WHERE known_contents.sha1_git=e.target)))
    ORDER BY name
        ''', (id,))
        for row in self.cursor.fetchall():
            yield {'type': row[0], 'target': row[1], 'name': row[2]}


def get_archive(cls: str, **kwargs) -> ArchiveInterface:
    if cls == "api":
        return ArchiveStorage(**kwargs["storage"])
    elif cls == "ps":
        conn = connect(kwargs["db"])
        return Archive(conn)
    else:
        raise NotImplementedError
