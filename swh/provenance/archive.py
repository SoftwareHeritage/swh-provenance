import psycopg2

from .db_utils import connect

from typing import List

from swh.storage import get_storage


class ArchiveInterface:
    def __init__(self):
        raise NotImplementedError

    def directory_ls(self, id: bytes):
        raise NotImplementedError

    def revision_get(self, ids: List[bytes]):
        raise NotImplementedError


class ArchiveStorage(ArchiveInterface):
    def __init__(self, cls: str, **kwargs):
        self.storage = get_storage(cls, **kwargs)

    def directory_ls(self, id: bytes):
        # TODO: filter unused fields
        yield from self.storage.directory_ls(id)

    def revision_get(self, ids: List[bytes]):
        # TODO: filter unused fields
        yield from self.storage.revision_get(ids)


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
    UNION
    (SELECT 'rev'::directory_entry_type AS type, e.target, e.name, NULL::sha1_git
     FROM ls_r
     LEFT JOIN directory_entry_rev e ON ls_r.entry_id=e.id)
    ORDER BY name
        ''', (id,))
        for row in self.cursor.fetchall():
            yield {'type': row[0], 'target': row[1], 'name': row[2]}

    def revision_get(self, ids: List[bytes]):
        raise NotImplementedError


def get_archive(cls: str, **kwargs) -> ArchiveInterface:
    if cls == "api":
        return ArchiveStorage(**kwargs["storage"])
    elif cls == "ps":
        conn = connect(kwargs["db"])
        return Archive(conn)
    else:
        raise NotImplementedError
