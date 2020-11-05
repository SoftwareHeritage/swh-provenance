import psycopg2

from ..archive import ArchiveInterface

from typing import List


class ArchivePostgreSQL(ArchiveInterface):
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
