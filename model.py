import operator
import psycopg2

from pathlib import PosixPath

from swh.storage.db import Db

CONTENT = "file"
DIRECTORY = "dir"

OTYPE_IDX = 1
PATH_IDX = 3
SWHID_IDX = 2


class Tree:
    def __init__(self, conn: psycopg2.extensions.connection, swhid: str):
        self.root = DirectoryEntry(conn, swhid, PosixPath('.'))


class TreeEntry:
    def __init__(self, swhid: str, name: PosixPath):
        self.swhid = swhid
        self.name = name


class DirectoryEntry(TreeEntry):
    def __init__(
        self,
        conn: psycopg2.extensions.connection,
        swhid: str,
        name: PosixPath
    ):
        super().__init__(swhid, name)
        self.conn = conn
        self.children = None

    def __iter__(self):
        if self.children is None:
            self.children = []
            storage = Db(self.conn)
            for child in storage.directory_walk_one(self.swhid):
                if child[OTYPE_IDX] == CONTENT:
                    self.children.append(FileEntry(
                        child[SWHID_IDX],
                        PosixPath(child[PATH_IDX].decode('utf-8'))
                    ))

                elif child[OTYPE_IDX] == DIRECTORY:
                    self.children.append(DirectoryEntry(
                        self.conn,
                        child[SWHID_IDX],
                        PosixPath(child[PATH_IDX].decode('utf-8'))
                    ))

        return iter(self.children)


class FileEntry(TreeEntry):
    pass
