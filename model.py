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
        self.root = DirectoryEntry(swhid, PosixPath('.'))

        storage = Db(conn)
        entries = list(map(
            lambda x: (x[SWHID_IDX], x[PATH_IDX].decode('utf-8'), x[OTYPE_IDX]),
            storage.directory_walk(swhid)
        ))
        entries.sort(key=operator.itemgetter(1))

        for entry in entries:
            self.root.addChild(entry[0], PosixPath(entry[1]), entry[2])


class TreeEntry:
    def __init__(self, swhid: str, name: PosixPath):
        self.swhid = swhid
        self.name = name


class DirectoryEntry(TreeEntry):
    def __init__(self, swhid: str, name: PosixPath):
        super().__init__(swhid, name)
        self.children = []

    def addChild(self, swhid: str, path: PosixPath, otype: str):
        if path.parent == PosixPath('.'):
            if otype == CONTENT:
                self.children.append(FileEntry(swhid, path.name))

            elif otype == DIRECTORY:
                self.children.append(DirectoryEntry(swhid, path.name))

        else:
            for child in filter(lambda x: isinstance(x, DirectoryEntry), self.children):
                if path.parts[0] == child.name:
                    child.addChild(swhid, PosixPath(*path.parts[1:]), otype)
                    break


class FileEntry(TreeEntry):
    pass
