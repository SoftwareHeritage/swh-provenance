import operator
import os
# import psycopg2
import swh.storage.interface

from pathlib import PosixPath

# from swh.storage.postgresql.db import Db

CONTENT = "file"
DIRECTORY = "dir"

# OTYPE_IDX = 1
# PATH_IDX = 3
# SWHID_IDX = 2


class Tree:
    def __init__(self, storage: swh.storage.interface.StorageInterface, swhid: str):
        self.root = DirectoryEntry(storage, swhid, PosixPath('.'))


class TreeEntry:
    def __init__(self, swhid: str, name: PosixPath):
        self.swhid = swhid
        self.name = name


class DirectoryEntry(TreeEntry):
    def __init__(
        self,
        storage: swh.storage.interface.StorageInterface,
        swhid: str,
        name: PosixPath
    ):
        super().__init__(swhid, name)
        self.storage = storage
        self.children = None

    def __iter__(self):
        if self.children is None:
            self.children = []
            for child in self.storage.directory_ls(self.swhid):
                if child['type'] == CONTENT:
                    self.children.append(FileEntry(
                        child['target'],
                        PosixPath(os.fsdecode(child['name']))
                    ))

                elif child['type'] == DIRECTORY:
                    self.children.append(DirectoryEntry(
                        self.storage,
                        child['target'],
                        PosixPath(os.fsdecode(child['name']))
                    ))

        return iter(self.children)


class FileEntry(TreeEntry):
    pass
