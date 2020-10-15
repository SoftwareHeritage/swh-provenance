import os

from pathlib import PosixPath
from swh.storage.interface import StorageInterface


class Tree:
    def __init__(self, storage: StorageInterface, id: bytes):
        self.root = DirectoryEntry(storage, id, PosixPath('.'))


class TreeEntry:
    def __init__(self, id: bytes, name: PosixPath):
        self.id = id
        self.name = name


class DirectoryEntry(TreeEntry):
    def __init__(self, storage: StorageInterface, id: bytes, name: PosixPath):
        super().__init__(id, name)
        self.storage = storage
        self.children = None

    def __iter__(self):
        if self.children is None:
            self.children = []
            for child in self.storage.directory_ls(self.id):
                if child['type'] == 'dir':
                    self.children.append(DirectoryEntry(
                        self.storage,
                        child['target'],
                        PosixPath(os.fsdecode(child['name']))
                    ))

                elif child['type'] == 'file':
                    self.children.append(FileEntry(
                        child['target'],
                        PosixPath(os.fsdecode(child['name']))
                    ))

        return iter(self.children)


class FileEntry(TreeEntry):
    pass
