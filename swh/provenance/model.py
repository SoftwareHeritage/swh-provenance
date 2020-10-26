import os

from .archive import ArchiveInterface

from pathlib import PosixPath


class Tree:
    def __init__(self, archive: ArchiveInterface, id: bytes):
        self.root = DirectoryEntry(archive, id, PosixPath('.'))


class TreeEntry:
    def __init__(self, id: bytes, name: PosixPath):
        self.id = id
        self.name = name


class DirectoryEntry(TreeEntry):
    def __init__(self, archive: ArchiveInterface, id: bytes, name: PosixPath):
        super().__init__(id, name)
        self.archive = archive
        self.children = None

    def __iter__(self):
        if self.children is None:
            self.children = []
            for child in self.archive.directory_ls(self.id):
                if child['type'] == 'dir':
                    self.children.append(DirectoryEntry(
                        self.archive,
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
