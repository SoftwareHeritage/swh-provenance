from .archive import ArchiveInterface


class TreeEntry:
    def __init__(self, id: bytes, name: bytes):
        self.id = id
        self.name = name


class DirectoryEntry(TreeEntry):
    def __init__(self, archive: ArchiveInterface, id: bytes, name: bytes):
        super().__init__(id, name)
        self.archive = archive
        self.children = None

    def __iter__(self):
        if self.children is None:
            self.children = []
            for child in self.archive.directory_ls(self.id):
                if child["type"] == "dir":
                    self.children.append(
                        DirectoryEntry(
                            self.archive,
                            child["target"],
                            child["name"]
                        )
                    )

                elif child["type"] == "file":
                    self.children.append(FileEntry(child["target"], child["name"]))

        return iter(self.children)


class FileEntry(TreeEntry):
    pass
