import psycopg2

from ..archive import ArchiveInterface

# from functools import lru_cache
from methodtools import lru_cache
from typing import List
from swh.storage import get_storage


class ArchiveStorage(ArchiveInterface):
    def __init__(self, cls: str, **kwargs):
        self.storage = get_storage(cls, **kwargs)

    @lru_cache(maxsize=1000000)
    def directory_ls(self, id: bytes):
        # TODO: filter unused fields
        return [entry for entry in self.storage.directory_ls(id)]

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
