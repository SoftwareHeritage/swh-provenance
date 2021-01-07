from typing import Any, Dict, List


class ArchiveInterface:
    def __init__(self, **kwargs):
        raise NotImplementedError

    def directory_ls(self, id: bytes) -> List[Dict[str, Any]]:
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
