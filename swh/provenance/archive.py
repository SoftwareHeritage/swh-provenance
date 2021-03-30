from typing import Any, Dict, Iterable, List

from typing_extensions import Protocol, runtime_checkable


@runtime_checkable
class ArchiveInterface(Protocol):
    def directory_ls(self, id: bytes) -> List[Dict[str, Any]]:
        ...

    def iter_origins(self):
        ...

    def iter_origin_visits(self, origin: str):
        ...

    def iter_origin_visit_statuses(self, origin: str, visit: int):
        ...

    def release_get(self, ids: Iterable[bytes]):
        ...

    def revision_get(self, ids: Iterable[bytes]):
        ...

    def snapshot_get_all_branches(self, snapshot: bytes):
        ...
