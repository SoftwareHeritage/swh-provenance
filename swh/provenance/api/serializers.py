# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from dataclasses import asdict
from typing import Callable, Dict, List, Tuple

from .. import interface


def _encode_dataclass(obj):
    return {
        **asdict(obj),
        "__type__": type(obj).__name__,
    }


def _decode_dataclass(d):
    return getattr(interface, d.pop("__type__"))(**d)


def _encode_enum(obj):
    return {
        "value": obj.value,
        "__type__": type(obj).__name__,
    }


def _decode_enum(d):
    return getattr(interface, d.pop("__type__"))(d["value"])


ENCODERS: List[Tuple[type, str, Callable]] = [
    (interface.ProvenanceResult, "dataclass", _encode_dataclass),
    (interface.RelationData, "dataclass", _encode_dataclass),
    (interface.RevisionData, "dataclass", _encode_dataclass),
    (interface.EntityType, "enum", _encode_enum),
    (interface.RelationType, "enum", _encode_enum),
    (set, "set", list),
]


DECODERS: Dict[str, Callable] = {
    "dataclass": _decode_dataclass,
    "enum": _decode_enum,
    "set": set,
}
