# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Decoder and encoders for swh-model SWHID objects."""

from typing import Callable, Dict, List, Tuple

from swh.model import swhids

ENCODERS: List[Tuple[type, str, Callable]] = [
    (swhids.CoreSWHID, "core_swhid", str),
    (swhids.ExtendedSWHID, "extended_swhid", str),
    (swhids.QualifiedSWHID, "qualified_swhid", str),
]

DECODERS: Dict[str, Callable] = {
    "core_swhid": swhids.CoreSWHID.from_string,
    "extended_swhid": swhids.ExtendedSWHID.from_string,
    "qualified_swhid": swhids.QualifiedSWHID.from_string,
}
