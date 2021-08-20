# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib
import os


def path_id(path: bytes) -> bytes:
    return hashlib.sha1(path).digest()


def path_normalize(path: bytes) -> bytes:
    return path[2:] if path.startswith(bytes("." + os.path.sep, "utf-8")) else path
