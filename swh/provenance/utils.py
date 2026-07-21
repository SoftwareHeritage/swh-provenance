# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import contextlib
import logging
from pathlib import Path
import shutil
from typing import Iterator, Union

import luigi

logger = logging.getLogger(__name__)


def _try_delete(path):
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass
    except NotADirectoryError:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


@contextlib.contextmanager
def atomic_path(path: Union[str, Path, luigi.LocalTarget]) -> Iterator[Path]:
    if isinstance(path, luigi.LocalTarget):
        path = Path(path.path)
    elif isinstance(path, str):
        path = Path(path)

    if path.exists():
        # no guarantee it won't be created while twe are running,
        # but this should catch most logic errors.
        raise FileExistsError(f"{path} already exists")

    tmp_path = Path(f"{path}.tmp")

    _try_delete(tmp_path)
    try:
        yield tmp_path
    except BaseException as e:
        _try_delete(tmp_path)
        raise e
    else:
        # commit
        _try_delete(path)
        tmp_path.rename(path)
