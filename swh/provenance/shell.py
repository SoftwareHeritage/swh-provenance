# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path
from typing import Optional

from swh.graph.shell import Rust as _Rust


class Rust(_Rust):
    """Subclass of :class:`swh.graph.shell.Rust` that runs executables from
    swh-dataset's binary directory instead of swh-graph's.
    """

    def __init__(
        self, *args, base_rust_executable_dir: Optional[Path] = None, **kwargs
    ):
        if base_rust_executable_dir is None:
            # in editable installs, __file__ is a symlink to the original file in
            # the source directory, which is where in the end the rust sources and
            # executable are. So resolve the symlink before looking for the target/
            # directory relative to the actual python file.
            path = Path(__file__).resolve()
            base_rust_executable_dir = path.parent.parent.parent / "target"
        super().__init__(
            *args, base_rust_executable_dir=base_rust_executable_dir, **kwargs
        )
