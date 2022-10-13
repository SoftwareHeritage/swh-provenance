# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import logging
import tempfile
from typing import Dict, List, Optional

from click.testing import CliRunner, Result
from yaml import safe_dump

from swh.provenance.cli import cli


def invoke(
    args: List[str], config: Optional[Dict] = None, catch_exceptions: bool = False
) -> Result:
    """Invoke swh journal subcommands"""
    runner = CliRunner()
    with tempfile.NamedTemporaryFile("a", suffix=".yml") as config_fd:
        if config is not None:
            safe_dump(config, config_fd)
            config_fd.seek(0)
            args = ["-C" + config_fd.name] + args

        result = runner.invoke(cli, args, obj={"log_level": logging.DEBUG}, env=None)
        if not catch_exceptions and result.exception:
            print(result.output)
            raise result.exception
    return result
