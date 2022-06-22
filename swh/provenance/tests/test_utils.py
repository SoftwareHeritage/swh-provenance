# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from os.path import join
from typing import Dict, List

from click.testing import CliRunner, Result
from yaml import safe_dump

from swh.provenance.cli import cli


def invoke(args: List[str], config_path: str, catch_exceptions: bool = False) -> Result:
    """Invoke swh journal subcommands"""
    runner = CliRunner()
    result = runner.invoke(cli, ["-C" + config_path] + args)
    if not catch_exceptions and result.exception:
        print(result.output)
        raise result.exception
    return result


def write_configuration_path(config: Dict, tmp_path: str) -> str:
    """Serialize yaml dict on disk given a configuration dict and and a temporary path."""
    config_path = join(str(tmp_path), "config.yml")
    with open(config_path, "w") as f:
        f.write(safe_dump(config))
    return config_path
