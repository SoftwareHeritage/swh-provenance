# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from click.testing import CliRunner
import yaml

from swh.core.cli import swh as swhmain
import swh.provenance.cli  # noqa ; ensure cli is loaded


def test_cli_swh_db_help():
    # swhmain.add_command(provenance_cli)
    result = CliRunner().invoke(swhmain, ["provenance", "-h"])
    assert result.exit_code == 0
    assert "Commands:" in result.output
    commands = result.output.split("Commands:")[1]
    for command in (
        "create",
        "find-all",
        "find-first",
        "iter-origins",
        "iter-revisions",
    ):
        assert f"  {command} " in commands


def test_cli_create_deprecated(provenance_db, tmp_path):
    conffile = tmp_path / "config.yml"
    conf = {
        "provenance": {"cls": "local", "with_path": True,},
    }
    yaml.dump(conf, conffile.open("w"))
    result = CliRunner().invoke(
        swhmain, ["provenance", "--config-file", str(conffile), "create", "--drop"]
    )
    assert result.exit_code == 0, result.output
    assert "DeprecationWarning" in result.output
