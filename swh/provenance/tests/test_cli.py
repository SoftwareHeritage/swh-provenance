# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import yaml
from click.testing import CliRunner
from psycopg2.extensions import parse_dsn

import swh.provenance.cli  # noqa ; ensure cli is loaded
from swh.core.cli import swh as swhmain
from swh.core.db.pytest_plugin import postgresql_fact

pytest_plugins = ["swh.storage.pytest_plugin"]

provenance_db = postgresql_fact("postgresql_proc", db_name="provenance",)


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


@pytest.mark.parametrize("with_path", (True, False))
def test_cli_create(provenance_db, tmp_path, with_path):
    conffile = tmp_path / "config.yml"
    dsn = parse_dsn(provenance_db.dsn)
    dsn["dbname"] = "test_provenance"
    conf = {
        "provenance": {"cls": "local", "with_path": with_path, "db": dsn,},
    }
    yaml.dump(conf, conffile.open("w"))
    result = CliRunner().invoke(
        swhmain, ["provenance", "--config-file", str(conffile), "create", "--drop"]
    )
    assert result.exit_code == 0, result.output

    # this will fail because the db already exists
    result = CliRunner().invoke(
        swhmain, ["provenance", "--config-file", str(conffile), "create"]
    )
    assert result.exit_code == 1, result.output
