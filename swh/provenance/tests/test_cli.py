# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Set

from _pytest.monkeypatch import MonkeyPatch
from click.testing import CliRunner
import psycopg2.extensions
import pytest

from swh.core.cli import swh as swhmain
import swh.core.cli.db  # noqa ; ensure cli is loaded
from swh.core.db import BaseDb
import swh.provenance.cli  # noqa ; ensure cli is loaded


def test_cli_swh_db_help() -> None:
    # swhmain.add_command(provenance_cli)
    result = CliRunner().invoke(swhmain, ["provenance", "-h"])
    assert result.exit_code == 0
    assert "Commands:" in result.output
    commands = result.output.split("Commands:")[1]
    for command in (
        "find-all",
        "find-first",
        "iter-origins",
        "iter-revisions",
    ):
        assert f"  {command} " in commands


TABLES = {
    "dbflavor",
    "dbversion",
    "content",
    "content_in_revision",
    "content_in_directory",
    "directory",
    "directory_in_revision",
    "location",
    "origin",
    "revision",
    "revision_before_revision",
    "revision_in_origin",
}


@pytest.mark.parametrize(
    "flavor, dbtables", (("with-path", TABLES | {"location"}), ("without-path", TABLES))
)
def test_cli_db_create_and_init_db_with_flavor(
    monkeypatch: MonkeyPatch,
    postgresql: psycopg2.extensions.connection,
    flavor: str,
    dbtables: Set[str],
) -> None:
    """Test that 'swh db init provenance' works with flavors

    for both with-path and without-path flavors"""

    dbname = f"{flavor}-db"

    # DB creation using 'swh db create'
    db_params = postgresql.get_dsn_parameters()
    monkeypatch.setenv("PGHOST", db_params["host"])
    monkeypatch.setenv("PGUSER", db_params["user"])
    monkeypatch.setenv("PGPORT", db_params["port"])
    result = CliRunner().invoke(swhmain, ["db", "create", "-d", dbname, "provenance"])
    assert result.exit_code == 0, result.output

    # DB init using 'swh db init'
    result = CliRunner().invoke(
        swhmain, ["db", "init", "-d", dbname, "--flavor", flavor, "provenance"]
    )
    assert result.exit_code == 0, result.output
    assert f"(flavor {flavor})" in result.output

    db_params["dbname"] = dbname
    cnx = BaseDb.connect(**db_params).conn
    # check the DB looks OK (check for db_flavor and expected tables)
    with cnx.cursor() as cur:
        cur.execute("select swh_get_dbflavor()")
        assert cur.fetchone() == (flavor,)

        cur.execute(
            "select table_name from information_schema.tables "
            "where table_schema = 'public' "
            f"and table_catalog = '{dbname}'"
        )
        tables = set(x for (x,) in cur.fetchall())
        assert tables == dbtables


def test_cli_init_db_default_flavor(postgresql: psycopg2.extensions.connection) -> None:
    "Test that 'swh db init provenance' defaults to a with-path flavored DB"
    dbname = postgresql.dsn
    result = CliRunner().invoke(swhmain, ["db", "init", "-d", dbname, "provenance"])
    assert result.exit_code == 0, result.output

    with postgresql.cursor() as cur:
        cur.execute("select swh_get_dbflavor()")
        assert cur.fetchone() == ("with-path",)
