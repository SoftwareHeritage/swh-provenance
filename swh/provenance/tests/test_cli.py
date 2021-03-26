# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from click.testing import CliRunner
import psycopg2
import pytest
import yaml

from swh.core.cli import swh as swhmain
import swh.core.cli.db  # noqa ; ensure cli is loaded
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
        "provenance": {
            "cls": "local",
            "with_path": True,
        },
    }
    yaml.dump(conf, conffile.open("w"))
    result = CliRunner().invoke(
        swhmain, ["provenance", "--config-file", str(conffile), "create", "--drop"]
    )
    assert result.exit_code == 0, result.output
    assert "DeprecationWarning" in result.output


TABLES = {
    "dbflavor",
    "dbversion",
    "content",
    "content_early_in_rev",
    "content_in_dir",
    "directory",
    "directory_in_rev",
    "origin",
    "revision",
    "revision_before_rev",
    "revision_in_org",
}


@pytest.mark.parametrize(
    "flavor, dbtables", (("with-path", TABLES | {"location"}), ("without-path", TABLES))
)
def test_cli_db_create_and_init_db_with_flavor(
    monkeypatch, postgresql, flavor, dbtables
):
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
    cnx = psycopg2.connect(**db_params)
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


def test_cli_init_db_default_flavor(provenance_db):
    "Test that 'swh db init provenance' defaults to a with-path flavored DB"
    dbname = provenance_db.dsn
    result = CliRunner().invoke(swhmain, ["db", "init", "-d", dbname, "provenance"])
    assert result.exit_code == 0, result.output

    with provenance_db.cursor() as cur:
        cur.execute("select swh_get_dbflavor()")
        assert cur.fetchone() == ("with-path",)