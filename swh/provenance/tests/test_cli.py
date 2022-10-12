# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, List

from _pytest.monkeypatch import MonkeyPatch
from click.testing import CliRunner
import psycopg2.extensions
import pytest

from swh.core.cli import swh as swhmain
import swh.core.cli.db  # noqa ; ensure cli is loaded
from swh.core.db import BaseDb
from swh.core.db.db_utils import init_admin_extensions
from swh.model.hashutil import MultiHash
import swh.provenance.cli  # noqa ; ensure cli is loaded
from swh.provenance.tests.conftest import fill_storage, load_repo_data
from swh.storage.interface import StorageInterface

from .conftest import get_datafile
from .test_utils import invoke, write_configuration_path


def test_cli_swh_db_help() -> None:
    # swhmain.add_command(provenance_cli)
    result = CliRunner().invoke(swhmain, ["provenance", "-h"])
    assert result.exit_code == 0
    assert "Commands:" in result.output
    commands = result.output.split("Commands:")[1]
    for command in (
        "find-all",
        "find-first",
        "iter-frontiers",
        "iter-origins",
        "iter-revisions",
    ):
        assert f"  {command} " in commands


TABLES = {
    "dbflavor",
    "dbmodule",
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


@pytest.mark.parametrize("flavor", ("normalized", "denormalized"))
def test_cli_db_create_and_init_db_with_flavor(
    monkeypatch: MonkeyPatch,
    postgresql: psycopg2.extensions.connection,
    flavor: str,
) -> None:
    """Test that 'swh db init provenance' works with flavors"""

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
        assert tables == TABLES


def test_cli_init_db_default_flavor(postgresql: psycopg2.extensions.connection) -> None:
    "Test that 'swh db init provenance' defaults to a normalized flavored DB"

    dbname = postgresql.dsn
    init_admin_extensions("swh.provenance", dbname)
    result = CliRunner().invoke(swhmain, ["db", "init", "-d", dbname, "provenance"])
    assert result.exit_code == 0, result.output

    with postgresql.cursor() as cur:
        cur.execute("select swh_get_dbflavor()")
        assert cur.fetchone() == ("normalized",)


@pytest.mark.origin_layer
@pytest.mark.parametrize(
    "subcommand",
    (["origin", "from-csv"], ["iter-origins"]),
)
def test_cli_origin_from_csv(
    swh_storage: StorageInterface,
    subcommand: List[str],
    swh_storage_backend_config: Dict,
    provenance,
    tmp_path,
):
    repo = "cmdbts2"
    origin_url = f"https://{repo}"
    data = load_repo_data(repo)
    fill_storage(swh_storage, data)

    assert len(data["origin"]) >= 1
    assert origin_url in [o["url"] for o in data["origin"]]

    cfg = {
        "provenance": {
            "archive": {
                "cls": "api",
                "storage": swh_storage_backend_config,
            },
            "storage": {
                "cls": "postgresql",
                # "db": provenance.storage.conn.dsn,
                "db": provenance.storage.conn.get_dsn_parameters(),
            },
        },
    }

    config_path = write_configuration_path(cfg, tmp_path)

    csv_filepath = get_datafile("origins.csv")
    subcommand = subcommand + [csv_filepath]

    result = invoke(subcommand, config_path)
    assert result.exit_code == 0, f"Unexpected result: {result.output}"

    origin_sha1 = MultiHash.from_data(
        origin_url.encode(), hash_names=["sha1"]
    ).digest()["sha1"]
    actual_result = provenance.storage.origin_get([origin_sha1])

    assert actual_result == {origin_sha1: origin_url}
