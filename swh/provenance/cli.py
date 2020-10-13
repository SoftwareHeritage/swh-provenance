# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import os
from typing import Any, Dict, Optional

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import click
import yaml

from swh.core import config
from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group
from swh.core.db import db_utils    # TODO: remove this in favour of local db_utils module
from swh.model.hashutil import (hash_to_bytes, hash_to_hex)
from swh.storage import get_storage

# All generic config code should reside in swh.core.config
CONFIG_ENVVAR = "SWH_CONFIG_FILE"
DEFAULT_CONFIG_PATH = os.path.join(click.get_app_dir("swh"), "global.yml")
DEFAULT_PATH = os.environ.get(CONFIG_ENVVAR, DEFAULT_CONFIG_PATH)

DEFAULT_CONFIG: Dict[str, Any] = {
    "storage": {
        "cls": "remote",
        "url": "http://uffizi.internal.softwareheritage.org:5002"
    },
    "db": "postgresql://postgres:postgres@localhost/provenance" # TODO: fix this!
}


CONFIG_FILE_HELP = f"""Configuration file:

\b
The CLI option or the environment variable will fail if invalid.
CLI option is checked first.
Then, environment variable {CONFIG_ENVVAR} is checked.
Then, if cannot load the default path, a set of default values are used.
Default config path is {DEFAULT_CONFIG_PATH}.
Default config values are:

\b
{yaml.dump(DEFAULT_CONFIG)}"""
PROVENANCE_HELP = f"""Software Heritage Scanner tools.

{CONFIG_FILE_HELP}"""


@swh_cli_group.group(
    name="provenance", context_settings=CONTEXT_SETTINGS, help=PROVENANCE_HELP,
)
@click.option(
    "-C",
    "--config-file",
    default=None,
    type=click.Path(exists=False, dir_okay=False, path_type=str),
    help="""YAML configuration file""",
)
@click.pass_context
def cli(ctx, config_file: Optional[str]):
    from .db_utils import adapt_conn

    if config_file is None and config.config_exists(DEFAULT_PATH):
        config_file = DEFAULT_PATH

    if config_file is None:
        conf = DEFAULT_CONFIG
    else:
        # read_raw_config do not fail on ENOENT
        if not config.config_exists(config_file):
            raise FileNotFoundError(config_file)
        conf = config.read_raw_config(config.config_basepath(config_file))
        conf = config.merge_configs(DEFAULT_CONFIG, conf)

    ctx.ensure_object(dict)
    ctx.obj["config"] = conf

    conn = db_utils.connect_to_conninfo(conf["db"])
    adapt_conn(conn)
    ctx.obj["conn"] = conn


@cli.command(name="create")
@click.option("--name", default='provenance')
@click.pass_context
def create(ctx, name):
    """Create new provenance database."""
    from .provenance import create_database
    from .db_utils import adapt_conn

    # Close default connection as it won't be used
    ctx.obj["conn"].close()

    # Connect to server without selecting a database
    conninfo = os.path.dirname(ctx.obj["config"]["db"])
    conn = db_utils.connect_to_conninfo(conninfo)
    adapt_conn(conn)

    create_database(conn, conninfo, name)


@cli.command(name="iter-revisions")
@click.argument("filename")
@click.option('-l', '--limit', type=int)
@click.option('-t', '--threads', type=int, default=1)
@click.pass_context
def iter_revisions(ctx, filename, limit, threads):
    """Iterate over provided list of revisions and add them to the provenance database."""
    from .provenance import FileRevisionIterator
    from .provenance import RevisionWorker

    conninfo = ctx.obj["config"]["db"]
    revisions = FileRevisionIterator(filename, limit=limit)
    storage = get_storage(**ctx.obj["config"]["storage"])
    workers = []

    for id in range(threads):
        worker = RevisionWorker(id, conninfo, storage, revisions)
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.join()


@cli.command(name="find-first")
@click.argument("swhid")
@click.pass_context
def find_first(ctx, swhid):
    """Find first occurrence of the requested blob."""
    from .provenance import content_find_first

    conn = ctx.obj["conn"]
    cursor = conn.cursor();
    row = content_find_first(cursor, hash_to_bytes(swhid))
    print(f'{hash_to_hex(row[0])}, {hash_to_hex(row[1])}, {row[2]}, {os.fsdecode(row[3])}')


@cli.command(name="find-all")
@click.argument("swhid")
@click.pass_context
def find_all(ctx, swhid):
    """Find all occurrences of the requested blob."""
    from .provenance import content_find_all

    conn = ctx.obj["conn"]
    cursor = conn.cursor();
    for row in content_find_all(cursor, hash_to_bytes(swhid)):
        print(f'{hash_to_hex(row[0])}, {hash_to_hex(row[1])}, {row[2]}, {os.fsdecode(row[3])}')
