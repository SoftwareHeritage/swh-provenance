# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import os
from typing import Any, Dict, Optional

import click
import yaml

from swh.core import config
from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group
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
    "db": {
        "host": "localhost",
        "database": "provenance",
        "user": "postgres",
        "password": "postgres"
    }
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
@click.option("--profile", is_flag=True)
@click.pass_context
def cli(ctx, config_file: Optional[str], profile: bool):
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

    if profile:
        import cProfile
        import pstats
        import io
        import atexit

        print("Profiling...")
        pr = cProfile.Profile()
        pr.enable()

        def exit():
            pr.disable()
            print("Profiling completed")
            s = io.StringIO()
            pstats.Stats(pr, stream=s).sort_stats("cumulative").print_stats()
            print(s.getvalue())

        atexit.register(exit)


@cli.command(name="create")
@click.option("--name", default=None)
@click.pass_context
def create(ctx, name):
    """Create new provenance database."""
    from .db_utils import connect
    from .provenance import create_database

    # Connect to server without selecting a database
    conninfo = ctx.obj["config"]["db"]
    database = conninfo.pop('database', None)
    conn = connect(conninfo)

    if name is None:
        name = database

    create_database(conn, conninfo, name)


@cli.command(name="iter-revisions")
@click.argument("filename")
@click.option('-l', '--limit', type=int)
@click.option('-t', '--threads', type=int, default=1)
@click.pass_context
def iter_revisions(ctx, filename, limit, threads):
    """Iterate over provided list of revisions and add them to the provenance database."""
    from .revision import FileRevisionIterator
    from .revision import RevisionWorker

    conninfo = ctx.obj["config"]["db"]
    storage = get_storage(**ctx.obj["config"]["storage"])
    revisions = FileRevisionIterator(filename, storage, limit=limit)
    workers = []

    for id in range(threads):
        worker = RevisionWorker(id, conninfo, storage, revisions)
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.join()


@cli.command(name="iter-origins")
@click.argument("filename")
@click.option('-l', '--limit', type=int)
#@click.option('-t', '--threads', type=int, default=1)
@click.pass_context
#def iter_revisions(ctx, filename, limit, threads):
def iter_origins(ctx, filename, limit):
    """Iterate over provided list of revisions and add them to the provenance database."""
    from .db_utils import connect
    from .origin import FileOriginIterator
    from .provenance import origin_add

    conn = connect(ctx.obj["config"]["db"])
    storage = get_storage(**ctx.obj["config"]["storage"])

    for origin in FileOriginIterator(filename, storage, limit=limit):
        # TODO: consider using threads and a OriginWorker class
        origin_add(conn, storage, origin)


@cli.command(name="find-first")
@click.argument("swhid")
@click.pass_context
def find_first(ctx, swhid):
    """Find first occurrence of the requested blob."""
    from .db_utils import connect
    from .provenance import content_find_first

    with connect(ctx.obj["config"]["db"]).cursor() as cursor:
        # TODO: return a dictionary with proper keys for each field
        row = content_find_first(cursor, hash_to_bytes(swhid))
        if row is not None:
            print(f'{hash_to_hex(row[0])}, {hash_to_hex(row[1])}, {row[2]}, {os.fsdecode(row[3])}')
        else:
            print(f'Cannot find a content with the id {swhid}')


@cli.command(name="find-all")
@click.argument("swhid")
@click.pass_context
def find_all(ctx, swhid):
    """Find all occurrences of the requested blob."""
    from .db_utils import connect
    from .provenance import content_find_all

    with connect(ctx.obj["config"]["db"]).cursor() as cursor:
        # TODO: return a dictionary with proper keys for each field
        for row in content_find_all(cursor, hash_to_bytes(swhid)):
            print(f'{hash_to_hex(row[0])}, {hash_to_hex(row[1])}, {row[2]}, {os.fsdecode(row[3])}')
