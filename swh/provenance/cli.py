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
from swh.model.hashutil import hash_to_bytes, hash_to_hex

# All generic config code should reside in swh.core.config
CONFIG_ENVVAR = "SWH_CONFIG_FILE"
DEFAULT_CONFIG_PATH = os.path.join(click.get_app_dir("swh"), "global.yml")
DEFAULT_PATH = os.environ.get(CONFIG_ENVVAR, DEFAULT_CONFIG_PATH)

DEFAULT_CONFIG: Dict[str, Any] = {
    "archive": {
        "cls": "api",
        "storage": {
            "cls": "remote",
            "url": "http://uffizi.internal.softwareheritage.org:5002",
        }
        # "cls": "local",
        # "db": {
        #     "host": "db.internal.softwareheritage.org",
        #     "dbname": "softwareheritage",
        #     "user": "guest"
        # }
    },
    "provenance": {"cls": "local", "db": {"host": "localhost", "dbname": "provenance"}},
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
    name="provenance", context_settings=CONTEXT_SETTINGS, help=PROVENANCE_HELP
)
@click.option(
    "-C",
    "--config-file",
    default=None,
    type=click.Path(exists=False, dir_okay=False, path_type=str),
    help="""YAML configuration file.""",
)
@click.option(
    "-P",
    "--profile",
    default=None,
    type=click.Path(exists=False, dir_okay=False, path_type=str),
    help="""Enable profiling to specified file.""",
)
@click.pass_context
def cli(ctx, config_file: Optional[str], profile: str):
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
        import atexit
        import cProfile

        print("Profiling...")
        pr = cProfile.Profile()
        pr.enable()

        def exit():
            pr.disable()
            pr.dump_stats(profile)

        atexit.register(exit)


@cli.command(name="create", deprecated=True)
@click.option("--maintenance-db", default=None)
@click.option("--drop/--no-drop", "drop_db", default=False)
@click.pass_context
def create(ctx, maintenance_db, drop_db):
    """Deprecated, please use:
      swh db create provenance
    and
      swh db init provenance
    instead.
    """


@cli.command(name="iter-revisions")
@click.argument("filename")
@click.option("-l", "--limit", type=int)
@click.pass_context
def iter_revisions(ctx, filename, limit):
    # TODO: add file size filtering
    """Process a provided list of revisions."""
    from . import get_archive, get_provenance
    from .provenance import revision_add
    from .revision import CSVRevisionIterator

    archive = get_archive(**ctx.obj["config"]["archive"])
    provenance = get_provenance(**ctx.obj["config"]["provenance"])
    revisions_provider = (
        line.strip().split(",") for line in open(filename, "r") if line.strip()
    )
    revisions = CSVRevisionIterator(revisions_provider, archive, limit=limit)

    for revision in revisions:
        revision_add(provenance, archive, revision)


@cli.command(name="iter-origins")
@click.argument("filename")
@click.option("-l", "--limit", type=int)
@click.pass_context
def iter_origins(ctx, filename, limit):
    """Process a provided list of origins."""
    from . import get_archive, get_provenance
    from .origin import FileOriginIterator
    from .provenance import origin_add

    archive = get_archive(**ctx.obj["config"]["archive"])
    provenance = get_provenance(**ctx.obj["config"]["provenance"])

    for origin in FileOriginIterator(filename, archive, limit=limit):
        origin_add(provenance, origin)


@cli.command(name="find-first")
@click.argument("swhid")
@click.pass_context
def find_first(ctx, swhid):
    """Find first occurrence of the requested blob."""
    from . import get_provenance

    provenance = get_provenance(**ctx.obj["config"]["provenance"])
    # TODO: return a dictionary with proper keys for each field
    row = provenance.content_find_first(hash_to_bytes(swhid))
    if row is not None:
        print(
            "{blob}, {rev}, {date}, {path}".format(
                blob=hash_to_hex(row[0]),
                rev=hash_to_hex(row[1]),
                date=row[2],
                path=os.fsdecode(row[3]),
            )
        )
    else:
        print(f"Cannot find a content with the id {swhid}")


@cli.command(name="find-all")
@click.argument("swhid")
@click.pass_context
def find_all(ctx, swhid):
    """Find all occurrences of the requested blob."""
    from swh.provenance import get_provenance

    provenance = get_provenance(**ctx.obj["config"]["provenance"])
    # TODO: return a dictionary with proper keys for each field
    for row in provenance.content_find_all(hash_to_bytes(swhid)):
        print(
            "{blob}, {rev}, {date}, {path}".format(
                blob=hash_to_hex(row[0]),
                rev=hash_to_hex(row[1]),
                date=row[2],
                path=os.fsdecode(row[3]),
            )
        )
