# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from datetime import datetime, timezone
import os
from typing import Any, Dict, Generator, Optional, Tuple

import click
import iso8601
import yaml

from swh.core import config
from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group
from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.model.model import Sha1Git

# All generic config code should reside in swh.core.config
CONFIG_ENVVAR = "SWH_CONFIG_FILENAME"
DEFAULT_PATH = os.environ.get(CONFIG_ENVVAR, None)

DEFAULT_CONFIG: Dict[str, Any] = {
    "provenance": {
        "archive": {
            # Storage API based Archive object
            # "cls": "api",
            # "storage": {
            #     "cls": "remote",
            #     "url": "http://uffizi.internal.softwareheritage.org:5002",
            # }
            # Direct access Archive object
            "cls": "direct",
            "db": {
                "host": "belvedere.internal.softwareheritage.org",
                "port": 5432,
                "dbname": "softwareheritage",
                "user": "guest",
            },
        },
        "storage": {
            # Local PostgreSQL Storage
            # "cls": "postgresql",
            # "db": {
            #     "host": "localhost",
            #     "user": "postgres",
            #     "password": "postgres",
            #     "dbname": "provenance",
            # },
            # Local MongoDB Storage
            # "cls": "mongodb",
            # "db": {
            #     "dbname": "provenance",
            # },
            # Remote RabbitMQ/PostgreSQL Storage
            "cls": "rabbitmq",
            "url": "amqp://localhost:5672/%2f",
            "storage_config": {
                "cls": "postgresql",
                "db": {
                    "host": "localhost",
                    "user": "postgres",
                    "password": "postgres",
                    "dbname": "provenance",
                },
            },
            "batch_size": 100,
            "prefetch_count": 100,
        },
    }
}


CONFIG_FILE_HELP = f"""
\b Configuration can be loaded from a yaml file given either as --config-file
option or the {CONFIG_ENVVAR} environment variable. If no configuration file
is specified, use the following default configuration::

\b
{yaml.dump(DEFAULT_CONFIG)}"""
PROVENANCE_HELP = f"""Software Heritage provenance index database tools

{CONFIG_FILE_HELP}
"""


@swh_cli_group.group(
    name="provenance", context_settings=CONTEXT_SETTINGS, help=PROVENANCE_HELP
)
@click.option(
    "-C",
    "--config-file",
    default=None,
    type=click.Path(exists=True, dir_okay=False, path_type=str),
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
def cli(ctx: click.core.Context, config_file: Optional[str], profile: str) -> None:
    if (
        config_file is None
        and DEFAULT_PATH is not None
        and config.config_exists(DEFAULT_PATH)
    ):
        config_file = DEFAULT_PATH

    if config_file is None:
        conf = DEFAULT_CONFIG
    else:
        # read_raw_config do not fail on ENOENT
        if not os.path.exists(config_file):
            raise FileNotFoundError(config_file)
        conf = yaml.safe_load(open(config_file, "rb"))

    ctx.ensure_object(dict)
    ctx.obj["config"] = conf

    if profile:
        import atexit
        import cProfile

        print("Profiling...")
        pr = cProfile.Profile()
        pr.enable()

        def exit() -> None:
            pr.disable()
            pr.dump_stats(profile)

        atexit.register(exit)


@cli.command(name="iter-revisions")
@click.argument("filename")
@click.option("-a", "--track-all", default=True, type=bool)
@click.option("-l", "--limit", type=int)
@click.option("-m", "--min-depth", default=1, type=int)
@click.option("-r", "--reuse", default=True, type=bool)
@click.option("-s", "--min-size", default=0, type=int)
@click.pass_context
def iter_revisions(
    ctx: click.core.Context,
    filename: str,
    track_all: bool,
    limit: Optional[int],
    min_depth: int,
    reuse: bool,
    min_size: int,
) -> None:
    """Process a provided list of revisions."""
    from . import get_archive, get_provenance
    from .revision import CSVRevisionIterator, revision_add

    archive = get_archive(**ctx.obj["config"]["provenance"]["archive"])
    revisions_provider = generate_revision_tuples(filename)
    revisions = CSVRevisionIterator(revisions_provider, limit=limit)

    with get_provenance(**ctx.obj["config"]["provenance"]["storage"]) as provenance:
        for revision in revisions:
            revision_add(
                provenance,
                archive,
                [revision],
                trackall=track_all,
                lower=reuse,
                mindepth=min_depth,
                minsize=min_size,
            )


def generate_revision_tuples(
    filename: str,
) -> Generator[Tuple[Sha1Git, datetime, Sha1Git], None, None]:
    for line in open(filename, "r"):
        if line.strip():
            revision, date, root = line.strip().split(",")
            yield (
                hash_to_bytes(revision),
                iso8601.parse_date(date, default_timezone=timezone.utc),
                hash_to_bytes(root),
            )


@cli.command(name="iter-origins")
@click.argument("filename")
@click.option("-l", "--limit", type=int)
@click.pass_context
def iter_origins(ctx: click.core.Context, filename: str, limit: Optional[int]) -> None:
    """Process a provided list of origins."""
    from . import get_archive, get_provenance
    from .origin import CSVOriginIterator, origin_add

    archive = get_archive(**ctx.obj["config"]["provenance"]["archive"])
    origins_provider = generate_origin_tuples(filename)
    origins = CSVOriginIterator(origins_provider, limit=limit)

    with get_provenance(**ctx.obj["config"]["provenance"]["storage"]) as provenance:
        for origin in origins:
            origin_add(provenance, archive, [origin])


def generate_origin_tuples(filename: str) -> Generator[Tuple[str, bytes], None, None]:
    for line in open(filename, "r"):
        if line.strip():
            url, snapshot = line.strip().split(",")
            yield (url, hash_to_bytes(snapshot))


@cli.command(name="find-first")
@click.argument("swhid")
@click.pass_context
def find_first(ctx: click.core.Context, swhid: str) -> None:
    """Find first occurrence of the requested blob."""
    from . import get_provenance

    with get_provenance(**ctx.obj["config"]["provenance"]["storage"]) as provenance:
        occur = provenance.content_find_first(hash_to_bytes(swhid))
        if occur is not None:
            print(
                f"swh:1:cnt:{hash_to_hex(occur.content)}, "
                f"swh:1:rev:{hash_to_hex(occur.revision)}, "
                f"{occur.date}, "
                f"{occur.origin}, "
                f"{os.fsdecode(occur.path)}"
            )
        else:
            print(f"Cannot find a content with the id {swhid}")


@cli.command(name="find-all")
@click.argument("swhid")
@click.option("-l", "--limit", type=int)
@click.pass_context
def find_all(ctx: click.core.Context, swhid: str, limit: Optional[int]) -> None:
    """Find all occurrences of the requested blob."""
    from . import get_provenance

    with get_provenance(**ctx.obj["config"]["provenance"]["storage"]) as provenance:
        for occur in provenance.content_find_all(hash_to_bytes(swhid), limit=limit):
            print(
                f"swh:1:cnt:{hash_to_hex(occur.content)}, "
                f"swh:1:rev:{hash_to_hex(occur.revision)}, "
                f"{occur.date}, "
                f"{occur.origin}, "
                f"{os.fsdecode(occur.path)}"
            )
