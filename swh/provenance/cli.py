# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from datetime import datetime, timezone
from functools import partial
import os
from typing import Any, Dict, Generator, Optional, Tuple

import click
from deprecated import deprecated
import iso8601
import yaml

try:
    from systemd.daemon import notify
except ImportError:
    notify = None

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
            "db": (
                "host=belvedere.internal.softwareheritage.org port=5432 "
                "dbname=softwareheritage user=guest"
            ),
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
            # Remote RabbitMQ/PostgreSQL Storage
            "cls": "rabbitmq",
            "url": "amqp://localhost:5672/%2f",
            "storage_config": {
                "cls": "postgresql",
                "db": "host=localhost user=postgres password=postgres dbname=provenance",
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


@cli.command(name="replay")
@click.option(
    "--stop-after-objects",
    "-n",
    default=None,
    type=int,
    help="Stop after processing this many objects. Default is to " "run forever.",
)
@click.option(
    "--type",
    "-t",
    "object_types",
    default=[],
    type=click.Choice(
        [
            "content",
            "directory",
            "revision",
            "location",
            "content_in_revision",
            "content_in_directory",
            "directory_in_revision",
        ]
    ),
    help="Object types to replay",
    multiple=True,
)
@click.pass_context
def replay(ctx: click.core.Context, stop_after_objects, object_types):
    """Fill a ProvenanceStorage by reading a Journal.

    This is typically used to replicate a Provenance database, reading the
    Software Heritage kafka journal to retrieve objects of the Software
    Heritage provenance storage to feed a replicate provenance storage. There
    can be several 'replayers' filling a ProvenanceStorage as long as they use
    the same `group-id`.

    The expected configuration file should have one 'provenance' section with 2
    subsections:

    - storage: the configuration of the provenance storage in which to add
      objects received from the kafka journal,

    - journal_client: the configuration of access to the kafka journal. See the
      documentation of `swh.journal` for more details on the possible
      configuration entries in this section.

      https://docs.softwareheritage.org/devel/apidoc/swh.journal.client.html

    eg.::

      provenance:
        storage:
          cls: postgresql
          db: [...]
        journal_client:
          cls: kafka
          prefix: swh.journal.provenance
          brokers: [...]
          [...]
    """
    import functools

    from swh.journal.client import get_journal_client
    from swh.provenance.storage import get_provenance_storage
    from swh.provenance.storage.replay import (
        ProvenanceObjectDeserializer,
        process_replay_objects,
    )

    conf = ctx.obj["config"]["provenance"]
    storage = get_provenance_storage(**conf.pop("storage"))

    client_cfg = conf.pop("journal_client")

    deserializer = ProvenanceObjectDeserializer()

    client_cfg["value_deserializer"] = deserializer.convert
    if object_types:
        client_cfg["object_types"] = object_types
    if stop_after_objects:
        client_cfg["stop_after_objects"] = stop_after_objects

    try:
        client = get_journal_client(**client_cfg)
    except ValueError as exc:
        ctx.fail(str(exc))

    worker_fn = functools.partial(process_replay_objects, storage=storage)

    if notify:
        notify("READY=1")

    try:
        with storage:
            n = client.process(worker_fn)
    except KeyboardInterrupt:
        ctx.exit(0)
    else:
        print(f"Done, processed {n} messages")
    finally:
        if notify:
            notify("STOPPING=1")
        client.close()


@cli.group(name="origin")
@click.pass_context
def origin(ctx: click.core.Context):
    from . import get_provenance
    from .archive import get_archive

    archive = get_archive(**ctx.obj["config"]["provenance"]["archive"])
    provenance = get_provenance(**ctx.obj["config"]["provenance"]["storage"])

    ctx.obj["provenance"] = provenance
    ctx.obj["archive"] = archive


@origin.command(name="from-csv")
@click.argument("filename", type=click.Path(exists=True))
@click.option(
    "-l",
    "--limit",
    type=int,
    help="""Limit the amount of entries (origins) to read from the input file.""",
)
@click.pass_context
def origin_from_csv(ctx: click.core.Context, filename: str, limit: Optional[int]):
    from swh.provenance.algos.origin import CSVOriginIterator, origin_add

    provenance = ctx.obj["provenance"]
    archive = ctx.obj["archive"]

    origins_provider = generate_origin_tuples(filename)
    origins = CSVOriginIterator(origins_provider, limit=limit)

    with provenance:
        for origin in origins:
            origin_add(provenance, archive, [origin])


@origin.command(name="from-journal")
@click.pass_context
def origin_from_journal(ctx: click.core.Context):
    from swh.journal.client import get_journal_client

    from .journal_client import process_journal_origins

    provenance = ctx.obj["provenance"]
    archive = ctx.obj["archive"]

    journal_cfg = ctx.obj["config"].get("journal_client", {})

    worker_fn = partial(
        process_journal_origins,
        archive=archive,
        provenance=provenance,
    )

    cls = journal_cfg.pop("cls", None) or "kafka"
    client = get_journal_client(
        cls,
        **{
            **journal_cfg,
            "object_types": ["origin_visit_status"],
        },
    )

    if notify:
        notify("READY=1")

    try:
        with provenance:
            client.process(worker_fn)
    except KeyboardInterrupt:
        ctx.exit(0)
    else:
        print("Done.")
    finally:
        if notify:
            notify("STOPPING=1")
        client.close()


@cli.group(name="revision")
@click.pass_context
def revision(ctx: click.core.Context):
    from . import get_provenance
    from .archive import get_archive

    archive = get_archive(**ctx.obj["config"]["provenance"]["archive"])
    provenance = get_provenance(**ctx.obj["config"]["provenance"]["storage"])

    ctx.obj["provenance"] = provenance
    ctx.obj["archive"] = archive


@revision.command(name="from-csv")
@click.argument("filename", type=click.Path(exists=True))
@click.option(
    "-a",
    "--track-all",
    default=True,
    type=bool,
    help="""Index all occurrences of files in the development history.""",
)
@click.option(
    "-f",
    "--flatten",
    default=True,
    type=bool,
    help="""Create flat models for directories in the isochrone frontier.""",
)
@click.option(
    "-l",
    "--limit",
    type=int,
    help="""Limit the amount of entries (revisions) to read from the input file.""",
)
@click.option(
    "-m",
    "--min-depth",
    default=1,
    type=int,
    help="""Set minimum depth (in the directory tree) at which an isochrone """
    """frontier can be defined.""",
)
@click.option(
    "-r",
    "--reuse",
    default=True,
    type=bool,
    help="""Prioritize the usage of previously defined isochrone frontiers """
    """whenever possible.""",
)
@click.option(
    "-s",
    "--min-size",
    default=0,
    type=int,
    help="""Set the minimum size (in bytes) of files to be indexed. """
    """Any smaller file will be ignored.""",
)
@click.option(
    "-d",
    "--max-directory-size",
    default=0,
    type=int,
    help="""Set the maximum recursive directory size of revisions to be indexed.""",
)
@click.pass_context
def revision_from_csv(
    ctx: click.core.Context,
    filename: str,
    track_all: bool,
    flatten: bool,
    limit: Optional[int],
    min_depth: int,
    reuse: bool,
    min_size: int,
    max_directory_size: int,
) -> None:
    from swh.provenance.algos.revision import CSVRevisionIterator, revision_add

    provenance = ctx.obj["provenance"]
    archive = ctx.obj["archive"]

    revisions_provider = generate_revision_tuples(filename)
    revisions = CSVRevisionIterator(revisions_provider, limit=limit)

    with provenance:
        for revision in revisions:
            revision_add(
                provenance,
                archive,
                [revision],
                trackall=track_all,
                flatten=flatten,
                lower=reuse,
                mindepth=min_depth,
                minsize=min_size,
                max_directory_size=max_directory_size,
            )


@revision.command(name="from-journal")
@click.option(
    "-a",
    "--track-all",
    default=True,
    type=bool,
    help="""Index all occurrences of files in the development history.""",
)
@click.option(
    "-f",
    "--flatten",
    default=True,
    type=bool,
    help="""Create flat models for directories in the isochrone frontier.""",
)
@click.option(
    "-l",
    "--limit",
    type=int,
    help="""Limit the amount of entries (revisions) to read from the input file.""",
)
@click.option(
    "-m",
    "--min-depth",
    default=1,
    type=int,
    help="""Set minimum depth (in the directory tree) at which an isochrone """
    """frontier can be defined.""",
)
@click.option(
    "-r",
    "--reuse",
    default=True,
    type=bool,
    help="""Prioritize the usage of previously defined isochrone frontiers """
    """whenever possible.""",
)
@click.option(
    "-s",
    "--min-size",
    default=0,
    type=int,
    help="""Set the minimum size (in bytes) of files to be indexed. """
    """Any smaller file will be ignored.""",
)
@click.option(
    "-d",
    "--max-directory-size",
    default=0,
    type=int,
    help="""Set the maximum recursive directory size of revisions to be indexed.""",
)
@click.pass_context
def revision_from_journal(
    ctx: click.core.Context,
    track_all: bool,
    flatten: bool,
    limit: Optional[int],
    min_depth: int,
    reuse: bool,
    min_size: int,
    max_directory_size: int,
) -> None:
    from swh.journal.client import get_journal_client

    from .journal_client import process_journal_revisions

    provenance = ctx.obj["provenance"]
    archive = ctx.obj["archive"]

    journal_cfg = ctx.obj["config"].get("journal_client", {})

    worker_fn = partial(
        process_journal_revisions,
        archive=archive,
        provenance=provenance,
        minsize=min_size,
        max_directory_size=max_directory_size,
    )

    cls = journal_cfg.pop("cls", None) or "kafka"
    client = get_journal_client(
        cls,
        **{
            **journal_cfg,
            "object_types": ["revision"],
        },
    )

    if notify:
        notify("READY=1")

    try:
        with provenance:
            client.process(worker_fn)
    except KeyboardInterrupt:
        ctx.exit(0)
    else:
        print("Done.")
    finally:
        if notify:
            notify("STOPPING=1")
        client.close()


@cli.group(name="directory")
@click.pass_context
def directory(ctx: click.core.Context):
    from . import get_provenance
    from .archive import get_archive

    archive = get_archive(**ctx.obj["config"]["provenance"]["archive"])
    provenance = get_provenance(**ctx.obj["config"]["provenance"]["storage"])

    ctx.obj["provenance"] = provenance
    ctx.obj["archive"] = archive


@directory.command(name="flatten")
@click.option(
    "--range-from", type=str, help="start ID of the range of directories to flatten"
)
@click.option(
    "--range-to", type=str, help="stop ID of the range of directories to flatten"
)
@click.option(
    "-s",
    "--min-size",
    default=0,
    type=int,
    help="""Set the minimum size (in bytes) of files to be indexed.
    Any smaller file will be ignored.""",
)
@click.pass_context
def directory_flatten(ctx: click.core.Context, range_from, range_to, min_size):
    from swh.provenance.algos.directory import directory_flatten_range

    provenance = ctx.obj["provenance"]
    archive = ctx.obj["archive"]

    directory_flatten_range(
        provenance,
        archive,
        hash_to_bytes(range_from),
        hash_to_bytes(range_to),
        min_size,
    )


# old (deprecated) commands
@cli.command(name="iter-frontiers")
@click.argument("filename")
@click.option(
    "-l",
    "--limit",
    type=int,
    help="""Limit the amount of entries (directories) to read from the input file.""",
)
@click.option(
    "-s",
    "--min-size",
    default=0,
    type=int,
    help="""Set the minimum size (in bytes) of files to be indexed. """
    """Any smaller file will be ignored.""",
)
@click.pass_context
def iter_frontiers(
    ctx: click.core.Context,
    filename: str,
    limit: Optional[int],
    min_size: int,
) -> None:
    """Process a provided list of directories in the isochrone frontier."""
    from swh.provenance import get_provenance
    from swh.provenance.algos.directory import CSVDirectoryIterator, directory_add
    from swh.provenance.archive import get_archive

    archive = get_archive(**ctx.obj["config"]["provenance"]["archive"])
    directories_provider = generate_directory_ids(filename)
    directories = CSVDirectoryIterator(directories_provider, limit=limit)

    with get_provenance(**ctx.obj["config"]["provenance"]["storage"]) as provenance:
        for directory in directories:
            directory_add(
                provenance,
                archive,
                [directory],
                minsize=min_size,
            )


def generate_directory_ids(
    filename: str,
) -> Generator[Sha1Git, None, None]:
    for line in open(filename, "r"):
        if line.strip():
            yield hash_to_bytes(line.strip())


@cli.command(name="iter-revisions")
@click.argument("filename")
@click.option(
    "-a",
    "--track-all",
    default=True,
    type=bool,
    help="""Index all occurrences of files in the development history.""",
)
@click.option(
    "-f",
    "--flatten",
    default=True,
    type=bool,
    help="""Create flat models for directories in the isochrone frontier.""",
)
@click.option(
    "-l",
    "--limit",
    type=int,
    help="""Limit the amount of entries (revisions) to read from the input file.""",
)
@click.option(
    "-m",
    "--min-depth",
    default=1,
    type=int,
    help="""Set minimum depth (in the directory tree) at which an isochrone """
    """frontier can be defined.""",
)
@click.option(
    "-r",
    "--reuse",
    default=True,
    type=bool,
    help="""Prioritize the usage of previously defined isochrone frontiers """
    """whenever possible.""",
)
@click.option(
    "-s",
    "--min-size",
    default=0,
    type=int,
    help="""Set the minimum size (in bytes) of files to be indexed. """
    """Any smaller file will be ignored.""",
)
@click.pass_context
def iter_revisions(
    ctx: click.core.Context,
    filename: str,
    track_all: bool,
    flatten: bool,
    limit: Optional[int],
    min_depth: int,
    reuse: bool,
    min_size: int,
) -> None:
    """Process a provided list of revisions."""
    from swh.provenance import get_provenance
    from swh.provenance.algos.revision import CSVRevisionIterator, revision_add
    from swh.provenance.archive import get_archive

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
                flatten=flatten,
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
@click.option(
    "-l",
    "--limit",
    type=int,
    help="""Limit the amount of entries (origins) to read from the input file.""",
)
@click.pass_context
@deprecated(version="0.0.1", reason="Use `swh provenance origin from-csv` instead")
def iter_origins(ctx: click.core.Context, filename: str, limit: Optional[int]) -> None:
    """Process a provided list of origins."""
    from swh.provenance import get_provenance
    from swh.provenance.algos.origin import CSVOriginIterator, origin_add
    from swh.provenance.archive import get_archive

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
@click.option(
    "-l", "--limit", type=int, help="""Limit the amount results to be retrieved."""
)
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
