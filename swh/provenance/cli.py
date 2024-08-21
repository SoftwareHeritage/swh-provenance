# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import logging

import click
from swh.core.cli import CONTEXT_SETTINGS, AliasedGroup
from swh.core.cli import swh as swh_cli_group


@swh_cli_group.group(
    name="provenance", context_settings=CONTEXT_SETTINGS, cls=AliasedGroup
)
@click.option(
    "--config-file",
    "-C",
    default=None,
    metavar="CONFIGFILE",
    type=click.Path(
        exists=True,
        dir_okay=False,
    ),
    help="Configuration file.",
)
@click.pass_context
def provenance_cli_group(ctx, config_file):
    """Software Heritage Provenance tools."""
    from swh.core import config

    ctx.ensure_object(dict)
    conf = config.read(config_file)
    ctx.obj["config"] = conf


@provenance_cli_group.command(name="rpc-serve")
@click.option(
    "--host",
    default="0.0.0.0",
    metavar="IP",
    show_default=True,
    help="Host ip address to bind the server on",
)
@click.option(
    "--port",
    default=5011,
    type=click.INT,
    metavar="PORT",
    show_default=True,
    help="Binding port of the server",
)
@click.option(
    "--debug/--no-debug",
    default=True,
    help="Indicates if the server should run in debug mode",
)
@click.pass_context
def rpc_serve(ctx, host, port, debug):
    """Software Heritage Provenance RPC server.

    Do NOT use this in a production environment.
    """
    from swh.provenance.api.server import app

    if "log_level" in ctx.obj:
        logging.getLogger("werkzeug").setLevel(ctx.obj["log_level"])
    app.config.update(ctx.obj["config"])
    app.run(host, port=int(port), debug=bool(debug))


def main():
    logging.basicConfig()
    return rpc_serve(auto_envvar_prefix="SWH_VAULT")


if __name__ == "__main__":
    main()
