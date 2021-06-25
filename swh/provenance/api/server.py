# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os

from swh.core import config
from swh.core.api import JSONFormatter, MsgpackFormatter, RPCServerApp, negotiate
from swh.provenance import get_provenance_storage
from swh.provenance.interface import ProvenanceStorageInterface

from .serializers import DECODERS, ENCODERS

storage = None


def get_global_provenance_storage():
    global storage
    if not storage:
        storage = get_provenance_storage(**app.config["provenance"]["storage"])
    return storage


class ProvenanceStorageServerApp(RPCServerApp):
    extra_type_decoders = DECODERS
    extra_type_encoders = ENCODERS


app = ProvenanceStorageServerApp(
    __name__,
    backend_class=ProvenanceStorageInterface,
    backend_factory=get_global_provenance_storage,
)


def has_no_empty_params(rule):
    return len(rule.defaults or ()) >= len(rule.arguments or ())


@app.route("/")
def index():
    return """<html>
<head><title>Software Heritage provenance storage RPC server</title></head>
<body>
<p>You have reached the
<a href="https://www.softwareheritage.org/">Software Heritage</a>
provenance storage RPC server.<br />
See its
<a href="https://docs.softwareheritage.org/devel/swh-provenance/">documentation
and API</a> for more information</p>
</body>
</html>"""


@app.route("/site-map")
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def site_map():
    links = []
    for rule in app.url_map.iter_rules():
        if has_no_empty_params(rule) and hasattr(
            ProvenanceStorageInterface, rule.endpoint
        ):
            links.append(
                dict(
                    rule=rule.rule,
                    description=getattr(
                        ProvenanceStorageInterface, rule.endpoint
                    ).__doc__,
                )
            )
    # links is now a list of url, endpoint tuples
    return links


def load_and_check_config(config_path, type="local"):
    """Check the minimal configuration is set to run the api or raise an
       error explanation.

    Args:
        config_path (str): Path to the configuration file to load
        type (str): configuration type. For 'local' type, more
                    checks are done.

    Raises:
        Error if the setup is not as expected

    Returns:
        configuration as a dict

    """
    if not config_path:
        raise EnvironmentError("Configuration file must be defined")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file {config_path} does not exist")

    cfg = config.read(config_path)

    pcfg = cfg.get("provenance")
    if not pcfg:
        raise KeyError("Missing 'provenance' configuration")

    scfg = pcfg.get("storage")
    if not scfg:
        raise KeyError("Missing 'provenance.storage' configuration")

    if type == "local":
        cls = scfg.get("cls")
        if cls != "local":
            raise ValueError(
                "The provenance backend can only be started with a 'local' "
                "configuration"
            )

        db = scfg.get("db")
        if not db:
            raise KeyError("Invalid configuration; missing 'db' config entry")

    return cfg


api_cfg = None


def make_app_from_configfile():
    """Run the WSGI app from the webserver, loading the configuration from
    a configuration file.

    SWH_CONFIG_FILENAME environment variable defines the
    configuration path to load.

    """
    global api_cfg
    if not api_cfg:
        config_path = os.environ.get("SWH_CONFIG_FILENAME")
        api_cfg = load_and_check_config(config_path)
        app.config.update(api_cfg)
    handler = logging.StreamHandler()
    app.logger.addHandler(handler)
    return app