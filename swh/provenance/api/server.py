# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
from typing import Any, Dict

from swh.core import config
from swh.core.api import RPCServerApp
from swh.core.api import encode_data_server as encode_data
from swh.core.api import error_handler

from swh.provenance import get_provenance
from swh.provenance.api.serializers import DECODERS, ENCODERS
from swh.provenance.exc import ProvenanceException
from swh.provenance.interface import ProvenanceInterface

logger = logging.getLogger(__name__)


def _get_provenance():
    global provenance
    if not provenance:
        provenance = get_provenance(**app.config["provenance"])
    return provenance


class ProvenanceServerApp(RPCServerApp):
    extra_type_decoders = DECODERS
    extra_type_encoders = ENCODERS


app = ProvenanceServerApp(
    __name__, backend_class=ProvenanceInterface, backend_factory=_get_provenance
)

provenance = None


@app.errorhandler(ProvenanceException)
def search_error_handler(exception):
    return error_handler(exception, encode_data, status_code=400)


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


@app.route("/")
def index():
    return "SWH Provenance API server"


api_cfg = None


def load_and_check_config(config_file: str) -> Dict[str, Any]:
    """Check the minimal configuration is set to run the api or raise an
       error explanation.

    Args:
        config_file: Path to the configuration file to load
        type: configuration type. For 'local' type, more
                    checks are done.

    Raises:
        Error if the setup is not as expected

    Returns:
        configuration as a dict

    """
    if not config_file:
        raise EnvironmentError("Configuration file must be defined")

    if not os.path.exists(config_file):
        raise FileNotFoundError("Configuration file %s does not exist" % (config_file,))

    cfg = config.read(config_file)
    if "provenance" not in cfg:
        raise KeyError("Missing 'provenance' configuration")

    return cfg


def make_app_from_configfile():
    """Run the WSGI app from the webserver, loading the configuration from
    a configuration file.

    SWH_CONFIG_FILENAME environment variable defines the
    configuration path to load.

    """
    global api_cfg
    if not api_cfg:
        config_file = os.environ.get("SWH_CONFIG_FILENAME")
        api_cfg = load_and_check_config(config_file)
        app.config.update(api_cfg)
    handler = logging.StreamHandler()
    app.logger.addHandler(handler)
    return app
