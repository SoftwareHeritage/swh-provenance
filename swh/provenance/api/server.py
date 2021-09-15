# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Any, Dict, Optional

from swh.core import config


def load_and_check_config(
    config_path: Optional[str], type: str = "local"
) -> Dict[str, Any]:
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
    if config_path is None:
        raise EnvironmentError("Configuration file must be defined")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file {config_path} does not exist")

    cfg = config.read(config_path)

    pcfg: Optional[Dict[str, Any]] = cfg.get("provenance")
    if pcfg is None:
        raise KeyError("Missing 'provenance' configuration")

    scfg: Optional[Dict[str, Any]] = pcfg.get("storage")
    if scfg is None:
        raise KeyError("Missing 'provenance.storage' configuration")

    if type == "local":
        cls = scfg.get("cls")
        if cls != "postgresql":
            raise ValueError(
                "The provenance backend can only be started with a 'postgresql' "
                "configuration"
            )

        db = scfg.get("db")
        if not db:
            raise KeyError("Invalid configuration; missing 'db' config entry")

    return cfg
