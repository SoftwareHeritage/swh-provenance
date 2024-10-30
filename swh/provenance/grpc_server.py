# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import shlex
import shutil
import subprocess
from pathlib import Path
import sys

import aiohttp.test_utils
import aiohttp.web

logger = logging.getLogger(__name__)


class ExecutableNotFound(EnvironmentError):
    pass


def default_rust_executable_dir(config):
    debug_mode = config.get("debug", "pytest" in sys.modules)

    # look for a target/ directory in the sources root directory
    profile = "debug" if debug_mode else "release"
    # in editable installs, __file__ is a symlink to the original file in
    # the source directory, which is where in the end the rust sources and
    # executable are. So resolve the symlink before looking for the target/
    # directory relative to the actual python file.
    path = Path(__file__).resolve()
    path = path.parent.parent.parent / "target" / profile
    return path


def build_rust_grpc_server_cmdline(**config):
    logger.debug("Checking configuration and populating default values")

    port = config.pop("port", None)
    if port is None:
        port = aiohttp.test_utils.unused_port()
        logger.debug("Port not configured, using random port %s", port)

    rust_executable_dir = config.get("rust_executable_dir") or default_rust_executable_dir(config)
    print(rust_executable_dir)
    grpc_path = str(rust_executable_dir) + "/swh-provenance-grpc-serve"
    if not os.path.isfile(grpc_path):
        grpc_path = shutil.which("swh-provenance-grpc-serve")
    if not grpc_path or not os.path.isfile(grpc_path):
        raise ExecutableNotFound("swh-provenance-grpc-serve executable not found")

    cmd = [str(grpc_path)]
    logger.debug("Configuration: %r", config)
    cmd.extend(["--bind", f"[::]:{port}"])
    cmd.extend(["--database", str(config["db"])])
    cmd.extend(["--graph", str(config["graph"])])
    if "indexes" in config:
        cmd.extend(["--indexes", str(config["indexes"])])
    if config.get("graph_format"):
        cmd.extend(["--graph-format", config["graph_format"]])
    print(f"Started GRPC using dataset from {grpc_path}")
    return cmd, port


def spawn_rust_grpc_server(**config):
    cmd, port = build_rust_grpc_server_cmdline(**config)
    print(cmd)
    # XXX: shlex.join() is in 3.8
    # logger.info("Starting gRPC server: %s", shlex.join(cmd))
    logger.info("Starting gRPC server: %s", " ".join(shlex.quote(x) for x in cmd))
    env = dict(os.environ)
    if config.get("debug", False):
        env.setdefault("RUST_LOG", "debug,h2=info,tonic=info")  # h2 and tonic are very verbose at DEBUG level
    if "statsd_host" in config:
        env["STATSD_HOST"] = config["statsd_host"]
    if "statsd_port" in config:
        env["STATSD_PORT"] = str(config["statsd_port"])
    server = subprocess.Popen(cmd, env=env)
    return server, port


def stop_grpc_server(server: subprocess.Popen, timeout: int = 15):
    server.terminate()
    try:
        server.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.warning("Server did not terminate, sending kill signal...")
        server.kill()

