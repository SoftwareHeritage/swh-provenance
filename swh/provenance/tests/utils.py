# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from contextlib import contextmanager
from datetime import datetime
import logging
import multiprocessing
from os import path
from pathlib import Path
import tempfile
from typing import Any, Dict, List, Optional

from aiohttp.test_utils import TestClient, TestServer, loop_context
from click.testing import CliRunner, Result
import msgpack
from yaml import safe_dump

from swh.graph.http_rpc_server import make_app
from swh.journal.serializers import msgpack_ext_hook
from swh.model.model import BaseModel, TimestampWithTimezone
from swh.provenance.cli import cli
from swh.storage.interface import StorageInterface
from swh.storage.replay import OBJECT_CONVERTERS, OBJECT_FIXERS, process_replay_objects


def invoke(
    args: List[str], config: Optional[Dict] = None, catch_exceptions: bool = False
) -> Result:
    """Invoke swh journal subcommands"""
    runner = CliRunner()
    with tempfile.NamedTemporaryFile("a", suffix=".yml") as config_fd:
        if config is not None:
            safe_dump(config, config_fd)
            config_fd.seek(0)
            args = ["-C" + config_fd.name] + args

        result = runner.invoke(cli, args, obj={"log_level": logging.DEBUG}, env=None)
        if not catch_exceptions and result.exception:
            print(result.output)
            raise result.exception
    return result


def fill_storage(storage: StorageInterface, data: Dict[str, List[dict]]) -> None:
    objects = {
        objtype: [objs_from_dict(objtype, d) for d in dicts]
        for objtype, dicts in data.items()
    }
    process_replay_objects(objects, storage=storage)


def get_datafile(fname: str) -> str:
    return path.join(path.dirname(__file__), "data", fname)


# TODO: this should return Dict[str, List[BaseModel]] directly, but it requires
#       refactoring several tests
def load_repo_data(repo: str) -> Dict[str, List[dict]]:
    data: Dict[str, List[dict]] = {}
    with open(get_datafile(f"{repo}.msgpack"), "rb") as fobj:
        unpacker = msgpack.Unpacker(
            fobj,
            raw=False,
            ext_hook=msgpack_ext_hook,
            strict_map_key=False,
            timestamp=3,  # convert Timestamp in datetime objects (tz UTC)
        )
        for msg in unpacker:
            if len(msg) == 2:  # old format
                objtype, objd = msg
            else:  # now we should have a triplet (type, key, value)
                objtype, _, objd = msg
            data.setdefault(objtype, []).append(objd)
    return data


def objs_from_dict(object_type: str, dict_repr: dict) -> BaseModel:
    if object_type in OBJECT_FIXERS:
        dict_repr = OBJECT_FIXERS[object_type](dict_repr)
    obj = OBJECT_CONVERTERS[object_type](dict_repr)
    return obj


def ts2dt(ts: Dict[str, Any]) -> datetime:
    return TimestampWithTimezone.from_dict(ts).to_datetime()


def run_grpc_server(queue, dataset_path):
    try:
        config = {
            "graph": {
                "cls": "local",
                "grpc_server": {"path": dataset_path},
                "http_rpc_server": {"debug": True},
            }
        }
        with loop_context() as loop:
            app = make_app(config=config)
            client = TestClient(TestServer(app), loop=loop)
            loop.run_until_complete(client.start_server())
            url = client.make_url("/graph/")
            queue.put((url, app["rpc_url"]))
            loop.run_forever()
    except Exception as e:
        queue.put(e)


@contextmanager
def grpc_server(dataset):
    dataset_path = (
        Path(__file__).parents[0] / "data/swhgraph" / dataset / "compressed/example"
    )
    queue = multiprocessing.Queue()
    server = multiprocessing.Process(
        target=run_grpc_server, kwargs={"queue": queue, "dataset_path": dataset_path}
    )
    server.start()
    res = queue.get()
    if isinstance(res, Exception):
        raise res
    grpc_url = res[1]
    try:
        yield grpc_url
    finally:
        server.terminate()
