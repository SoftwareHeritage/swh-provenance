# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
from datetime import datetime
from enum import Enum
import functools
import logging
import multiprocessing
import os
import queue
import threading
from typing import Any, Callable
from typing import Counter as TCounter
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple, Union, cast

import pika
import pika.channel
import pika.connection
import pika.exceptions
from pika.exchange_type import ExchangeType
import pika.frame
import pika.spec

from swh.core import config
from swh.core.api.serializers import encode_data_client as encode_data
from swh.core.api.serializers import msgpack_loads as decode_data
from swh.model.hashutil import hash_to_hex
from swh.model.model import Sha1Git

from .. import get_provenance_storage
from ..interface import (
    DirectoryData,
    EntityType,
    RelationData,
    RelationType,
    RevisionData,
)
from ..util import path_id
from .serializers import DECODERS, ENCODERS

LOG_FORMAT = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)
LOGGER = logging.getLogger(__name__)

TERMINATE = object()


class ServerCommand(Enum):
    TERMINATE = "terminate"
    CONSUMING = "consuming"


class TerminateSignal(BaseException):
    pass


def resolve_dates(dates: Iterable[Tuple[Sha1Git, datetime]]) -> Dict[Sha1Git, datetime]:
    result: Dict[Sha1Git, datetime] = {}
    for sha1, date in dates:
        known = result.setdefault(sha1, date)
        if date < known:
            result[sha1] = date
    return result


def resolve_directory(
    data: Iterable[Tuple[Sha1Git, DirectoryData]]
) -> Dict[Sha1Git, DirectoryData]:
    result: Dict[Sha1Git, DirectoryData] = {}
    for sha1, dir in data:
        known = result.setdefault(sha1, dir)
        value = known
        if dir.date < known.date:
            value = DirectoryData(date=dir.date, flat=value.flat)
        if dir.flat:
            value = DirectoryData(date=value.date, flat=dir.flat)
        if value != known:
            result[sha1] = value
    return result


def resolve_revision(
    data: Iterable[Union[Tuple[Sha1Git, RevisionData], Tuple[Sha1Git]]]
) -> Dict[Sha1Git, RevisionData]:
    result: Dict[Sha1Git, RevisionData] = {}
    for row in data:
        sha1 = row[0]
        rev = (
            cast(Tuple[Sha1Git, RevisionData], row)[1]
            if len(row) > 1
            else RevisionData(date=None, origin=None)
        )
        known = result.setdefault(sha1, RevisionData(date=None, origin=None))
        value = known
        if rev.date is not None and (known.date is None or rev.date < known.date):
            value = RevisionData(date=rev.date, origin=value.origin)
        if rev.origin is not None:
            value = RevisionData(date=value.date, origin=rev.origin)
        if value != known:
            result[sha1] = value
    return result


def resolve_relation(
    data: Iterable[Tuple[Sha1Git, Sha1Git, bytes]]
) -> Dict[Sha1Git, Set[RelationData]]:
    result: Dict[Sha1Git, Set[RelationData]] = {}
    for src, dst, path in data:
        result.setdefault(src, set()).add(RelationData(dst=dst, path=path))
    return result


class ProvenanceStorageRabbitMQWorker(multiprocessing.Process):
    EXCHANGE_TYPE = ExchangeType.direct
    extra_type_decoders = DECODERS
    extra_type_encoders = ENCODERS

    def __init__(
        self,
        url: str,
        exchange: str,
        range: int,
        storage_config: Dict[str, Any],
        batch_size: int = 100,
        prefetch_count: int = 100,
    ) -> None:
        """Setup the worker object, passing in the URL we will use to connect to
        RabbitMQ, the exchange to use, the range id on which to operate, and the
        connection information for the underlying local storage object.

        :param str url: The URL for connecting to RabbitMQ
        :param str exchange: The name of the RabbitMq exchange to use
        :param str range: The ID range to operate on
        :param dict storage_config: Configuration parameters for the underlying
            ``ProvenanceStorage`` object expected by
            ``swh.provenance.get_provenance_storage``
        :param int batch_size: Max amount of elements call to the underlying storage
        :param int prefetch_count: Prefetch value for the RabbitMQ connection when
            receiving messaged

        """
        super().__init__(name=f"{exchange}_{range:x}")

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag: Dict[str, str] = {}
        self._consuming: Dict[str, bool] = {}
        self._prefetch_count = prefetch_count

        self._url = url
        self._exchange = exchange
        self._binding_keys = list(
            ProvenanceStorageRabbitMQServer.get_binding_keys(self._exchange, range)
        )
        self._queues: Dict[str, str] = {}
        self._storage_config = storage_config
        self._batch_size = batch_size

        self.command: multiprocessing.Queue = multiprocessing.Queue()
        self.signal: multiprocessing.Queue = multiprocessing.Queue()

    def connect(self) -> pika.SelectConnection:
        LOGGER.info("Connecting to %s", self._url)
        return pika.SelectConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
        )

    def close_connection(self) -> None:
        assert self._connection is not None
        self._consuming = {binding_key: False for binding_key in self._binding_keys}
        if self._connection.is_closing or self._connection.is_closed:
            LOGGER.info("Connection is closing or already closed")
        else:
            LOGGER.info("Closing connection")
            self._connection.close()

    def on_connection_open(self, _unused_connection: pika.SelectConnection) -> None:
        LOGGER.info("Connection opened")
        self.open_channel()

    def on_connection_open_error(
        self, _unused_connection: pika.SelectConnection, err: Exception
    ) -> None:
        LOGGER.error("Connection open failed, reopening in 5 seconds: %s", err)
        assert self._connection is not None
        self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_connection_closed(self, _unused_connection: pika.SelectConnection, reason):
        assert self._connection is not None
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning("Connection closed, reopening in 5 seconds: %s", reason)
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def open_channel(self) -> None:
        LOGGER.info("Creating a new channel")
        assert self._connection is not None
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel: pika.channel.Channel) -> None:
        LOGGER.info("Channel opened")
        self._channel = channel
        LOGGER.info("Adding channel close callback")
        assert self._channel is not None
        self._channel.add_on_close_callback(callback=self.on_channel_closed)
        self.setup_exchange()

    def on_channel_closed(
        self, channel: pika.channel.Channel, reason: Exception
    ) -> None:
        LOGGER.warning("Channel %i was closed: %s", channel, reason)
        self.close_connection()

    def setup_exchange(self) -> None:
        LOGGER.info("Declaring exchange %s", self._exchange)
        assert self._channel is not None
        self._channel.exchange_declare(
            exchange=self._exchange,
            exchange_type=self.EXCHANGE_TYPE,
            callback=self.on_exchange_declare_ok,
        )

    def on_exchange_declare_ok(self, _unused_frame: pika.frame.Method) -> None:
        LOGGER.info("Exchange declared: %s", self._exchange)
        self.setup_queues()

    def setup_queues(self) -> None:
        for binding_key in self._binding_keys:
            LOGGER.info("Declaring queue %s", binding_key)
            assert self._channel is not None
            callback = functools.partial(
                self.on_queue_declare_ok,
                binding_key=binding_key,
            )
            self._channel.queue_declare(queue=binding_key, callback=callback)

    def on_queue_declare_ok(self, frame: pika.frame.Method, binding_key: str) -> None:
        LOGGER.info(
            "Binding queue %s to exchange %s with routing key %s",
            frame.method.queue,
            self._exchange,
            binding_key,
        )
        assert self._channel is not None
        callback = functools.partial(self.on_bind_ok, queue_name=frame.method.queue)
        self._queues[binding_key] = frame.method.queue
        self._channel.queue_bind(
            queue=frame.method.queue,
            exchange=self._exchange,
            routing_key=binding_key,
            callback=callback,
        )

    def on_bind_ok(self, _unused_frame: pika.frame.Method, queue_name: str) -> None:
        LOGGER.info("Queue bound: %s", queue_name)
        assert self._channel is not None
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok
        )

    def on_basic_qos_ok(self, _unused_frame: pika.frame.Method) -> None:
        LOGGER.info("QOS set to: %d", self._prefetch_count)
        self.start_consuming()

    def start_consuming(self) -> None:
        LOGGER.info("Issuing consumer related RPC commands")
        LOGGER.info("Adding consumer cancellation callback")
        assert self._channel is not None
        self._channel.add_on_cancel_callback(callback=self.on_consumer_cancelled)
        for binding_key in self._binding_keys:
            self._consumer_tag[binding_key] = self._channel.basic_consume(
                queue=self._queues[binding_key], on_message_callback=self.on_request
            )
            self._consuming[binding_key] = True
        self.signal.put(ServerCommand.CONSUMING)

    def on_consumer_cancelled(self, method_frame: pika.frame.Method) -> None:
        LOGGER.info("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if self._channel:
            self._channel.close()

    def on_request(
        self,
        channel: pika.channel.Channel,
        deliver: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        LOGGER.info(
            "Received message # %s from %s: %s",
            deliver.delivery_tag,
            properties.app_id,
            body,
        )
        # XXX: for some reason this function is returning lists instead of tuples
        #      (the client send tuples)
        batch = decode_data(data=body, extra_decoders=self.extra_type_decoders)["data"]
        for item in batch:
            self._request_queues[deliver.routing_key].put(
                (tuple(item), (properties.correlation_id, properties.reply_to))
            )
        LOGGER.info("Acknowledging message %s", deliver.delivery_tag)
        channel.basic_ack(delivery_tag=deliver.delivery_tag)

    def stop_consuming(self) -> None:
        if self._channel:
            LOGGER.info("Sending a Basic.Cancel RPC command to RabbitMQ")
            for binding_key in self._binding_keys:
                callback = functools.partial(self.on_cancel_ok, binding_key=binding_key)
                self._channel.basic_cancel(
                    self._consumer_tag[binding_key], callback=callback
                )

    def on_cancel_ok(self, _unused_frame: pika.frame.Method, binding_key: str) -> None:
        self._consuming[binding_key] = False
        LOGGER.info(
            "RabbitMQ acknowledged the cancellation of the consumer: %s",
            self._consuming[binding_key],
        )
        LOGGER.info("Closing the channel")
        assert self._channel is not None
        self._channel.close()

    def run(self) -> None:
        self._command_thread = threading.Thread(target=self.run_command_thread)
        self._command_thread.start()

        self._request_queues: Dict[str, queue.Queue] = {}
        self._request_threads: Dict[str, threading.Thread] = {}
        for binding_key in self._binding_keys:
            meth_name, relation = ProvenanceStorageRabbitMQServer.get_meth_name(
                binding_key
            )
            self._request_queues[binding_key] = queue.Queue()
            self._request_threads[binding_key] = threading.Thread(
                target=self.run_request_thread,
                args=(binding_key, meth_name, relation),
            )
            self._request_threads[binding_key].start()

        while not self._closing:
            try:
                self._connection = self.connect()
                assert self._connection is not None
                self._connection.ioloop.start()
            except KeyboardInterrupt:
                LOGGER.info("Connection closed by keyboard interruption, reopening")
                if self._connection is not None:
                    self._connection.ioloop.stop()
            except TerminateSignal as ex:
                LOGGER.info("Termination requested: %s", ex)
                self.stop()
                if self._connection is not None and not self._connection.is_closed:
                    # Finish closing
                    self._connection.ioloop.start()
            except BaseException as ex:
                LOGGER.warning("Unexpected exception, terminating: %s", ex)
                self.stop()
                if self._connection is not None and not self._connection.is_closed:
                    # Finish closing
                    self._connection.ioloop.start()

        for binding_key in self._binding_keys:
            self._request_queues[binding_key].put(TERMINATE)
        for binding_key in self._binding_keys:
            self._request_threads[binding_key].join()
        self._command_thread.join()
        LOGGER.info("Stopped")

    def run_command_thread(self) -> None:
        while True:
            try:
                command = self.command.get()
                if command == ServerCommand.TERMINATE:
                    self.request_termination()
                    break
            except queue.Empty:
                pass
            except BaseException as ex:
                self.request_termination(str(ex))
                break

    def request_termination(self, reason: str = "Normal shutdown") -> None:
        assert self._connection is not None

        def termination_callback():
            raise TerminateSignal(reason)

        self._connection.ioloop.add_callback_threadsafe(termination_callback)

    def run_request_thread(
        self, binding_key: str, meth_name: str, relation: Optional[RelationType]
    ) -> None:
        with get_provenance_storage(**self._storage_config) as storage:
            request_queue = self._request_queues[binding_key]
            merge_items = ProvenanceStorageRabbitMQWorker.get_conflicts_func(meth_name)
            while True:
                terminate = False
                elements = []
                while True:
                    try:
                        # TODO: consider reducing this timeout or removing it
                        elem = request_queue.get(timeout=0.1)
                        if elem is TERMINATE:
                            terminate = True
                            break
                        elements.append(elem)
                    except queue.Empty:
                        break

                    if len(elements) >= self._batch_size:
                        break

                if terminate:
                    break

                if not elements:
                    continue

                try:
                    items, props = zip(*elements)
                    acks_count: TCounter[Tuple[str, str]] = Counter(props)
                    data = merge_items(items)

                    args = (relation, data) if relation is not None else (data,)
                    if getattr(storage, meth_name)(*args):
                        for (correlation_id, reply_to), count in acks_count.items():
                            # FIXME: this is running in a different thread! Hence, if
                            # self._connection drops, there is no guarantee that the
                            # response can be sent for the current elements. This
                            # situation should be handled properly.
                            assert self._connection is not None
                            self._connection.ioloop.add_callback_threadsafe(
                                functools.partial(
                                    ProvenanceStorageRabbitMQWorker.respond,
                                    channel=self._channel,
                                    correlation_id=correlation_id,
                                    reply_to=reply_to,
                                    response=count,
                                )
                            )
                    else:
                        LOGGER.warning(
                            "Unable to process elements for queue %s", binding_key
                        )
                        for elem in elements:
                            request_queue.put(elem)
                except BaseException as ex:
                    self.request_termination(str(ex))
                    break

    def stop(self) -> None:
        assert self._connection is not None
        if not self._closing:
            self._closing = True
            LOGGER.info("Stopping")
            if any(self._consuming):
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            LOGGER.info("Stopped")

    @staticmethod
    def get_conflicts_func(meth_name: str) -> Callable[[Iterable[Any]], Any]:
        if meth_name == "content_add":
            return resolve_dates
        elif meth_name == "directory_add":
            return resolve_directory
        elif meth_name == "location_add":
            return lambda data: set(data)  # just remove duplicates
        elif meth_name == "origin_add":
            return lambda data: dict(data)  # last processed value is good enough
        elif meth_name == "revision_add":
            return resolve_revision
        elif meth_name == "relation_add":
            return resolve_relation
        else:
            LOGGER.warning(
                "Unexpected conflict resolution function request for method %s",
                meth_name,
            )
            return lambda x: x

    @staticmethod
    def respond(
        channel: pika.channel.Channel,
        correlation_id: str,
        reply_to: str,
        response: Any,
    ):
        channel.basic_publish(
            exchange="",
            routing_key=reply_to,
            properties=pika.BasicProperties(
                content_type="application/msgpack",
                correlation_id=correlation_id,
            ),
            body=encode_data(
                response,
                extra_encoders=ProvenanceStorageRabbitMQServer.extra_type_encoders,
            ),
        )


class ProvenanceStorageRabbitMQServer:
    extra_type_decoders = DECODERS
    extra_type_encoders = ENCODERS

    queue_count = 16

    def __init__(
        self,
        url: str,
        storage_config: Dict[str, Any],
        batch_size: int = 100,
        prefetch_count: int = 100,
    ) -> None:
        """Setup the server object, passing in the URL we will use to connect to
        RabbitMQ, and the connection information for the underlying local storage
        object.

        :param str url: The URL for connecting to RabbitMQ
        :param dict storage_config: Configuration parameters for the underlying
            ``ProvenanceStorage`` object expected by
            ``swh.provenance.get_provenance_storage``
        :param int batch_size: Max amount of elements call to the underlying storage
        :param int prefetch_count: Prefetch value for the RabbitMQ connection when
            receiving messaged

        """
        self._workers: List[ProvenanceStorageRabbitMQWorker] = []
        for exchange in ProvenanceStorageRabbitMQServer.get_exchanges():
            for range in ProvenanceStorageRabbitMQServer.get_ranges(exchange):
                worker = ProvenanceStorageRabbitMQWorker(
                    url=url,
                    exchange=exchange,
                    range=range,
                    storage_config=storage_config,
                    batch_size=batch_size,
                    prefetch_count=prefetch_count,
                )
                self._workers.append(worker)
        self._running = False

    def start(self) -> None:
        if not self._running:
            self._running = True
            for worker in self._workers:
                worker.start()
            for worker in self._workers:
                try:
                    signal = worker.signal.get(timeout=60)
                    assert signal == ServerCommand.CONSUMING
                except queue.Empty:
                    LOGGER.error(
                        "Could not initialize worker %s. Leaving...", worker.name
                    )
                    self.stop()
                    return
            LOGGER.info("Start serving")

    def stop(self) -> None:
        if self._running:
            for worker in self._workers:
                worker.command.put(ServerCommand.TERMINATE)
            for worker in self._workers:
                worker.join()
            LOGGER.info("Stop serving")
            self._running = False

    @staticmethod
    def get_binding_keys(exchange: str, range: int) -> Generator[str, None, None]:
        for meth_name, relation in ProvenanceStorageRabbitMQServer.get_meth_names(
            exchange
        ):
            if relation is None:
                assert (
                    meth_name != "relation_add"
                ), "'relation_add' requires 'relation' to be provided"
                yield f"{meth_name}.unknown.{range:x}".lower()
            else:
                assert (
                    meth_name == "relation_add"
                ), f"'{meth_name}' requires 'relation' to be None"
                yield f"{meth_name}.{relation.value}.{range:x}".lower()

    @staticmethod
    def get_exchange(meth_name: str, relation: Optional[RelationType] = None) -> str:
        if meth_name == "relation_add":
            assert (
                relation is not None
            ), "'relation_add' requires 'relation' to be provided"
            split = relation.value
        else:
            assert relation is None, f"'{meth_name}' requires 'relation' to be None"
            split = meth_name
        exchange, *_ = split.split("_")
        return exchange

    @staticmethod
    def get_exchanges() -> Generator[str, None, None]:
        yield from [entity.value for entity in EntityType] + ["location"]

    @staticmethod
    def get_meth_name(
        binding_key: str,
    ) -> Tuple[str, Optional[RelationType]]:
        meth_name, relation, *_ = binding_key.split(".")
        return meth_name, (RelationType(relation) if relation != "unknown" else None)

    @staticmethod
    def get_meth_names(
        exchange: str,
    ) -> Generator[Tuple[str, Optional[RelationType]], None, None]:
        if exchange == EntityType.CONTENT.value:
            yield from [
                ("content_add", None),
                ("relation_add", RelationType.CNT_EARLY_IN_REV),
                ("relation_add", RelationType.CNT_IN_DIR),
            ]
        elif exchange == EntityType.DIRECTORY.value:
            yield from [
                ("directory_add", None),
                ("relation_add", RelationType.DIR_IN_REV),
            ]
        elif exchange == EntityType.ORIGIN.value:
            yield from [("origin_add", None)]
        elif exchange == EntityType.REVISION.value:
            yield from [
                ("revision_add", None),
                ("relation_add", RelationType.REV_BEFORE_REV),
                ("relation_add", RelationType.REV_IN_ORG),
            ]
        elif exchange == "location":
            yield "location_add", None

    @staticmethod
    def get_ranges(unused_exchange: str) -> Generator[int, None, None]:
        # XXX: we might want to have a different range per exchange
        yield from range(ProvenanceStorageRabbitMQServer.queue_count)

    @staticmethod
    def get_routing_key(
        item: bytes, meth_name: str, relation: Optional[RelationType] = None
    ) -> str:
        hashid = (
            path_id(item).hex()
            if meth_name.startswith("location")
            else hash_to_hex(item)
        )
        idx = int(hashid[0], 16) % ProvenanceStorageRabbitMQServer.queue_count
        if relation is None:
            assert (
                meth_name != "relation_add"
            ), "'relation_add' requires 'relation' to be provided"
            return f"{meth_name}.unknown.{idx:x}".lower()
        else:
            assert (
                meth_name == "relation_add"
            ), f"'{meth_name}' requires 'relation' to be None"
            return f"{meth_name}.{relation.value}.{idx:x}".lower()

    @staticmethod
    def is_write_method(meth_name: str) -> bool:
        return "_add" in meth_name


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

    rcfg: Optional[Dict[str, Any]] = pcfg.get("rabbitmq")
    if rcfg is None:
        raise KeyError("Missing 'provenance.rabbitmq' configuration")

    scfg: Optional[Dict[str, Any]] = rcfg.get("storage_config")
    if scfg is None:
        raise KeyError("Missing 'provenance.rabbitmq.storage_config' configuration")

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


def make_server_from_configfile() -> ProvenanceStorageRabbitMQServer:
    config_path = os.environ.get("SWH_CONFIG_FILENAME")
    server_cfg = load_and_check_config(config_path)
    return ProvenanceStorageRabbitMQServer(**server_cfg["provenance"]["rabbitmq"])
