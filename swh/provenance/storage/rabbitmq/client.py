# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

import functools
import inspect
import logging
import queue
import threading
import time
from types import TracebackType
from typing import Any, Dict, Iterable, Optional, Set, Tuple, Type, Union
import uuid

import pika
import pika.channel
import pika.connection
import pika.frame
import pika.spec

from swh.core.api.serializers import encode_data_client as encode_data
from swh.core.api.serializers import msgpack_loads as decode_data
from swh.core.statsd import statsd
from swh.provenance.storage import get_provenance_storage
from swh.provenance.storage.interface import (
    ProvenanceStorageInterface,
    RelationData,
    RelationType,
)

from .serializers import DECODERS, ENCODERS
from .server import ProvenanceStorageRabbitMQServer

LOG_FORMAT = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)
LOGGER = logging.getLogger(__name__)

STORAGE_DURATION_METRIC = "swh_provenance_storage_rabbitmq_duration_seconds"


class ResponseTimeout(Exception):
    pass


class TerminateSignal(Exception):
    pass


def split_ranges(
    data: Iterable[bytes], meth_name: str, relation: Optional[RelationType] = None
) -> Dict[str, Set[Tuple[Any, ...]]]:
    ranges: Dict[str, Set[Tuple[Any, ...]]] = {}
    if relation is not None:
        assert isinstance(data, dict), "Relation data must be provided in a dictionary"
        for src, dsts in data.items():
            key = ProvenanceStorageRabbitMQServer.get_routing_key(
                src, meth_name, relation
            )
            for rel in dsts:
                assert isinstance(
                    rel, RelationData
                ), "Values in the dictionary must be RelationData structures"
                ranges.setdefault(key, set()).add((src, rel.dst, rel.path))
    else:
        items: Union[Set[Tuple[bytes, Any]], Set[Tuple[bytes]]]
        if isinstance(data, dict):
            items = set(data.items())
        else:
            # TODO this is probably not used any more
            items = {(item,) for item in data}
        for id, *rest in items:
            key = ProvenanceStorageRabbitMQServer.get_routing_key(id, meth_name)
            ranges.setdefault(key, set()).add((id, *rest))
    return ranges


class MetaRabbitMQClient(type):
    def __new__(cls, name, bases, attributes):
        # For each method wrapped with @remote_api_endpoint in an API backend
        # (eg. :class:`swh.indexer.storage.IndexerStorage`), add a new
        # method in RemoteStorage, with the same documentation.
        #
        # Note that, despite the usage of decorator magic (eg. functools.wrap),
        # this never actually calls an IndexerStorage method.
        backend_class = attributes.get("backend_class", None)
        for base in bases:
            if backend_class is not None:
                break
            backend_class = getattr(base, "backend_class", None)
        if backend_class:
            for meth_name, meth in backend_class.__dict__.items():
                if hasattr(meth, "_endpoint_path"):
                    cls.__add_endpoint(meth_name, meth, attributes)
        return super().__new__(cls, name, bases, attributes)

    @staticmethod
    def __add_endpoint(meth_name, meth, attributes):
        wrapped_meth = inspect.unwrap(meth)

        @functools.wraps(meth)  # Copy signature and doc
        def meth_(*args, **kwargs):
            with statsd.timed(
                metric=STORAGE_DURATION_METRIC, tags={"method": meth_name}
            ):
                # Match arguments and parameters
                data = inspect.getcallargs(wrapped_meth, *args, **kwargs)

                # Remove arguments that should not be passed
                self = data.pop("self")

                # Call storage method with remaining arguments
                return getattr(self._storage, meth_name)(**data)

        @functools.wraps(meth)  # Copy signature and doc
        def write_meth_(*args, **kwargs):
            with statsd.timed(
                metric=STORAGE_DURATION_METRIC, tags={"method": meth_name}
            ):
                # Match arguments and parameters
                post_data = inspect.getcallargs(wrapped_meth, *args, **kwargs)

                try:
                    # Remove arguments that should not be passed
                    self = post_data.pop("self")
                    relation = post_data.pop("relation", None)
                    assert len(post_data) == 1
                    data = next(iter(post_data.values()))

                    ranges = split_ranges(data, meth_name, relation)
                    acks_expected = sum(len(items) for items in ranges.values())
                    self._correlation_id = str(uuid.uuid4())

                    exchange = ProvenanceStorageRabbitMQServer.get_exchange(
                        meth_name, relation
                    )
                    try:
                        self._delay_close = True
                        for routing_key, items in ranges.items():
                            items_list = list(items)
                            batches = (
                                items_list[idx : idx + self._batch_size]
                                for idx in range(0, len(items_list), self._batch_size)
                            )
                            for batch in batches:
                                # FIXME: this is running in a different thread! Hence, if
                                # self._connection drops, there is no guarantee that the
                                # request can be sent for the current elements. This
                                # situation should be handled properly.
                                self._connection.ioloop.add_callback_threadsafe(
                                    functools.partial(
                                        ProvenanceStorageRabbitMQClient.request,
                                        channel=self._channel,
                                        reply_to=self._callback_queue,
                                        exchange=exchange,
                                        routing_key=routing_key,
                                        correlation_id=self._correlation_id,
                                        data=batch,
                                    )
                                )
                        return self.wait_for_acks(meth_name, acks_expected)
                    finally:
                        self._delay_close = False
                except BaseException as ex:
                    self.request_termination(str(ex))
                    return False

        if meth_name not in attributes:
            attributes[meth_name] = (
                write_meth_
                if ProvenanceStorageRabbitMQServer.is_write_method(meth_name)
                else meth_
            )


class ProvenanceStorageRabbitMQClient(threading.Thread, metaclass=MetaRabbitMQClient):
    backend_class = ProvenanceStorageInterface
    extra_type_decoders = DECODERS
    extra_type_encoders = ENCODERS

    def __init__(
        self,
        url: str,
        storage_config: Dict[str, Any],
        batch_size: int = 100,
        prefetch_count: int = 100,
        wait_min: float = 60,
        wait_per_batch: float = 10,
    ) -> None:
        """Setup the client object, passing in the URL we will use to connect to
        RabbitMQ, and the connection information for the local storage object used
        for read-only operations.

        :param str url: The URL for connecting to RabbitMQ
        :param dict storage_config: Configuration parameters for the underlying
            ``ProvenanceStorage`` object expected by
            ``swh.provenance.get_provenance_storage``
        :param int batch_size: Max amount of elements per package (after range
            splitting) for writing operations
        :param int prefetch_count: Prefetch value for the RabbitMQ connection when
            receiving ack packages
        :param float wait_min: Min waiting time for response on a writing operation, in
            seconds
        :param float wait_per_batch: Waiting time for response per batch of items on a
            writing operation, in seconds

        """
        super().__init__()

        self._connection = None
        self._callback_queue: Optional[str] = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._correlation_id = str(uuid.uuid4())
        self._prefetch_count = prefetch_count

        self._batch_size = batch_size
        self._response_queue: queue.Queue = queue.Queue()
        self._storage = get_provenance_storage(**storage_config)
        self._url = url

        self._wait_min = wait_min
        self._wait_per_batch = wait_per_batch

        self._delay_close = False

    def __enter__(self) -> ProvenanceStorageInterface:
        self.open()
        assert isinstance(self, ProvenanceStorageInterface)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "open"})
    def open(self) -> None:
        self.start()
        while self._callback_queue is None:
            time.sleep(0.1)
        self._storage.open()

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "close"})
    def close(self) -> None:
        assert self._connection is not None
        self._connection.ioloop.add_callback_threadsafe(self.request_termination)
        self.join()
        self._storage.close()

    def request_termination(self, reason: str = "Normal shutdown") -> None:
        assert self._connection is not None

        def termination_callback():
            raise TerminateSignal(reason)

        self._connection.ioloop.add_callback_threadsafe(termination_callback)

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
        self._consuming = False
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
        LOGGER.debug("Creating a new channel")
        assert self._connection is not None
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel: pika.channel.Channel) -> None:
        LOGGER.debug("Channel opened")
        self._channel = channel
        LOGGER.debug("Adding channel close callback")
        assert self._channel is not None
        self._channel.add_on_close_callback(callback=self.on_channel_closed)
        self.setup_queue()

    def on_channel_closed(
        self, channel: pika.channel.Channel, reason: Exception
    ) -> None:
        LOGGER.warning("Channel %i was closed: %s", channel, reason)
        self.close_connection()

    def setup_queue(self) -> None:
        LOGGER.debug("Declaring callback queue")
        assert self._channel is not None
        self._channel.queue_declare(
            queue="", exclusive=True, callback=self.on_queue_declare_ok
        )

    def on_queue_declare_ok(self, frame: pika.frame.Method) -> None:
        LOGGER.debug("Binding queue to default exchanger")
        assert self._channel is not None
        self._callback_queue = frame.method.queue
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok
        )

    def on_basic_qos_ok(self, _unused_frame: pika.frame.Method) -> None:
        LOGGER.debug("QOS set to: %d", self._prefetch_count)
        self.start_consuming()

    def start_consuming(self) -> None:
        LOGGER.debug("Issuing consumer related RPC commands")
        LOGGER.debug("Adding consumer cancellation callback")
        assert self._channel is not None
        self._channel.add_on_cancel_callback(callback=self.on_consumer_cancelled)
        assert self._callback_queue is not None
        self._consumer_tag = self._channel.basic_consume(
            queue=self._callback_queue, on_message_callback=self.on_response
        )
        self._consuming = True

    def on_consumer_cancelled(self, method_frame: pika.frame.Method) -> None:
        LOGGER.debug("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if self._channel:
            self._channel.close()

    def on_response(
        self,
        channel: pika.channel.Channel,
        deliver: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        self._response_queue.put(
            (
                properties.correlation_id,
                decode_data(body, extra_decoders=self.extra_type_decoders),
            )
        )
        channel.basic_ack(delivery_tag=deliver.delivery_tag)

    def stop_consuming(self) -> None:
        if self._channel:
            LOGGER.debug("Sending a Basic.Cancel RPC command to RabbitMQ")
            self._channel.basic_cancel(self._consumer_tag, self.on_cancel_ok)

    def on_cancel_ok(self, _unused_frame: pika.frame.Method) -> None:
        self._consuming = False
        LOGGER.debug(
            "RabbitMQ acknowledged the cancellation of the consumer: %s",
            self._consumer_tag,
        )
        LOGGER.debug("Closing the channel")
        assert self._channel is not None
        self._channel.close()

    def run(self) -> None:
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
        LOGGER.info("Stopped")

    def stop(self) -> None:
        assert self._connection is not None
        if not self._closing:
            if self._delay_close:
                LOGGER.info("Delaying termination: waiting for a pending request")
                delay_start = time.monotonic()
                wait = 1
                while self._delay_close:
                    if wait >= 32:
                        LOGGER.warning(
                            "Still waiting for pending request (for %2f seconds)...",
                            time.monotonic() - delay_start,
                        )
                    time.sleep(wait)
                    wait = min(wait * 2, 60)

            self._closing = True
            LOGGER.info("Stopping")
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            LOGGER.info("Stopped")

    @staticmethod
    def request(
        channel: pika.channel.Channel,
        reply_to: str,
        exchange: str,
        routing_key: str,
        correlation_id: str,
        **kwargs,
    ) -> None:
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            properties=pika.BasicProperties(
                content_type="application/msgpack",
                correlation_id=correlation_id,
                reply_to=reply_to,
            ),
            body=encode_data(
                kwargs,
                extra_encoders=ProvenanceStorageRabbitMQClient.extra_type_encoders,
            ),
        )

    def wait_for_acks(self, meth_name: str, acks_expected: int) -> bool:
        acks_received = 0
        timeout = max(
            (acks_expected / self._batch_size) * self._wait_per_batch,
            self._wait_min,
        )
        start = time.monotonic()
        end = start + timeout
        while acks_received < acks_expected:
            local_timeout = end - time.monotonic()
            if local_timeout < 1.0:
                local_timeout = 1.0
            try:
                acks_received += self.wait_for_response(timeout=local_timeout)
            except ResponseTimeout:
                LOGGER.warning(
                    "Timed out waiting for acks in %s, %s received, %s expected (in %ss)",
                    meth_name,
                    acks_received,
                    acks_expected,
                    time.monotonic() - start,
                )
                return False
        return acks_received == acks_expected

    def wait_for_response(self, timeout: float = 120.0) -> Any:
        start = time.monotonic()
        end = start + timeout
        while True:
            try:
                local_timeout = end - time.monotonic()
                if local_timeout < 1.0:
                    local_timeout = 1.0
                correlation_id, response = self._response_queue.get(
                    timeout=local_timeout
                )
                if correlation_id == self._correlation_id:
                    return response
            except queue.Empty:
                raise ResponseTimeout
