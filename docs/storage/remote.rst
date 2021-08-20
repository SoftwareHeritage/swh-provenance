:orphan:

ProvenanceStorageRabbitMQClient
===============================

The remote storage client connects to a remote server using a RabbitMQ
broker, and delegates all writing operations to it. However, reading
operations are still done using a local storage object (ie.
``ProvenanceStorageMongoDb`` or ``ProvenanceStoragePostgreSql``). This
is done to speed up the process by avoiding the unnecessary RabbitMQ
overhead for operations that are conflict-free.

.. warning::

   No check is done to guarantee that the local storage used by the
   client is the same as the one the server uses on its side, but it is
   assumed to be the case.

When a writing operation is invoked, the client splits the set of
elements to be written by ID range, and send a series of packages to the
server (via the RabbitMQ broker). Each package consists of elements
belonging to the same ID range but, to avoid sending huge packages,
there might be more than one package per range. After this, the client
blocks waiting for the server to acknowledge the writing of all
requested elements.

To initialize a client object it is required to provide two mandatory
parameters:

-  ``url``: the URL string of the broker where the server expects to
   receive the packages.
-  ``storage_config``: a dictionary containing the storage configuration
   for the local storage object, as expected by
   ``swh.provenance.get_provenance_storage``.

Additionally, some optional parameter can be specified that may affect
the performance of the remote storage as a whole:

-  ``batch_size``: an integer specifying the maximum allowed amount of
   elements per package, after range splitting. Default to 100.
-  ``prefetch_count``: an integer specifying how many ack packages are
   prefetched from the broker. Default to 100.
-  ``wait_min``: a float specifying the minimum amount of seconds that
   the client should wait for the server’s response before failing.
   Default to 60.
-  ``wait_per_batch``: a float specifying the amount of seconds to wait
   per sent batch if items (ie. package). If
   ``wait_per_batch * number_of_packages`` is less than ``wait_min``,
   the latter will be used instead. Default to 10.

As all ``ProvenanceStorageInterface`` compliant objects, the remote
storage client has to be opened before being able to use it, and closed
after it is no longer needed to properly release resources. It can be
operated as a context manager using the keyword ``with``. This will take
care of actually opening/closing both the connection to the remote
server as well as the underlying local storage objects.

Client lifecycle
----------------

Connecting to the remote server object implies taking care of the
RabbitMQ’s lifecycle. To that end, an internal thread is launched, which
will re-establish the connection in case of an error until an explicit
disconnect request is made. The class
``ProvenanceStorageRabbitMQClient`` extends the standard
``threading.Thread`` class, thus the ``run`` method in the class is the
target function of such thread, that will loop indefinitely calling the
``connect`` method and the blocking ``ioloop.connect`` method of the
established connection. Interaction with such loop is done by setting
callback functions.

The following is a diagram of the interaction between the methods of the
class:

.. graphviz::

   digraph {
     ProvenanceStorageRabbitMQClient

     close[shape=record]
     open[shape=record]
     add[shape=record,label="write method"]
     get[shape=record,label="read method"]

     ProvenanceStorageInterface

     start[shape=record]
     stop[shape=record]

     request[shape=record]
     wait_for_acks[shape=record]
     wait_for_response[shape=record]

     subgraph cluster_connection_thread {
       style=rounded
       bgcolor=gray95
       color=gray
       labelloc=b

       run[shape=record]

       connect[shape=record]
       on_connection_open[shape=record]
       on_connection_open_error[shape=record]
       on_connection_closed[shape=record]
       close_connection[shape=record]
       open_channel[shape=record]
       on_channel_open[shape=record]
       on_channel_closed[shape=record]
       setup_queue[shape=record]
       on_queue_declare_ok[shape=record]
       on_basic_qos_ok[shape=record]
       start_consuming[shape=record]
       on_consumer_cancelled[shape=record]
       on_response[shape=record]
       stop_consuming[shape=record]
       on_cancel_ok[shape=record]
     }

     ProvenanceStorageRabbitMQClient->{add,close,get,open}

     close->{stop}
     open->{start}

     start->{run}
     stop->{stop_consuming}

     run->{connect,stop}

     connect->{on_connection_open,on_connection_open_error,on_connection_closed}

     on_connection_open->{open_channel}

     open_channel->{on_channel_open}

     on_cancel_ok->{on_channel_closed}
     on_consumer_cancelled->{on_channel_closed}
     on_channel_open->{setup_queue}

     on_channel_closed->{close_connection}

     setup_queue->{on_queue_declare_ok}

     on_queue_declare_ok->{on_basic_qos_ok}

     on_basic_qos_ok->{start_consuming}

     start_consuming->{on_consumer_cancelled,on_response}

     on_response->{wait_for_response}[label="_response_queue",arrowhead="none"]

     stop_consuming->{on_cancel_ok}

     add->{request,wait_for_acks}
     wait_for_acks->{wait_for_response}

     get->{ProvenanceStorageInterface}
   }

Every write method in the ``ProvenanceStorageInterface`` performs
splitting by ID range and send a series of messages to the server by
calling the ``request`` method. After that, it blocks waiting for all
necessary acks packages to arrive by calling the ``wait_for_acks``
methods, which in-turn calls ``wait_for_response``. Calls to the write
methods may come from distinct threads, thus the interaction between
``on_response`` and ``wait_for_response`` is done with a thread-safe
``Queue.queue`` structure ``_response_queue``.

Below there is a summary of the methods present in the diagram and their
functionality. Methods are listed in alphabetic order:

-  ``close_connection``: this method properly closes the connection to
   RabbitMQ.
-  ``connect``: this method connects to RabbitMQ, returning the
   connection handle. When the connection is established, the
   ``on_connection_open`` callback method will be invoked by ``pika``.
   If there is an error establishing the connection, the
   ``on_connection_open_error`` callback method will be invoked by
   ``pika``. If the connection closes unexpectedly, the
   ``on_connection_closed`` callback method will be invoked by ``pika``.
-  ``on_basic_qos_ok``: this callback method is invoked by ``pika`` when
   the ``Basic.QoS`` RPC call made in ``on_queue_declare_ok`` has
   completed. At this point it is safe to start consuming messages,
   hence the ``start_consuming`` method is called, which will invoke the
   needed RPC commands to start the process.
-  ``on_cancel_ok``: this callback method is invoked by ``pika`` when
   RabbitMQ acknowledges the cancellation of a consumer. At this point
   the channel close is requested, thus indirectly invoking the
   ``on_channel_closed`` callback method once the channel has been
   closed, which will in-turn close the connection.
-  ``on_channel_closed``: this callback method is invoked by ``pika``
   when RabbitMQ unexpectedly closes the channel. Channels are usually
   closed if there is an attempt to do something that violates the
   protocol, such as re-declare an exchange or queue with different
   parameters. In this case, the connection will be closed by invoking
   the ``close_connection``.
-  ``on_channel_open``: this callback method is invoked by ``pika`` when
   the channel has been opened. The ``on_channel_closed`` callback is
   set here. Since the channel is now open, it is safe to set up the
   client’s response queue on RabbitMQ by invoking the ``setup_queue``
   method.
-  ``on_connection_closed``: this callback method is invoked by ``pika``
   when the connection to RabbitMQ is closed unexpectedly. Since it is
   unexpected, an attempt to reconnect to RabbitMQ will be done.
-  ``on_connection_open``: this callback method is invoked by ``pika``
   once the connection to RabbitMQ has been established. It proceeds to
   open the channel by calling the ``open_channel`` method.
-  ``on_connection_open_error``: this callback method is invoked by
   ``pika`` if the connection to RabbitMQ can’t be established. Since it
   is unexpected, an attempt to reconnect to RabbitMQ will be done.
-  ``on_consumer_cancelled``: this callback methods is invoked by
   ``pika`` when RabbitMQ sends a ``Basic.Cancel`` for a consumer
   receiving messages. At this point the channel close is requested,
   thus indirectly invoking the ``on_channel_closed`` callback method
   once the channel has been closed, which will in-turn close the
   connection.
-  ``on_queue_declare_ok``: this callback method is invoked by ``pika``
   when the ``Queue.Declare`` RPC call made in ``setup_queue`` has
   completed. This method sets up the consumer prefetch count by
   invoking the ``Basic.QoS`` RPC command. When it is completed, the
   ``on_basic_qos_ok`` method will be invoked by ``pika``.
-  ``on_response``: this callback method is invoked by ``pika`` when a
   message is delivered from RabbitMQ. The decoded response together
   with its correlation ID is enqueued in the internal
   ``_response_queue``, so that the data is forwarded to the
   ``wait_for_response`` method, that might be running on a distinct
   thread. A ``Basic.Ack`` RPC command is issued to acknowledge the
   delivery of the message to RabbitMQ.
-  ``open_channel``: this method opens a new channel with RabbitMQ by
   issuing the ``Channel.Open`` RPC command. When RabbitMQ responds that
   the channel is open, the ``on_channel_open`` callback will be invoked
   by ``pika``.
-  ``request``: this methods send a message to RabbitMQ by issuing a
   ``Basic.Publish`` RPC command. The body of the message is properly
   encoded, while correlation ID and request key are used as passed by
   the calling method.
-  ``run``: main method of the internal thread. It requests to open a
   connection to RabbitMQ by calling the ``connect`` method, and starts
   the internal ``IOLoop`` of the returned handle (blocking operation).
   In case of failure, this method will indefinitely try to reconnect.
   When an explicit ``TerminateSignal`` is received, the ``stop`` method
   is invoked.
-  ``setup_queue``: this methods sets up an exclusive queue for the
   client on RabbitMQ by invoking the ``Queue.Declare`` RPC command.
   When it is completed, the ``on_queue_declare_ok`` method will be
   invoked by ``pika``.
-  ``start``: inherited from ``threading.Thread``. It launches the
   internal thread with method ``run`` as target.
-  ``start_consuming``: this method sets up the consumer by first
   registering the ``on_consumer_cancelled`` callback, so that the
   client is notified if RabbitMQ cancels the consumer. It then issues
   the ``Basic.Consume`` RPC command which returns the consumer tag that
   is used to uniquely identify the consumer with RabbitMQ. The
   ``on_response`` method is passed in as a callback ``pika`` will
   invoke when a message is fully received.
-  ``stop``: this method cleanly shutdown the connection to RabbitMQ by
   calling the ``stop_consuming`` method. The ``IOLoop`` is started
   again because this method is invoked by raising a ``TerminateSignal``
   exception. This exception stops the ``IOLoop`` which needs to be
   running for ``pika`` to communicate with RabbitMQ. All of the
   commands issued prior to starting the ``IOLoop`` will be buffered but
   not processed.
-  ``stop_consuming``: this method sends a ``Basic.Cancel`` RPC command.
   When RabbitMQ confirms the cancellation, the ``on_cancel_ok``
   callback methods will be invoked by ``pika``, which will then close
   the channel and connection.
-  ``wait_for_acks``: this method is invoked by every write methods in
   the ``ProvenanceStorageInterface``, after sending a series of write
   requests to the server. It will call ``wait_for_response`` until it
   receives all expected ack responses, or until it receives the first
   timeout. The timeout is calculated based on the number of expected
   acks and class initialization parameters ``wait_per_batch`` and
   ``wait_min``.
-  ``wait_for_response``: this method is called from ``wait_for_acks``
   to retrieve ack packages from the internal ``_response_queue``. The
   response correlation ID is used to validate the received ack
   corresponds to the current write request. The method returns the
   decoded body of the received response.

ProvenanceStorageRabbitMQServer
===============================

The remote storage server is responsible for defining the ID range
splitting policy for each entity and relation in the provenance
solution. Based on this policy, it will launch a series of worker
processes that will take care of actually processing the requests for
each defined range in the partition. These processes are implemented in
the ``ProvenanceStorageRabbitMQWorker`` described below.

To initialize a server object it is required to provide two mandatory
parameters:

-  ``url``: the URL string of the broker where the server expects to
   receive the packages.
-  ``storage_config``: a dictionary containing the storage configuration
   for the local storage object, as expected by
   ``swh.provenance.get_provenance_storage``.

Additionally, some optional parameter can be specified that may affect
the performance of the remote storage as a whole:

-  ``batch_size``: an integer specifying the maximum allowed amount of
   elements to be processed at a time, ie. forwarded to the underlying
   storage object by each worker. Default to 100.
-  ``prefetch_count``: an integer specifying how many packages are
   prefetched from the broker by each worker. Default to 100.

On initialization, the server will create the necessary
``ProvenanceStorageRabbitMQWorker``, forwarding to them the parameters
mentioned above, but it won’t launch these underlying processes until it
is explicitly started. To that end, the
``ProvenanceStorageRabbitMQServer`` objects provide the following
methods: - ``start``: this method launches all the necessary worker
subprocesses and ensures they all are in a proper consuming state before
returning control to the caller. - ``stop``: this method signals all
worker subprocesses for termination and blocks until they all finish
successfully.

ID range splitting policy:
--------------------------

The ID range splitting policy is defined as follows

-  There is an exchange in the RabbitMQ broker for each entity in the
   provenance solution, plus an extra one to handle locations:
   ``content``, ``directory``, ``location``, ``origin``, and
   ``revision``.
-  Any request to add an entity should send the necessary packages to
   the entity’s associated exchange for proper routing: ie. requests for
   ``content_add`` will be handled by the ``content`` exchange.
-  Any request to add a relation entry is handled by the exchange of the
   source entity in the relation: ie. requests for ``relation_add`` with
   ``relation=CNT_EARLY_IN_REV`` will be handled by the ``content``
   exchange.
-  ID range splitting is done by looking at the first byte in the SWHID
   of the entity (ie. a hex value), hence 16 possible ranges are defined
   for each operation associated to each entity. In the case of
   locations, a hex hash is calculated over its value.
-  Each exchange then handles 16 queues for each method associated to
   the exchange’s entity, with a ``direct`` routing policy. For
   instance, the ``content`` exchange has 16 queues associated to each
   of the following methods: ``content_add``, ``relation_add`` with
   ``relation=CNT_EARLY_IN_REV``, and ``relation_add`` with
   ``relation=CNT_IN_DIR`` (ie. a total of 48 queues).
-  For each exchange, 16 ``ProvenanceStorageRabbitMQWorker`` processes
   are launched, each of them taking care of one ID range for the
   associated entity.

All in all, this gives a total of 80 ``ProvenanceStorageRabbitMQWorker``
processes (16 per exchange) and 160 RabbitMQ queues (48 for ``content``
and ``revision``, 32 for ``directory``, and 16 for ``location`` and
``origin``). In this way, it is guaranteed that, regardless of the
operation being performed, there would never be more than one process
trying to write on a given ID range for a given entity. Thus, resolving
all potential conflicts.

Although the ID range splitting policy is defined on the server side, so
it can properly configure and launch the necessary worker processes, it
is the client the responsible for actually splitting the input to each
write method and send the write requests to the proper queues for
RabbitMQ route them to the correct worker. For that, the server defines
a series of static methods that allow to query the ID range splitting
policy:

-  ``get_binding_keys``: this method is meant to be used by the server
   workers. Given an exchange and a range ID, it yields all the RabbitMQ
   routing keys the worker process should bind to.
-  ``get_exchange``: given the name of a write method in the
   ``ProvenanceStorageInterface``, and an optional relation type, it
   return the name of the exchange to which the writing request should
   be sent.
-  ``get_exchanges``: this method yields the names of all the exchanges
   in the RabbitMQ broker.
-  ``get_meth_name``: this method is meant to be used by the server
   workers. Given a binding key as returned by ``get_binding_keys``, it
   return the ``ProvenanceStorageInterface`` method associated to it.
-  ``get_meth_names``: given an exchange name, it yields all the methods
   that are associated to it. In case of ``relation_add``, the method
   also returns the supported relation type.
-  ``get_ranges``: given an exchange name, it yields the integer value
   of all supported ranges IDs (currently 0-15 for all exchanges). The
   idea behind this method is to allow defining a custom ID range split
   for each exchange.
-  ``get_routing_key``: given the name of a write method in the
   ``ProvenanceStorageInterface``, an optional relation type, and a
   tuple with the data to be passed to the method (first parameter),
   this method returns the routing key of the queue responsible to
   handle that tuple. It is assumed that the first value in the tuple is
   a ``Sha1Git`` ID.
-  ``is_write_method``: given the name of a method in the
   ``ProvenanceStorageInterface``, it decides if it is a write method or
   not.

ProvenanceStorageRabbitMQWorker
===============================

The remote storage worker consume messages published in the RabbitMQ
broker by the remote storage clients, and proceed to perform the actual
writing operations to a local storage object (ie.
``ProvenanceStorageMongoDb`` or ``ProvenanceStoragePostgreSql``). Each
worker process messages associated to a particular entity and range ID.
It is the client’s responsibility to properly split data along messages
according to the remote storage server policy.

Since there is overlapping between methods in the
``ProvenanceInterface`` operating over the same entity, one worker may
have to handle more than one method to guarantee conflict-free writings
to the underlying storage. For instance, consider the ``content``
entity, for a given ID range, methods ``content_add`` and
``relation_add`` with ``relation=CNT_EARLY_IN_REV`` may conflict. Is the
worker’s responsibility to solve this kind of conflicts.

To initialize a server object it is required to provide two mandatory
parameters:

-  ``url``: the URL string of the broker where the server expects to
   receive the packages.
-  ``exchange``: the RabbitMQ exchange to which the worker will
   subscribe. See ``ProvenanceStorageRabbitMQServer``\ ’s ID range
   splitting policy for further details.
-  ``range``: the range ID the worker will be processing. See
   ``ProvenanceStorageRabbitMQServer``\ ’s ID range splitting policy for
   further details.
-  ``storage_config``: a dictionary containing the storage configuration
   for the local storage object, as expected by
   ``swh.provenance.get_provenance_storage``.

Additionally, some optional parameter can be specified that may affect
the performance of the remote storage as a whole:

-  ``batch_size``: an integer specifying the maximum allowed amount of
   elements to be processed at a time, ie. forwarded to the underlying
   storage object for writing. Default to 100.
-  ``prefetch_count``: an integer specifying how many packages are
   prefetched from the broker. Default to 100.

.. warning::

   This class is not meant to be used directly but through an instance
   of ``ProvenanceStorageRabbitMQServer``. The parameters ``url``,
   ``storage_config``, ``batch_size`` and ``prefetch_count`` above are
   forwarded as passed to the server on initialization. Additional
   arguments ``exchange`` and ``range`` are generated by the server
   based on its ID range splitting policy.

Worker lifecycle
----------------

All interaction between the provenance solution and a remote storage
worker object happens through RabbitMQ packages. To maximize
concurrency, each instance of the ``ProvenanceStorageRabbitMQWorker``
launches a distinct sub-process, hence avoiding unnecessary
synchronization with other components on the solution. For this,
``ProvenanceStorageRabbitMQWorker`` extends ``multiprocessing.Process``
and only has a direct channel of communication to the master
``ProvenanceStorageRabbitMQServer``, through ``multiprocessing.Queue``
structures. Then, the entry point of the sub-process is the method
``run`` which will in-turn launch a bunch of threads to handle the
different provenance methods the worker needs to support, and an extra
thread to handle communication with the server object. RabbitMQ’s
lifecycle will be taken care of in the main thread.

The following is a diagram of the interaction between the methods of the
class:

.. graphviz::

   digraph {
     ProvenanceStorageRabbitMQServer
     ProvenanceStorageRabbitMQWorker

     start[shape=record]
     run[shape=record]

     connect[shape=record]
     on_connection_open[shape=record]
     on_connection_open_error[shape=record]
     on_connection_closed[shape=record]
     close_connection[shape=record]
     open_channel[shape=record]
     on_channel_open[shape=record]
     on_channel_closed[shape=record]

     setup_exchange[shape=record]
     on_exchange_declare_ok[shape=record]

     setup_queues[shape=record]
     on_queue_declare_ok[shape=record]
     on_bind_ok[shape=record]
     on_basic_qos_ok[shape=record]
     start_consuming[shape=record]
     on_consumer_cancelled[shape=record]
     on_request[shape=record]
     stop_consuming[shape=record]
     on_cancel_ok[shape=record]

     request_termination[shape=record]
     stop[shape=record]

     respond[shape=record]
     get_conflicts_func[shape=record]

     subgraph cluster_command_thread {
       style=rounded
       bgcolor=gray95
       color=gray
       labelloc=b

       run_command_thread[shape=record]
     }

     subgraph cluster_request_thread {
       style=rounded
       bgcolor=gray95
       color=gray
       labelloc=b

       ProvenanceStorageInterface

       run_request_thread[shape=record]
     }

     ProvenanceStorageRabbitMQWorker->{start}

     start->{run}
     stop->{stop_consuming}

     run->{connect,run_command_thread,run_request_thread,stop}

     connect->{on_connection_open,on_connection_open_error,on_connection_closed}

     on_connection_open->{open_channel}

     open_channel->{on_channel_open}

     on_cancel_ok->{on_channel_closed}
     on_consumer_cancelled->{on_channel_closed}
     on_channel_open->{setup_exchange}

     on_channel_closed->{close_connection}

     setup_exchange->{on_exchange_declare_ok}
     on_exchange_declare_ok->{setup_queues}

     setup_queues->{on_queue_declare_ok}
     on_queue_declare_ok->{on_bind_ok}
     on_bind_ok->{on_basic_qos_ok}
     on_basic_qos_ok->{start_consuming}

     start_consuming->{on_consumer_cancelled,on_request}
     start_consuming->{ProvenanceStorageRabbitMQServer}[label="  signal",arrowhead="none"]

     on_request->{run_request_thread}[label="   _request_queues",arrowhead="none"]

     stop_consuming->{on_cancel_ok}

     run_command_thread->{request_termination}
     run_command_thread->{ProvenanceStorageRabbitMQServer}[label="  command",arrowhead="none"]

     run_request_thread->{ProvenanceStorageInterface,get_conflicts_func,respond,request_termination}

     request_termination->{run}[label="TerminateSignal",arrowhead="none"]
   }

There is a request thread for each ``ProvenanceStorageInterface`` method
the worker needs to handle, each thread with its own provenance storage
object (ie. exclusive connection). Each of these threads will receive
sets of parameters to be passed to their correspondent method and
perform explicit conflict resolution by using the method-specific
function returned by ``get_conflicts_func``, prior to passing these
parameters to the underlying storage.

Below there is a summary of the methods present in the diagram and their
functionality. Methods are listed in alphabetic order:

-  ``close_connection``: this method properly closes the connection to
   RabbitMQ.
-  ``connect``: this method connects to RabbitMQ, returning the
   connection handle. When the connection is established, the
   ``on_connection_open`` callback method will be invoked by ``pika``.
   If there is an error establishing the connection, the
   ``on_connection_open_error`` callback method will be invoked by
   ``pika``. If the connection closes unexpectedly, the
   ``on_connection_closed`` callback method will be invoked by ``pika``.
-  ``on_basic_qos_ok``: this callback method is invoked by ``pika`` when
   the ``Basic.QoS`` RPC call made in ``on_bind_ok`` has completed. At
   this point it is safe to start consuming messages, hence the
   ``start_consuming`` method is called, which will invoke the needed
   RPC commands to start the process.
-  ``on_bind_ok``:this callback method is invoked by ``pika`` when the
   ``Queue.Bind`` RPC call made in ``on_queue_declare_ok`` has
   completed. This method sets up the consumer prefetch count by
   invoking the ``Basic.QoS`` RPC command. When it is completed, the
   ``on_basic_qos_ok`` method will be invoked by ``pika``.
-  ``on_cancel_ok``: this method is invoked by ``pika`` when RabbitMQ
   acknowledges the cancellation of a consumer. At this point the
   channel close is requested, thus indirectly invoking the
   ``on_channel_closed`` callback method once the channel has been
   closed, which will in-turn close the connection.
-  ``on_channel_closed``: this callback method is invoked by ``pika``
   when RabbitMQ unexpectedly closes the channel. Channels are usually
   closed if there is an attempt to do something that violates the
   protocol, such as re-declare an exchange or queue with different
   parameters. In this case, the connection will be closed by invoking
   the ``close_connection``.
-  ``on_channel_open``: this callback method is invoked by ``pika`` when
   the channel has been opened. The ``on_channel_closed`` callback is
   set here. Since the channel is now open, it is safe to declare the
   exchange to use by invoking the ``setup_exchange`` method.
-  ``on_connection_closed``: this callback method is invoked by ``pika``
   when the connection to RabbitMQ is closed unexpectedly. Since it is
   unexpected, an attempt to reconnect to RabbitMQ will be done.
-  ``on_connection_open``: this callback method is called by ``pika``
   once the connection to RabbitMQ has been established. It proceeds to
   open the channel by calling the ``open_channel`` method.
-  ``on_connection_open_error``: this callback method is called by
   ``pika`` if the connection to RabbitMQ can’t be established. Since it
   is unexpected, an attempt to reconnect to RabbitMQ will be done.
-  ``on_consumer_cancelled``: this callback method is invoked by
   ``pika`` when RabbitMQ sends a ``Basic.Cancel`` for a consumer
   receiving messages. At this point the channel close is requested,
   thus indirectly invoking the ``on_channel_closed`` callback method
   once the channel has been closed, which will in-turn close the
   connection.
-  ``on_exchange_declare_ok``: this callback methods is invoked by
   ``pika`` when the ``Exchange.Declare`` RPC call made in
   ``setup_exchange`` has completed. At this point it is time to set up
   the queues for the different request handling threads. This is done
   by calling the ``setup_queues`` method.
-  ``on_queue_declare_ok``: this callback method is invoked by ``pika``
   when each ``Queue.Declare`` RPC call made in ``setup_queues`` has
   completed. Now it is time to bind the current queue and exchange
   together with the correspondent routing key by issuing the
   ``Queue.Bind`` RPC command. When this command is completed, the
   ``on_bind_ok`` method will be invoked by ``pika``.
-  ``on_request``: this callback method is invoked by ``pika`` when a
   message is delivered from RabbitMQ to any of the queues bound by the
   worker. The decoded request together with its correlation ID and
   reply-to property are enqueued in the correspondent internal
   ``_request_queues`` (the actual queue it identified by the message
   routing key), so that the data is forwarded to the thread that
   handles the particular method the message is associated to. A
   ``Basic.Ack`` RPC command is issued to acknowledge the delivery of
   the message to RabbitMQ.
-  ``open_channel``: this method opens a new channel with RabbitMQ by
   issuing the ``Channel.Open`` RPC command. When RabbitMQ responds that
   the channel is open, the ``on_channel_open`` callback will be invoked
   by ``pika``.
-  ``request_termination``: this method send a signal to the main thread
   of the process to cleanly release resources and terminate. This is
   done by setting a callback in the ``IOLoop`` that raises a
   ``TerminateSignal``, which will eventually be handled in by ``run``
   method.
-  ``respond``: this methods send a message to RabbitMQ by issuing a
   ``Basic.Publish`` RPC command. The body of the message is properly
   encoded, while correlation ID and request key are used as passed by
   the calling method.
-  ``run``: main method of the process. It launches a thread to handle
   communication with the ``ProvenanceStorageRabbitMQServer`` object,
   targeting the ``run_command_thread`` method. Also, it launches on
   thread for each ``ProvenanceStorageInterface`` method the worker
   needs to handle, each targeting the ``run_request_thread`` method.
   Finally, it requests to open a connection to RabbitMQ by calling the
   ``connect`` method, and starts the internal ``IOLoop`` of the
   returned handle (blocking operation). In case of failure, this method
   will indefinitely try to reconnect. When an explicit
   ``TerminateSignal`` is received, the ``stop`` method is invoked. All
   internal thread will be signalled for termination as well, and
   resources released.
-  ``run_command_thread``: main method of the command thread. It
   received external commands from the
   ``ProvenanceStorageRabbitMQServer`` object through a
   ``multiprocessing.Queue`` structure. The only supported command for
   now is for the worker to be signalled to terminate, in which case the
   ``request_termination`` method is invoked.
-  ``run_request_thread``: main method of the request threads. The
   worker has one such thread per ``ProvenanceStorageInterface`` method
   it needs to handle. This method will initialize its own storage
   object and interact with the ``on_request`` through a ``queue.Queue``
   structure, waiting for items to be passed to the storage method
   associated with the thread. When elements become available, it will
   perform a conflict resolution step by resorting to the
   method-specific function returned by ``get_conflicts_func``. After
   this it will forward the items to the underlying storage object. If
   the storage method returns successfully, acknowledgements are sent
   back to each client by through ``respond`` method. If the call to the
   storage method fails, items will be enqueued back to retry in a
   future iteration. In case of an unexpected exception, the worker is
   signalled for termination by calling the ``request_termination``
   method.
-  ``setup_exchange``: this method sets up the exchange on RabbitMQ by
   invoking the ``Exchange.Declare`` RPC command. When it is completed,
   the ``on_exchange_declare_ok`` method will be invoked by ``pika``.
-  ``setup_queues``: this methods sets up the necessary queues for the
   worker on RabbitMQ by invoking several ``Queue.Declare`` RPC commands
   (one per supported provenance storage method). When each command is
   completed, the ``on_queue_declare_ok`` method will be invoked by
   ``pika``.
-  ``start``: inherited from ``multiprocessing.Process``. It launches
   the worker sub-process with method ``run`` as target.
-  ``start_consuming``: this method sets up the worker by first
   registering the ``add_on_cancel_callback`` callback, so that the
   object is notified if RabbitMQ cancels the consumer. It then issues
   one ``Basic.Consume`` RPC command per supported provenance storage
   method, which return the consumer tag that is used to uniquely
   identify the consumer with RabbitMQ. The ``on_request`` method is
   passed in as a callback ``pika`` will invoke when a message is fully
   received. After setting up all consumers, a ``CONSUMING`` signal is
   sent to the ``ProvenanceStorageRabbitMQServer`` object through a
   ``multiprocessing.Queue`` structure.
-  ``stop``: this method cleanly shutdown the connection to RabbitMQ by
   calling the ``stop_consuming`` method. The ``IOLoop`` is started
   again because this method is invoked by raising a ``TerminateSignal``
   exception. This exception stops the ``IOLoop`` which needs to be
   running for ``pika`` to communicate with RabbitMQ. All of the
   commands issued prior to starting the ``IOLoop`` will be buffered but
   not processed.
-  ``stop_consuming``: this method sends a ``Basic.Cancel`` RPC command
   for each supported provenance storage method. When RabbitMQ confirms
   the cancellation, the ``on_cancel_ok`` callback methods will be
   invoked by ``pika``, which will then close the channel and
   connection.
-  ``get_conflicts_func``: this method returns the conflict resolution
   function to be used based on the provenance storage method.
