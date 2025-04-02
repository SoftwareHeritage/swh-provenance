.. _swh-provenance-grpc-api:

==================
Using the gRPC API
==================

.. highlight:: console

The gRPC API is the core API used to query the provenance database remotely. It uses the
`gRPC framework <https://grpc.io/>`_ to provide high-performance provenance answers
with server streaming.

Quickstart
==========

Building the server
-------------------

Get Rust >= 1.79, eg. with `rustup <https://rustup.rs/>`_.

Run::

    RUSTFLAGS="-C target-cpu=native" cargo install --locked https://gitlab.softwareheritage.org/swh/devel/swh-provenance.git

Or::

    git clone https://gitlab.softwareheritage.org/swh/devel/swh-provenance.git
    cd swh-provenance
    cargo build --release

Getting a provenance database
-----------------------------

::

    pip3 install awscli
    aws s3 cp --no-sign-request --recursive s3://softwareheritage/derived_datasets/2024-12-06/provenance/all/ provenance-2024-12-06/

You also need a local graph. Either use ``swh graph download`` to download a full graph, or get
only the minimal set of required files with::

    aws s3 cp --no-sign-request s3://softwareheritage/graph/2024-12-06/compressed/graph.nodes.count.txt graph-2024-12-06/
    aws s3 cp --no-sign-request s3://softwareheritage/graph/2024-12-06/compressed/graph.pthash graph-2024-12-06/
    aws s3 cp --no-sign-request s3://softwareheritage/graph/2024-12-06/compressed/graph.pthash.order graph-2024-12-06/
    aws s3 cp --no-sign-request s3://softwareheritage/graph/2024-12-06/compressed/graph.node2swhid.bin.zst graph-2024-12-06/
    aws s3 cp --no-sign-request s3://softwareheritage/graph/2024-12-06/compressed/graph.node2type.bin.zst graph-2024-12-06/
    cd graph-2024-12-06/
    unzstd graph.node2swhid.bin.zst graph.node2type.bin.zst


Starting the server
-------------------

Before the first start, you need to build database indexes::

    $ swh-provenance-index --database file:///provenance-2024-12-06/ --indexes provenance-2024-12-06-indexes/

Or, if you installed from Git::

    $ cargo run --release --bin swh-provenance-index -- --database file:///provenance-2024-12-06/ --indexes provenance-2024-12-06-indexes/

The gRPC server is automatically started on port 50091 when the HTTP server
is started with::

    $ swh-provenance-grpc-serve --graph graph-2024-12-06/ --database file:///provenance-2024-12-06/ --indexes provenance-2024-12-06-indexes/

Or, if you installed from Git::

    $ cargo run --release --bin swh-graph-grpc-serve -- --graph graph-2024-12-06/ --database file:///provenance-2024-12-06/ --indexes provenance-2024-12-06-indexes/



Running queries
---------------

The `gRPC command line tool
<https://github.com/grpc/grpc/blob/master/doc/command_line_tool.md>`_
can be an easy way to query the gRPC API from the command line. It is
invoked with the ``grpc_cli`` command. Of course, it is also possible to use
a generated RPC client in any programming language supported by gRPC.

All RPC methods are defined in the service ``swh.provenance.ProvenanceService``.
The available endpoints can be listed with ``ls``::

    $ rpc_cli ls localhost:50141 swh.provenance.ProvenanceService
    WhereIsOne
    WhereAreOne

A RPC method can be called with the ``call`` subcommand.::

    $ grpc_cli call localhost:50141 swh.provenance.ProvenanceService.WhereIsOne "swhid: 'swh:1:cnt:27766b99cdcab4e9b68501c3b50f1712e016c945'"
    swhid: "swh:1:cnt:27766b99cdcab4e9b68501c3b50f1712e016c945"
    anchor: "swh:1:rev:1564a9e70426251655286156957f8d710f0db278"

