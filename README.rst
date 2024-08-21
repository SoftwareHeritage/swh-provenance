Software Heritage - Provenance
==============================

This service provide a provenance query service for the Software Heritage
Archive. Provenance is the ability to ask for a given object stored in the
Archive: "where does it come from?"

This question generally does not have a simple and unambiguous answer. It can
be, among other:

- what it the oldest revision in which this object has been found?
- what is the "better" origin in which this object can be found?

Answering this kind of question requires querying the Merkle DAG on which the
Software Heritage Archive is built with complex queries, mostly from the bottom
to the top (aka from Content to Origin objects).

The idea is to use both the compressed graph representation of the Archive
(swh-graph) and a preprocessed provennce index to speed up some of the
provenance queries.


API Description
===============

For a single object::

    Input: SWHID (core SWHID of an artifact found in the use code base)

    Output: SWHID or origin URI where input SWHID was found + context information
        Context information, a subset of:
            snapshot (snp SWHID)
            release (rel)
            revision (rev)
            path (filesystem-style path)

    Non-functional requirements: TODO something about the fact that both the
    answer and the context information should be "as high as possible" in the
    graph


Public API
----------

::

    GET /whereis/:swhid

    GET /whereis_all/

    POST /whereare/TODO
      :swhids
