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
(swh-graph) and a preprocessed provenance index to speed up some of the
provenance queries.


Description
===========

The core feature of this tool is to provide a service to the reference to an
object within the Software Heritage Archive where the queried object can be
found.

There are mostly 2 kinds of provenance queries that can be done:
- search for the best provenance answer from a given object;
- search for all the possible provenance answers for a given object.

For each input object, the definition of "best provenance answer" is simple and
unambiguous; for now, the best answer is the *an* origin in which the oldest
revision (in the sense of the revision with the oldest commit date) in which
this object has been found.

Provenance can be looked for:

- `Content`
- `Directory`
- `Revision`
- `Release`

For each object:

- Input: SWHID (core SWHID of an artifact found in the user code base)
- Output: SWHID or origin URI where input SWHID was found + context information,
  a subset of:
    - snapshot (snp SWHID)
    - release (rel)
    - revision (rev)
    - path (filesystem-style path)
- Non-functional requirements:
    - the returned object should be as high as possible; i.e. prefer an
      Origin (if any), then a Snapshot, then a Release, then a Revision,
    - the returned object should be the best possible answer, if possible;
      the definition of "best answer" being something like:
      "*an* origin in which the oldest revision (in the sense of the
      revision with the oldest commit date) in which this object has been
      found."

This documents the backend provenance service; it is not meant to be
used directly but rather via the Public API; please refer to its
[description](https://archive.softwareheritage.org/api/1/) for more
details on how to use the Provenance public API.
