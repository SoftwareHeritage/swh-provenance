.. _swh-provenance-design:

=====================
swh-provenance design
=====================

The provenance database is created read-only by Rust scripts, which takes about a day on the production graph.
Their output is a bunch of `Parquet https://parquet.apache.org/`_ tables
(a Parquet table being a directory that contains Parquet files that all have the same schema).

Database format and layout
==========================

While Parquet is design for analytics workloads (ie. reading and aggregating values from every or most rows),
careful layout of Parquet files and `a custom reader library <https://crates.io/crates/parquet_aramid>`_
allow point queries (ie. reading a specific set of rows) in 100 or 200ms on the production graph.

This layout assumes a "primary key", which is a column that can be efficiently searched,
while searches through other columns potentially reading gigabytes of data only to be discarded.


Parquet primer
--------------

Rows in a Parquet file are split into row groups (a row group has in the order of a million rows).
Within a row group, all values of a specific column (called a chunk) are stored consecutively,
which allows compressing them together efficiently.
For example, this table::

    A1  B1  C1
    A2  B2  C2
    ⋮
    A10  B10  C10
    <next values>

Would be stored on disk as::

    [row group header] A1 A2 ... A10 B1 B2 ... B10 C1 C2 ... C10 [row group header] <next values>

Each row group header contains indexes that allows skipping the entire row group when looking for a specific value.
These indexes are, for each chunk:

* statistics: the minimum and maximum value of chunk
* a `Bloom Filter <https://en.wikipedia.org/wiki/Bloom_filter>`_ of values in that chunk

Additionally, chunks are split into pages (in the order of megabytes).
There is a page index that stores the minimum and maximum value of each page

All of these indexes can have false positives:
they can tell whether a value **may be** in the chunk or **is definitely not** in the chunk.


Indexes
-------

Bloom Filters are big, so we try not to rely on them in the Provenance database.
Instead, we write rows in Parquet files so that values of the primary key are mostly sorted.
This significantly reduces the risk of a false positive when using Statistics indexes (both row groups and pages).

Additionally, we store an `Elias-Fano <https://docs.rs/sux/latest/sux/dict/elias_fano/>`_ structure alongside
each file, listing all values the primary key takes in that file.
Due to sorting rows, this means that each value of the primary key is (usually) only in a single file.


Tables
------

Tables in the Provenance database are:

* ``nodes``, which maps between node ids and SWHIDs. This is used as a last resort, as it is bigger and less efficient than querying swh-graph. Each provenance database is generated from a specific version of swh-graph, and shares node ids with that version
* ``frontier_directories_in_revisions`` which, for a subset of directories called "frontier directories", lists all revisions these directories are in
* ``contents_in_frontier_directories`` which lists all "frontier directories" each content is in
* ``contents_in_revisions_without_frontiers`` which lists all revisions each content is reachable from without going through a frontier directory

Taken together (``(contents_in_frontier_directories INNER JOIN frontier_directories_in_revisions) UNION contents_in_revisions_without_frontiers``), the last three allow listing all revisions that each content is in.


Database construction
=====================

1. :command:`compute-earliest-timestamps` computes an array mapping content node ids to the timestamp of the earliest revision containing that content
   (ie. ``forall content, earliest_timestamp(content) = min_{forall revision containing the content} timestamp(revision)``).
   This is roughly the date the content appeared
2. :command:`list-directory-with-max-leaf-timestamp` computes the maximum earliest_timestamp of all contents it contains, recursively
   (ie. ``forall directory, max_leaf_timestamp(directory) = max_{forall content in directory} earliest_timestamp(content)``).
   This is a lower bound of the directory's creation date.
   FIXME: Actually, couldn't we use ``earliest_timestamp(directory)`` instead? this seems like a leftover from the initial design of Provenance.
3. :command:`compute-directory-frontier` computes a set of "frontier directories", which is a set of key directories,
   used to break the combinatorial explosion of `contents × revisions`, using the previous two arrays.
4. :command:`frontier-directories-in-revisions`, :command:`contents_in_revisions_without_frontiers`, and :command:`contents_in_frontier_directories` compute the final tables

Queries
=======

Querying is essentially a hand-written version of what a query engine like SparkSQL, DuckDB, or Datafusion would do on a query like
``(contents_in_frontier_directories INNER JOIN frontier_directories_in_revisions) UNION contents_in_revisions_without_frontiers``.

This may eventually be replaced by using Datafusion using custom table providers (to use our custom indexes and table layout) and join operators (optimized for low cardinality).
But Datafusion seemed badly suited for this kind of queries at the time we tried it,
especially as it did not support predicate pushdown
(ie. using native Parquet filters instead of filtering results after a scan) while doing a nested loop join.
