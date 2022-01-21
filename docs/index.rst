.. _swh-provenance:

Software Heritage Provenance
============================

A provenance index database based on the Software Heritage Archive. This is an
implementation of the paper `Software Provenance Tracking at the Scale of
Public Source Code`_ published in `Empirical Software Engineering`_

This provenance index database is a tool to help answering the question "where
does this source code artifact come from?", which the main Software Heritage
Archive cannot easily solve.


Quick Start
-----------

Database creation
~~~~~~~~~~~~~~~~~

Create a provenance index database (in this example we use pifpaf_ to easily
set up a test Postgresql database. Adapt the example below to your Postgresql
setup):

.. code-block:: shell

  eval $(pifpaf run postgresql)
  swh db create -d provdb provenance
  swh db init-admin -d provdb provenance
  swh db init -d provdb provenance

The provenance index DB comes in 2 feature flags, so there are 4 possible flavors. Feature flags are:

- ``with-path`` / ``without-path``: whether the provenance index database will store file path,
- ``normalized`` / ``denormalized``: whether or not the main relation tables are normalized (see below).

So the possible flavors are:

- ``with-path``
- ``without-path``
- ``with-path-denormalized``
- ``without-path-denormalized``

Filling the provenance index database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This step requires an access to the Software Heritage Archive to retrieve the
actual data from the Archive.

It currently also needs an input CSV file of revisions and origins to insert in
the provenance database.

Examples of such files are available in the `provenance public dataset`_.

.. _`provenance public dataset`: https://annex.softwareheritage.org/public/dataset/provenance

.. code-block:: shell

  wget https://annex.softwareheritage.org/public/dataset/provenance/sample_10k.csv.bz2
  bunzip2 sample_10k.csv.bz2

You need a configuration file, like:

.. code-block:: yaml

  # config.yaml
  provenance:
    storage:
      cls: postgresql
      db:
        host: /tmp/tmpifn2ov_j
        port: 9824
        dbname: provdb
    archive:
      cls: api
      storage:
        cls: remote
        url: http://storage:5002/

Note that you need access to the internal API of a :ref:`swh-storage
<swh-storage>` instance (here the machine named ``storage``) for this.

Then you can feed the provenance index database using:

.. code-block:: shell

  swh provenance -C config.yaml iter-revisions sample_10k.csv


This may take a while to complete.

Querying the provenance index database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using the same config file, you may look for the first known occurrence of a file content:

.. code-block:: shell

  swh provenance -C config.yaml find-first 8a54694c92c944fcb06d73c17743ac72444a5b72
  swh:1:cnt:8a54694c92c944fcb06d73c17743ac72444a5b72, swh:1:rev:6193fae0668d082d90207f6c9f33d6e8c98dd04a, 2008-10-06 18:32:23+00:00, None, lua/effects/bloodstream/init.lua


Or all the known occurrences:

.. code-block:: shell

  swh provenance -C config.yaml find-all 8a54694c92c944fcb06d73c17743ac72444a5b72
  swh:1:cnt:8a54694c92c944fcb06d73c17743ac72444a5b72, swh:1:rev:6193fae0668d082d90207f6c9f33d6e8c98dd04a, 2008-10-06 18:32:23+00:00, None, lua/effects/bloodstream/init.lua
  swh:1:cnt:8a54694c92c944fcb06d73c17743ac72444a5b72, swh:1:rev:f0a5078eed8808323b93ed09cddb003dbe2a85e4, 2008-10-06 18:32:23+00:00, None, trunk/lua/effects/bloodstream/init.lua
  [...]


(De)normalized database
-----------------------

For some relation tables (like the ``content_in_revision`` storing, for each
content object, in which revision it has been found), the default data schema
is to store one row for each relation.

For a big database, this can have a significant cost in terms of storage.

So it is possible to store these relations using an array as destination column
(the ``revision`` column in the case of the ``content_in_revisison`` table).

This can drastically reduce the database storage size, possibly at the price of
a slight performance hit.

Warning: the denormalized version of the database is still under test and
validation. Do not use for serious work.


.. _`Empirical Software Engineering`: http://link.springer.com/journal/10664
.. _`Software Provenance Tracking at the Scale of Public Source Code`: http://dx.doi.org/10.1007/s10664-020-09828-5
.. _pifpaf: https://github.com/jd/pifpaf

.. toctree::
   :maxdepth: 2
   :caption: Contents:


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
