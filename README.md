Provenance compact model
========================

Prototype for the provenance compact model database.

Setup
-----

The solution works within the SWH environment, using its API to retrieve
information from the archive. It assumes the SHW modules are installed and
accessible, as well as a VPN connection to query the archive directly.


Usage
-----

The solution consists of a simple command-line utility to query the
compact model database and, eventually, reconstructing it.

To simply query an existing compact model database, the utility just requires
the connection credentials to it to be provided on a separate configuration
file, and the amount of content blobs to be retrieved from the database (it
retrieves them ordered by SWHID). The configuration content sections like:

```
[db_name]
host=localhost
database=db_name
user=postgres
password=postgres
```

Then the command-line utility can simply be invoked like this:

```
(swh) user@host:~/compact -c conf_file db_name count
```

where `confile` is the path to the configuration file, `db_name` is the name of
the section containing the connection information (it should match the database
actual name), and `count` is the amount of content blobs to be retrieved.


For reconstructing the database it also requires a source of revisions to
iterate. There are two option for specifying it: (1) giving a connection to the
archive that will be queried using the provided API, (2) giving a local comma
separated values file where each row consists of the revision's SWHID, its
timestamp and its root directory SWHID.

In the first case the command-line utility can simply be invoked with the `-d`
option followed by the configuration file for the connection to the archive and
the database name just as for the `-c` option:

```
(swh) user@host:~/compact -c conf_file db_name -d conf_file db_name count
```

In the second case the option `-f` should be used, followed by the path to the
CSV file to be read.

```
(swh) user@host:~/compact -c conf_file db_name -f csv_file count
```

Both alternatives support the `-l` option, to limit the amount of revision to
be considered for reconstruction the compact model database.

WARNING: when reconstructing the database any previous content of the provided
compact model database is erased!


Algorithm
---------

The algorithm for constructing the compact model database is based on that
described in [1]. It iterates over a list of revisions assuming chronological
order and building and isochrone graph for each element in the list. Every time
a directory is found in the isochrone frontier of the current revision, the
exploration is aborted given that all the elements is that branch are already
known. Whenever a revision comes out-of-order, elements already known from a
previously computed isochrone graph are treated as unknown.


Structure of the code
---------------------

The previously described algorithm is implemented in `compact.py`. It is
actually done as a recursive walk over the directories of a given revision.
The module `iterator.py` contains utilities to properly iterate over revisions
coming both from a remote archive database or from a local CSV file. The module
`model.py` implements an iterator for directory entries that retrieves only the
necessary information from the archive and prevents multiple queries over the
same directory.


[1] Guillaume Rousseau, Roberto Di Cosmo, Stefano Zacchiroli. Software
Provenance Tracking at the Scale of Public Source Code. In Empirical Software
Engineering, volume 25, issue 4, pp. 2930-2959. ISSN 1382-3256, Springer. 2020.
