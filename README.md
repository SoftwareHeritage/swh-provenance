swh-provenance
==============

Provenance DB module to query the provenance of source code artifacts present
in the Software Heritage archive.

This project allows to build such a provenance db from the Software Heritage
Archive, and query this database.

## Building a provenance database

Building the provenance database requires a read access to the Software
Heritage archive, either via a direct access to the database (preferred for
better performances), or using the RPC API to a Software Heritage Storage
instance.

It also need a postgresql database in which the provenance db will be written
into.

A configuration file is needed with with the access to both these databases:

```
archive:
  cls: api
  storage:
      cls: remote
      url: http://uffizi.internal.softwareheritage.org:5002

provenance:
  cls: direct
  db:
    dbname: provenance
    host: localhost


```

Running in Docker
-----------------

### Build the image
```
docker build -t swh-provenance .
```

### Run the services
```
docker-compose up -d
docker-compose logs -f
```
