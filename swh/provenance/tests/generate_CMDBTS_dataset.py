# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict

import click

from swh.core.api.serializers import msgpack_dumps
from swh.model.hashutil import hash_to_bytes as h2b
from swh.provenance.tests.test_provenance_db import ts2dt
from swh.storage import get_storage


def dump_file(hash, storage, cache):
    if hash not in cache:
        content = storage.content_find({"sha1_git": hash})[0]
        cache[hash] = content
        yield "content", content.to_dict()


def dump_directory(hash, storage, cache):
    if hash not in cache:
        dircontent = list(storage.directory_ls(hash))
        cache[hash] = dircontent
        yield "directory", {"id": hash, "entries": list(storage.directory_ls(hash))}
        for direntry in dircontent:
            if direntry["type"] == "dir":
                yield from dump_directory(direntry["target"], storage, cache)
            elif direntry["type"] == "file":
                yield from dump_file(direntry["target"], storage, cache)
            else:
                raise ValueError("Unexpected directory entry type {direntry['type']}")


def dump_git_revision(hash, storage, cache):
    if hash not in cache:
        rev = storage.revision_get([hash])[0]
        revd = {
            "id": rev.id,
            "date": ts2dt(rev.date.to_dict()),
            "parents": rev.parents,
            "directory": rev.directory,
        }
        revd = rev.to_dict()
        cache[hash] = revd
        for parent in rev.parents:
            yield from dump_git_revision(parent, storage, cache)
        yield from dump_directory(rev.directory, storage, cache)
    yield "revision", cache[hash]


@click.command()
@click.option(
    "-r",
    "--head",
    default="4c5551b4969eb2160824494d40b8e1f6187fc01e",
    help="head revision to start from",
)
@click.option("-o", "--output", default="data/CMDBTS.msgpack", help="output file")
@click.argument("storage-url")
def main(head, output, storage_url):
    "simple tool to generate the CMDBTS.msgpack dataset filed used in tests"
    sto = get_storage(cls="remote", url=storage_url)

    cache: Dict[bytes, dict] = {}
    outf = open(output, "wb")
    outd = []
    for e in dump_git_revision(h2b(head), storage=sto, cache=cache):
        outd.append(e)
    outf.write(msgpack_dumps(outd))
    click.echo(f"Wrote {len(outd)} objects in {output}")


if __name__ == "__main__":
    main()
