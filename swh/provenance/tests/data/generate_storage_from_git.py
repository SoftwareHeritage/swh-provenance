# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone
import os
import re
from subprocess import check_output
from typing import Dict

import click

from swh.core.api.serializers import msgpack_dumps
from swh.loader.git.from_disk import GitLoaderFromDisk
from swh.model.hashutil import hash_to_bytes as h2b
from swh.provenance.tests.test_provenance_db import ts2dt
from swh.storage import get_storage


def load_git_repo(url, directory, storage):
    visit_date = datetime.now(tz=timezone.utc)
    loader = GitLoaderFromDisk(
        url=url,
        directory=directory,
        visit_date=visit_date,
        storage=storage,
    )
    return loader.load()


def pop_key(d, k):
    d.pop(k)
    return d


def dump_file(hash, storage, cache):
    if hash not in cache:
        content = storage.content_find({"sha1_git": hash})[0]
        cache[hash] = content
        # we remove ctime to make the resulting data (eg. output msgpack file)
        # independent from execution time
        yield "content", pop_key(content.to_dict(), "ctime")


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
    default="master",
    help="head revision to start from",
)
@click.option("-o", "--output", default=None, help="output file")
@click.argument("git-repo")
def main(head, output, git_repo):
    "simple tool to generate the git_repo.msgpack dataset file used in some tests"
    sto = get_storage(cls="memory")
    if git_repo.endswith("/"):
        git_repo = git_repo[:-1]

    reponame = os.path.basename(git_repo)
    load_git_repo(f"https://{reponame}", git_repo, sto)

    if output is None:
        output = f"{git_repo}.msgpack"

    if not re.match("[0-9a-fA-F]{40}", head):
        headhash = (
            check_output(["git", "-C", git_repo, "rev-parse", head]).decode().strip()
        )
        click.echo(f"Revision hash for {head} is {headhash}")
    else:
        headhash = head
    cache: Dict[bytes, dict] = {}
    outf = open(output, "wb")
    outd = []
    for e in dump_git_revision(h2b(headhash), storage=sto, cache=cache):
        outd.append(e)
    outf.write(msgpack_dumps(outd))
    click.echo(f"Wrote {len(outd)} objects in {output}")


if __name__ == "__main__":
    main()
