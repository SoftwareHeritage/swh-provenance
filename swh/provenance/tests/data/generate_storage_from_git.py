# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone
import os

import click

from swh.loader.git.from_disk import GitLoaderFromDisk
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


@click.command()
@click.option("-o", "--output", default=None, help="output file")
@click.argument("git-repo", type=click.Path(exists=True, file_okay=False))
def main(output, git_repo):
    "simple tool to generate the git_repo.msgpack dataset file used in some tests"
    if output is None:
        output = f"{git_repo}.msgpack"
    with open(output, "wb") as outstream:
        sto = get_storage(
            cls="memory", journal_writer={"cls": "stream", "output_stream": outstream}
        )
        if git_repo.endswith("/"):
            git_repo = git_repo[:-1]

        reponame = os.path.basename(git_repo)
        load_git_repo(f"https://{reponame}", git_repo, sto)
    click.echo(f"Serialized the storage made from {reponame} in {output}")


if __name__ == "__main__":
    main()
