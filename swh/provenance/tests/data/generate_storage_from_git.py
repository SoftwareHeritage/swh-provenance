# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone
import os
from subprocess import check_output
from typing import Dict, Optional

import click
import yaml

from swh.loader.git.from_disk import GitLoaderFromDisk
from swh.model.hashutil import hash_to_bytes
from swh.model.model import (
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Snapshot,
    SnapshotBranch,
    TargetType,
)
from swh.storage import get_storage
from swh.storage.interface import StorageInterface


def load_git_repo(
    url: str, directory: str, storage: StorageInterface
) -> Dict[str, str]:
    visit_date = datetime.now(tz=timezone.utc)
    loader = GitLoaderFromDisk(
        url=url,
        directory=directory,
        visit_date=visit_date,
        storage=storage,
    )
    return loader.load()


@click.command()
@click.option("-o", "--output", default=None, help="output file")
@click.option(
    "-v",
    "--visits",
    type=click.File(mode="rb"),
    default=None,
    help="additional visits to generate.",
)
@click.argument("git-repo", type=click.Path(exists=True, file_okay=False))
def main(output: Optional[str], visits: bytes, git_repo: str) -> None:
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

        if visits:
            # retrieve all branches from the actual git repo
            all_branches = {
                ref: sha1
                for sha1, ref in (
                    line.strip().split()
                    for line in check_output(["git", "-C", git_repo, "show-ref"])
                    .decode()
                    .splitlines()
                )
            }

            for visit in yaml.full_load(visits):
                # add the origin (if it already exists, this is a noop)
                sto.origin_add([Origin(url=visit["origin"])])
                # add a new visit for this origin
                visit_id = list(
                    sto.origin_visit_add(
                        [
                            OriginVisit(
                                origin=visit["origin"],
                                date=datetime.fromtimestamp(
                                    visit["date"], tz=timezone.utc
                                ),
                                type="git",
                            )
                        ]
                    )
                )[0].visit
                assert visit_id is not None
                # add a snapshot with branches from the input file
                branches = {
                    f"refs/heads/{name}".encode(): SnapshotBranch(
                        target=hash_to_bytes(all_branches[f"refs/heads/{name}"]),
                        target_type=TargetType.REVISION,
                    )
                    for name in visit["branches"]
                }
                snap = Snapshot(branches=branches)
                sto.snapshot_add([snap])
                # add a "closing" origin visit status update referencing the snapshot
                status = OriginVisitStatus(
                    origin=visit["origin"],
                    visit=visit_id,
                    date=datetime.fromtimestamp(visit["date"], tz=timezone.utc),
                    status="full",
                    snapshot=snap.id,
                )
                sto.origin_visit_status_add([status])

    click.echo(f"Serialized the storage made from {reponame} in {output}")


if __name__ == "__main__":
    main()
