# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Generator, Iterable, Iterator, List, Optional

from swh.core.statsd import statsd
from swh.model.model import Sha1Git

from .archive import ArchiveInterface
from .interface import ProvenanceInterface
from .model import DirectoryEntry

REVISION_DURATION_METRIC = "swh_provenance_directory_duration_seconds"


class CSVDirectoryIterator:
    """Iterator over directories typically present in the given CSV file.

    The input is an iterator that produces ids (sha1_git) of directories
    """

    def __init__(
        self,
        directories: Iterable[Sha1Git],
        limit: Optional[int] = None,
    ) -> None:
        self.directories: Iterator[Sha1Git]
        if limit is not None:
            from itertools import islice

            self.directories = islice(directories, limit)
        else:
            self.directories = iter(directories)

    def __iter__(self) -> Generator[DirectoryEntry, None, None]:
        for id in self.directories:
            yield DirectoryEntry(id)


@statsd.timed(metric=REVISION_DURATION_METRIC, tags={"method": "main"})
def directory_add(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    directories: List[DirectoryEntry],
    minsize: int = 0,
    commit: bool = True,
) -> None:
    for directory in directories:
        # Only flatten directories that are present in the provenance model, but not
        # flattenned yet.
        flattenned = provenance.directory_already_flattenned(directory)
        if flattenned is not None and not flattenned:
            directory_flatten(
                provenance,
                archive,
                directory,
                minsize=minsize,
            )
    if commit:
        provenance.flush()


@statsd.timed(metric=REVISION_DURATION_METRIC, tags={"method": "flatten"})
def directory_flatten(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    directory: DirectoryEntry,
    minsize: int = 0,
) -> None:
    """Recursively retrieve all the files of 'directory' and insert them in the
    'provenance' database in the 'content_to_directory' table.
    """
    stack = [(directory, b"")]
    while stack:
        current, prefix = stack.pop()
        current.retrieve_children(archive, minsize=minsize)
        for f_child in current.files:
            # Add content to the directory with the computed prefix.
            provenance.content_add_to_directory(directory, f_child, prefix)
        for d_child in current.dirs:
            # Recursively walk the child directory.
            stack.append((d_child, os.path.join(prefix, d_child.name)))
    provenance.directory_flag_as_flattenned(directory)
