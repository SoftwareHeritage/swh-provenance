import logging

from .archive import ArchiveInterface
from .model import DirectoryEntry, FileEntry
from .origin import OriginEntry
from .revision import RevisionEntry

from datetime import datetime
from pathlib import PosixPath
from typing import Dict, List, Optional, Tuple

from swh.model.hashutil import hash_to_hex


class ProvenanceInterface:
    def __init__(self, **kwargs):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def content_add_to_directory(
        self, directory: DirectoryEntry, blob: FileEntry, prefix: PosixPath
    ):
        raise NotImplementedError

    def content_add_to_revision(
        self, revision: RevisionEntry, blob: FileEntry, prefix: PosixPath
    ):
        raise NotImplementedError

    def content_find_first(self, blobid: str):
        raise NotImplementedError

    def content_find_all(self, blobid: str):
        raise NotImplementedError

    def content_get_early_date(self, blob: FileEntry) -> datetime:
        raise NotImplementedError

    def content_get_early_dates(self, blobs: List[FileEntry]) -> Dict[bytes, datetime]:
        raise NotImplementedError

    def content_set_early_date(self, blob: FileEntry, date: datetime):
        raise NotImplementedError

    def directory_add_to_revision(
        self, revision: RevisionEntry, directory: DirectoryEntry, path: PosixPath
    ):
        raise NotImplementedError

    def directory_get_date_in_isochrone_frontier(
        self, directory: DirectoryEntry
    ) -> datetime:
        raise NotImplementedError

    def directory_get_early_dates(
        self, dirs: List[DirectoryEntry]
    ) -> Dict[bytes, datetime]:
        raise NotImplementedError

    def directory_set_date_in_isochrone_frontier(
        self, directory: DirectoryEntry, date: datetime
    ):
        raise NotImplementedError

    def origin_get_id(self, origin: OriginEntry) -> int:
        raise NotImplementedError

    def revision_add(self, revision: RevisionEntry):
        raise NotImplementedError

    def revision_add_before_revision(
        self, relative: RevisionEntry, revision: RevisionEntry
    ):
        raise NotImplementedError

    def revision_add_to_origin(self, origin: OriginEntry, revision: RevisionEntry):
        raise NotImplementedError

    def revision_get_early_date(self, revision: RevisionEntry) -> datetime:
        raise NotImplementedError

    def revision_get_prefered_origin(self, revision: RevisionEntry) -> int:
        raise NotImplementedError

    def revision_in_history(self, revision: RevisionEntry) -> bool:
        raise NotImplementedError

    def revision_set_prefered_origin(
        self, origin: OriginEntry, revision: RevisionEntry
    ):
        raise NotImplementedError

    def revision_visited(self, revision: RevisionEntry) -> bool:
        raise NotImplementedError


def directory_process_content(
    provenance: ProvenanceInterface,
    directory: DirectoryEntry,
    relative: DirectoryEntry,
    prefix: PosixPath,
):
    stack = [(directory, prefix)]

    while stack:
        current, path = stack.pop()

        for child in iter(current):
            if isinstance(child, FileEntry):
                # Add content to the relative directory with the computed path.
                provenance.content_add_to_directory(relative, child, path)
            else:
                # Recursively walk the child directory.
                stack.append((child, path / child.name))


def directory_update_content(
    stack: List[Tuple[DirectoryEntry, PosixPath]],
    provenance: ProvenanceInterface,
    revision: RevisionEntry,
    directory: DirectoryEntry,
    path: PosixPath,
    subdirs: Optional[List[DirectoryEntry]] = None,
    blobs: Optional[List[FileEntry]] = None,
    blobdates: Optional[Dict[bytes, datetime]] = None,
):
    assert revision.date is not None

    # Init optional parameters if not provided.
    if subdirs is None:
        subdirs = [
            child for child in iter(directory) if isinstance(child, DirectoryEntry)
        ]

    if blobs is None:
        blobs = [child for child in iter(directory) if isinstance(child, FileEntry)]

    if blobdates is None:
        blobdates = provenance.content_get_early_dates(blobs)

    # Iterate over blobs updating their date if necessary.
    for blob in blobs:
        date = blobdates.get(blob.id, None)
        if date is None or revision.date < date:
            provenance.content_set_early_date(blob, revision.date)
        provenance.content_add_to_revision(revision, blob, path)

    # Push all subdirectories with its corresponding path to analyze them
    # recursively.
    for subdir in subdirs:
        stack.append((subdir, path / subdir.name))


def origin_add(provenance: ProvenanceInterface, origin: OriginEntry):
    # TODO: refactor to iterate over origin visit statuses and commit only once
    # per status.
    origin.id = provenance.origin_get_id(origin)

    for revision in origin.revisions:
        # logging.info(f'Processing revision {hash_to_hex(revision.id)} from origin {origin.url}')
        origin_add_revision(provenance, origin, revision)

        # Commit after each revision
        provenance.commit()  # TODO: verify this!


def origin_add_revision(
    provenance: ProvenanceInterface, origin: OriginEntry, revision: RevisionEntry
):
    stack: List[Tuple[Optional[RevisionEntry], RevisionEntry]] = [(None, revision)]

    while stack:
        relative, current = stack.pop()

        # Check if current revision has no prefered origin and update if necessary.
        prefered = provenance.revision_get_prefered_origin(current)
        # logging.debug(f'Prefered origin for revision {hash_to_hex(rev.id)}: {prefered}')

        if prefered is None:
            provenance.revision_set_prefered_origin(origin, current)
        ########################################################################

        if relative is None:
            # This revision is pointed directly by the origin.
            visited = provenance.revision_visited(current)
            logging.debug(
                f"Revision {hash_to_hex(current.id)} in origin {origin.id}: {visited}"
            )

            logging.debug(
                f"Adding revision {hash_to_hex(current.id)} to origin {origin.id}"
            )
            provenance.revision_add_to_origin(origin, current)

            if not visited:
                stack.append((current, current))

        else:
            # This revision is a parent of another one in the history of the
            # relative revision.
            for parent in iter(current):
                visited = provenance.revision_visited(parent)
                logging.debug(
                    f"Parent {hash_to_hex(parent.id)} in some origin: {visited}"
                )

                if not visited:
                    # The parent revision has never been seen before pointing
                    # directly to an origin.
                    known = provenance.revision_in_history(parent)
                    logging.debug(
                        f"Revision {hash_to_hex(parent.id)} before revision: {known}"
                    )

                    if known:
                        # The parent revision is already known in some other
                        # revision's history. We should point it directly to
                        # the origin and (eventually) walk its history.
                        logging.debug(
                            f"Adding revision {hash_to_hex(parent.id)} directly to origin {origin.id}"
                        )
                        stack.append((None, parent))
                    else:
                        # The parent revision was never seen before. We should
                        # walk its history and associate it with the same
                        # relative revision.
                        logging.debug(
                            f"Adding parent revision {hash_to_hex(parent.id)} to revision {hash_to_hex(relative.id)}"
                        )
                        provenance.revision_add_before_revision(relative, parent)
                        stack.append((relative, parent))
                else:
                    # The parent revision already points to an origin, so its
                    # history was properly processed before. We just need to
                    # make sure it points to the current origin as well.
                    logging.debug(
                        f"Adding parent revision {hash_to_hex(parent.id)} to origin {origin.id}"
                    )
                    provenance.revision_add_to_origin(origin, parent)


def revision_add(
    provenance: ProvenanceInterface, archive: ArchiveInterface, revision: RevisionEntry
):
    assert revision.date is not None
    assert revision.root is not None

    # Processed content starting from the revision's root directory
    date = provenance.revision_get_early_date(revision)
    if date is None or revision.date < date:
        provenance.revision_add(revision)
        revision_process_content(
            provenance, revision, DirectoryEntry(archive, revision.root, PosixPath("."))
        )

    return provenance.commit()


def revision_process_content(
    provenance: ProvenanceInterface, revision: RevisionEntry, directory: DirectoryEntry
):
    assert revision.date is not None

    stack = [(directory, directory.name)]

    while stack:
        # Get next directory to process and query its date right before
        # processing to be sure we get the most recently updated value.
        current, path = stack.pop()
        date = provenance.directory_get_date_in_isochrone_frontier(current)

        if date is None:
            # The directory has never been seen on the isochrone graph of a
            # revision. Its children should be checked.
            blobs = [child for child in iter(current) if isinstance(child, FileEntry)]
            dirs = [
                child for child in iter(current) if isinstance(child, DirectoryEntry)
            ]

            blobdates = provenance.content_get_early_dates(blobs)
            dirdates = provenance.directory_get_early_dates(dirs)

            # Get the list of ids with no duplicates to ensure we have
            # available dates for all the elements. This prevents takign a
            # wrong decision when a blob occurres more than once in the same
            # directory.
            ids = list(dict.fromkeys([child.id for child in blobs + dirs]))
            if ids:
                dates = list(blobdates.values()) + list(dirdates.values())

                if len(dates) == len(ids):
                    # All child nodes of current directory are already known.
                    maxdate = max(dates) < revision.date

                    if maxdate < revision.date:
                        # The directory belongs to the outer isochrone frontier
                        # of the current revision, and this is the first time
                        # it appears as such.
                        # TODO: maybe the parent directory is the one that has
                        # to be added to the frontier instead!
                        provenance.directory_set_date_in_isochrone_frontier(
                            current, maxdate
                        )
                        provenance.directory_add_to_revision(revision, current, path)
                        directory_process_content(
                            provenance,
                            directory=current,
                            relative=current,
                            prefix=PosixPath("."),
                        )

                    else:
                        # This revision is either in the inner frontiern or out
                        # of order. All the children from the current directory
                        # should be analyzed (and timestamps eventually
                        # updated) yet current.
                        directory_update_content(
                            stack,
                            provenance,
                            revision,
                            current,
                            path,
                            subdirs=dirs,
                            blobs=blobs,
                            blobdates=blobdates,
                        )

                else:
                    # Al least one child node is known, ie. the directory is
                    # not on the outer isochrone frontier of the current
                    # revision. Its child nodes should be analyzed and current
                    # directory updated before them.
                    # FIXME: I believe the only different between this branche
                    # and the two 'else' cases above is this push to the stack.
                    # If so, we might refactor this to avoid so many branches.
                    # stack.append((current, path))
                    # TODO: to uncomment the above line is it necessary to
                    # postpone the adding of dictories to the isochrone
                    # frontier from the branch above (maxdate < revision.date).
                    directory_update_content(
                        stack,
                        provenance,
                        revision,
                        current,
                        path,
                        subdirs=dirs,
                        blobs=blobs,
                        blobdates=blobdates,
                    )

        elif revision.date < date:
            # The directory has already been seen on the isochrone frontier of
            # a revision, but current revision is earlier. Its children should
            # be updated.
            directory_update_content(stack, provenance, revision, current, path)
            provenance.directory_set_date_in_isochrone_frontier(current, revision.date)

        else:
            # The directory has already been seen on the outer isochrone
            # frontier of an earlier revision. Just add it to the current
            # revision.
            provenance.directory_add_to_revision(revision, current, path)
