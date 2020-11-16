import logging

from .archive import ArchiveInterface
from .model import DirectoryEntry, FileEntry
from .origin import OriginEntry
from .revision import RevisionEntry

from datetime import datetime
from pathlib import PosixPath
from typing import Dict, List

from swh.model.hashutil import hash_to_hex


class ProvenanceInterface:
    def __init__(self, **kwargs):
        raise NotImplementedError


    def commit(self):
        raise NotImplementedError


    def content_add_to_directory(self, directory: DirectoryEntry, blob: FileEntry, prefix: PosixPath):
        raise NotImplementedError


    def content_add_to_revision(self, revision: RevisionEntry, blob: FileEntry, prefix: PosixPath):
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


    def directory_add_to_revision(self, revision: RevisionEntry, directory: DirectoryEntry, path: PosixPath):
        raise NotImplementedError


    def directory_get_date_in_isochrone_frontier(self, directory: DirectoryEntry) -> datetime:
        raise NotImplementedError


    def directory_get_early_dates(self, dirs: List[DirectoryEntry]) -> Dict[bytes, datetime]:
        raise NotImplementedError


    def directory_set_date_in_isochrone_frontier(self, directory: DirectoryEntry, date: datetime):
        raise NotImplementedError


    def origin_get_id(self, origin: OriginEntry) -> int:
        raise NotImplementedError


    def revision_add(self, revision: RevisionEntry):
        raise NotImplementedError


    def revision_add_before_revision(self, relative: RevisionEntry, revision: RevisionEntry):
        raise NotImplementedError


    def revision_add_to_origin(self, origin: OriginEntry, revision: RevisionEntry):
        raise NotImplementedError


    def revision_get_early_date(self, revision: RevisionEntry) -> datetime:
        raise NotImplementedError


    def revision_get_prefered_origin(self, revision: RevisionEntry) -> int:
        raise NotImplementedError


    def revision_in_history(self, revision: RevisionEntry) -> bool:
        raise NotImplementedError


    def revision_set_prefered_origin(self, origin: OriginEntry, revision: RevisionEntry):
        raise NotImplementedError


    def revision_visited(self, revision: RevisionEntry) -> bool:
        raise NotImplementedError


def directory_process_content(
    provenance: ProvenanceInterface,
    directory: DirectoryEntry,
    relative: DirectoryEntry,
    prefix: PosixPath
):
    stack = [(directory, prefix)]

    while stack:
        dir, path = stack.pop()

        for child in iter(dir):
            if isinstance(child, FileEntry):
                # Add content to the relative directory with the computed path.
                provenance.content_add_to_directory(relative, child, path)
            else:
                # Recursively walk the child directory.
                stack.append((child, path / child.name))


def origin_add(
    provenance: ProvenanceInterface,
    origin: OriginEntry
):
    origin.id = provenance.origin_get_id(origin)

    for revision in origin.revisions:
        # logging.info(f'Processing revision {hash_to_hex(revision.id)} from origin {origin.url}')
        origin_add_revision(provenance, origin, revision)

        # Commit after each revision
        provenance.commit()      # TODO: verify this!


def origin_add_revision(
    provenance: ProvenanceInterface,
    origin: OriginEntry,
    revision: RevisionEntry
):
    stack = [(None, revision)]

    while stack:
        relative, rev = stack.pop()

        # Check if current revision has no prefered origin and update if necessary.
        prefered = provenance.revision_get_prefered_origin(rev)
        # logging.debug(f'Prefered origin for revision {hash_to_hex(rev.id)}: {prefered}')

        if prefered is None:
            provenance.revision_set_prefered_origin(origin, rev)
        ########################################################################

        if relative is None:
            # This revision is pointed directly by the origin.
            visited = provenance.revision_visited(rev)
            logging.debug(f'Revision {hash_to_hex(rev.id)} in origin {origin.id}: {visited}')

            logging.debug(f'Adding revision {hash_to_hex(rev.id)} to origin {origin.id}')
            provenance.revision_add_to_origin(origin, rev)

            if not visited:
                stack.append((rev, rev))

        else:
            # This revision is a parent of another one in the history of the
            # relative revision.
            for parent in iter(rev):
                visited = provenance.revision_visited(parent)
                logging.debug(f'Parent {hash_to_hex(parent.id)} in some origin: {visited}')

                if not visited:
                    # The parent revision has never been seen before pointing
                    # directly to an origin.
                    known = provenance.revision_in_history(parent)
                    logging.debug(f'Revision {hash_to_hex(parent.id)} before revision: {known}')

                    if known:
                        # The parent revision is already known in some other
                        # revision's history. We should point it directly to
                        # the origin and (eventually) walk its history.
                        logging.debug(f'Adding revision {hash_to_hex(parent.id)} directly to origin {origin.id}')
                        stack.append((None, parent))
                    else:
                        # The parent revision was never seen before. We should
                        # walk its history and associate it with the same
                        # relative revision.
                        logging.debug(f'Adding parent revision {hash_to_hex(parent.id)} to revision {hash_to_hex(relative.id)}')
                        provenance.revision_add_before_revision(relative, parent)
                        stack.append((relative, parent))
                else:
                    # The parent revision already points to an origin, so its
                    # history was properly processed before. We just need to
                    # make sure it points to the current origin as well.
                    logging.debug(f'Adding parent revision {hash_to_hex(parent.id)} to origin {origin.id}')
                    provenance.revision_add_to_origin(origin, parent)


def revision_add(
    provenance: ProvenanceInterface,
    archive: ArchiveInterface,
    revision: RevisionEntry
):
    # Processed content starting from the revision's root directory
    date = provenance.revision_get_early_date(revision)
    if date is None or revision.date < date:
        provenance.revision_add(revision)
        revision_process_content(
            provenance,
            revision,
            DirectoryEntry(archive, revision.root, PosixPath('.'))
        )
    return provenance.commit()


def revision_process_content(
    provenance: ProvenanceInterface,
    revision: RevisionEntry,
    directory: DirectoryEntry
):
    date = provenance.directory_get_date_in_isochrone_frontier(directory)
    stack = [(directory, date, directory.name)]
    # stack = [(directory, directory.name)]

    while stack:
        dir, date, path = stack.pop()
        # dir, path = stack.pop()
        # date = provenance.directory_get_date_in_isochrone_frontier(dir)

        if date is None:
            # The directory has never been seen on the isochrone graph of a
            # revision. Its children should be checked.
            blobs = [child for child in iter(dir) if isinstance(child, FileEntry)]
            dirs = [child for child in iter(dir) if isinstance(child, DirectoryEntry)]

            blobdates = provenance.content_get_early_dates(blobs)
            # TODO: this will only return timestamps for diretories that were
            # seen in an isochrone frontier. But a directory may only cointain a
            # subdirectory whose contents are already known. Which one should be
            # added to the frontier then (the root or the sub directory)?
            dirdates = provenance.directory_get_early_dates(dirs)

            ids = list(dict.fromkeys([child.id for child in blobs + dirs]))
            if ids:
                dates = list(blobdates.values()) + list(dirdates.values())

                if len(dates) == len(ids) and max(dates) <= revision.date:
                    # The directory belongs to the isochrone frontier of the
                    # current revision, and this is the first time it appears
                    # as such.
                    provenance.directory_set_date_in_isochrone_frontier(dir, max(dates))
                    provenance.directory_add_to_revision(revision, dir, path)
                    directory_process_content(
                        provenance,
                        directory=dir,
                        relative=dir,
                        prefix=PosixPath('.')
                    )

                else:
                    # The directory is not on the isochrone frontier of the
                    # current revision. Its child nodes should be analyzed.
                    ############################################################
                    for child in blobs:
                        date = blobdates.get(child.id, None)
                        # date = provenance.content_get_early_date(child)
                        if date is None or revision.date < date:
                            provenance.content_set_early_date(child, revision.date)
                        provenance.content_add_to_revision(revision, child, path)

                    for child in dirs:
                        date = dirdates.get(child.id, None)
                        # date = provenance.directory_get_date_in_isochrone_frontier(child)
                        stack.append((child, date, path / child.name))
                        # stack.append((child, path / child.name))
                    ############################################################

        elif revision.date < date:
            # The directory has already been seen on the isochrone frontier of
            # a revision, but current revision is earlier. Its children should
            # be updated.
            blobs = [child for child in iter(dir) if isinstance(child, FileEntry)]
            dirs = [child for child in iter(dir) if isinstance(child, DirectoryEntry)]

            blobdates = provenance.content_get_early_dates(blobs)
            dirdates = provenance.directory_get_early_dates(dirs)

            ####################################################################
            for child in blobs:
                # date = blobdates.get(child.id, None)
                date = provenance.content_get_early_date(child)
                if date is None or revision.date < date:
                    provenance.content_set_early_date(child, revision.date)
                provenance.content_add_to_revision(revision, child, path)

            for child in dirs:
                # date = dirdates.get(child.id, None)
                date = provenance.directory_get_date_in_isochrone_frontier(child)
                stack.append((child, date, path / child.name))
                # stack.append((child, path / child.name))
            ####################################################################

            provenance.directory_set_date_in_isochrone_frontier(dir, revision.date)

        else:
            # The directory has already been seen on the isochrone frontier of
            # an earlier revision. Just add it to the current revision.
            provenance.directory_add_to_revision(revision, dir, path)
