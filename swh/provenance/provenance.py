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
    # TODO: turn this into a real interface and move PostgreSQL implementation
    # to a separate file
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
    directory: DirectoryEntry):

    rootcmaxdates = provenance.directory_get_date_in_isochrone_frontier(directory)
    # we check if root directory is known as part of the outer isochrone frontier
    if rootcmaxdates==None or rootcmaxdates>=revision.date:
        stack = [(directory, rootcmaxdates, directory.name, None, None, None)]
    else:
        # rootcmaxdates is defined and < revision.date
        provenance.directory_add_to_revision(revision, dir, path)
        # and we are done
        stack=[]

    TE={} # used to store cmaxdates through recursive calls
            
    while stack:
        # stack is used to implement a recursive call without python intrinsic limitation in the max depth of recursive call
        dir, cmaxdates, path, dirs, cTEblobs, cTEdirs = stack.pop()
        # dir, path = stack.pop()
        # cmaxdate 
        # dirs = list of dirs that need to be processed recursively
        # cTEblobs : cTEblobs[child.id]=[final date (Value),intial date (None or Value)]
        # cTEdirs : cTEdirs[child.id]=[final date (Value),intial date (None or Value)]

        if nextdirs is None:
            # first time we process this dir
            blobs = [child for child in iter(dir) if isinstance(child, FileEntry)]
            dirs = [child for child in iter(dir) if isinstance(child, DirectoryEntry)]
            cTEblobs={}
            cTEdirs={}
        else:
            # all blobs have been processed already
            blobs=[]

        nextdirs=[]
        cstack=[]

        blobdates = provenance.content_get_early_dates(blobs)
        dirdates = provenance.directory_get_early_dates(dirs)

        cblobdates = []
        for child in blobs: 
            cdate = blobdates.get(child.id, None)
            if cdate is None:
                cTEblobs[child.id]=[revision.date,None]
            elif cdate<revision.date:
                cTEblobs[child.id]=[cdate,cdate]
            elif cdate==revision.date:
                cTEblobs[child.id]=[cdate,cdate]
            else:
                # cdate>revision.date
                cTEblobs[child.id]=[revision.date,cdate]

            cblobdates.append(cTEblobs[child.id][0])

        cdirdates = []
        for child in dirs:
            cdate = dirdates.get(child.id, None)
            # check if thid directory has been processed through the stack
            if child.id in TE and TE[child.id]!=None:
                cdate=TE[child.id]
            if cdate is None:
                cTEdirs[child.id]=[None,None]
                cstack.append((child, None, path / child.name, None, None, None))
            elif cdate<revision.date:
                # outer frontier
                cTEdirs[child.id]=[cdate,cdate]
            elif cdate==revision.date:
                # inner frontier
                cTEdirs[child.id]=[cdate,cdate]
            elif cdate>revision.date:
                # need to be processed to update all TE
                cTEdirs[child.id]=[None,cdate]
                cstack.append((child, None, path / child.name, None, None, None))

            if cTEdirs[child.id][0]!=None:
                cdirdates.append(cTEdirs[child.id][0])
            else:
                nextdirs.append(child)

        # define max of cmaxdates according to new values
        # starting from previous value (ie blobs and dirs already processed)

        if cblobdates:
            if cmaxdates is None:
                cmaxdates=max(cblobdates)
            else:
                cmaxdates=max(max(cblobdates),cmaxdates)

        if cdirdates:
            if cmaxdates is None:
                cmaxdates=max(cdirdates)
            else:
                cmaxdates=max(max(cdirdates),cmaxdates)

        # if at least one dir is None, we need to fill the stack recursively
        if nextdirs: 
            # ! the parent dir must be append before the child directories
            stack.append((dir, cmaxdates, path / child.name, nextdirs, cTEblobs, cTEdirs))
            stack+=cstack
            for child in nextdirs: 
                if child.id not in TE:
                    TE[child.id]=None
        else:
            # we can proceed
            if cmaxdates is None:
                # we may have a problem or not.
                # only possible for an empty directory 
                # return revision.date
                if len(cTEblobs)!=0: 
                    print("ERROR")
                else:
                    if dir.id in TE:
                        TE[dir.id]=revision.date
            elif cmaxdates<revision.date:
                # the directory is outside the isochrone graph.
                # if this is the root directory we are almost done
                # or we need to process upper directories
                if stack:
                    # this is not the root directory
                    # update TE and move forward (value will be used later)
                    if dir.id in TE:
                        TE[dir.id]=cmaxdates
                else:
                    # this is the root directory (it means that the root directory has been seen earlier)
                    # if D not in directory
                    if rootcmaxdates==None:
                        directory_process_content(
	                        provenance,
	                        directory=dir,
	                        relative=dir,
	                        prefix=PosixPath('.')
                                )
                    else:
                        if rootcmaxdates>cmaxdates:
                            # ! update with min on the DB side (it may have change while processing the revision
                            # if more than one process running
                            provenance.directory_set_date_in_isochrone_frontier(dir, cmaxdates)
                    # fill D-R
                    provenance.directory_add_to_revision(revision, dir, path)
            elif cmaxdates==revision.date:
                # the current directory is in the inner isochrone frontier
                blobs = [child for child in iter(dir) if isinstance(child, FileEntry)]
                dirs = [child for child in iter(dir) if isinstance(child, DirectoryEntry)]
                for child in blobs:
                    cdate=cTEblobs[child.id][0]
                    if (cTEblobs[child.id][1] is None) or (cdate<cTEblobs[child.id][1]): 
                        provenance.content_set_early_date(child, cdate)
                    provenance.content_add_to_revision(revision, child, path)
                    # update earliest
                for child in dirs:
                    cdate=cTEdirs[child.id][0]
                    if cdate<revision.date:
                        # child directory is in the outer isochrone frontier
                        # fill D-R
                        provenance.directory_add_to_revision(revision, child, path)
                        # D not in directory, or need update
                        if (cTEdirs[child.id][1] is None) or (cdate<cTEdirs[child.id][1]): 
                                provenance.directory_set_date_in_isochrone_frontier(child, cdate)
                        # D not in directory, need to fill C-D
                        if cTEdirs[child.id][1]==None:
                            directory_process_content(
	                            provenance,
	                            directory=child,
	                            relative=child,
	                            prefix=PosixPath('.')
                                    )
                    else:
                        # cdate=revision.date
                        # nothing to do
                        pass
            elif cmaxdates>revision.date:
                # should not happend
                pass
            else:
                # should not happend
                pass
