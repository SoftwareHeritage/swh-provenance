def revision_process_content(
    provenance: ProvenanceInterface, revision: RevisionEntry, directory: DirectoryEntry
):
    date = provenance.directory_get_date_in_isochrone_frontier(directory)
    # stack = [(directory, directory.name)]

    # XXX
    # deal with revision root (if we still want to allow directory root in D-R table)
    # should be managed in revision_add() with an "else" statment folowwing the "if"
    # but I put it here because I need to be sure, that all directories in the outer
    # frontier will be child of a directory (the root directory is a particular case)
    if (date != None) and (date < revision.date):
        # push directory root
        provenance.directory_add_to_revision(revision, directory, directory)
        stack = []
    else:
        stack = [(directory, date, directory.name)]

    # used to store directory timestamp
    TE = {}

    while stack:
        dir, date, path = stack.pop()

        if dir.id in TE:
            dateTE = TE[dir.id]
            if (date == None) or (dateTE < date):
                date = dateTE

        dirs = [child for child in iter(dir) if isinstance(child, DirectoryEntry)]
        dirdates = provenance.directory_get_early_dates(dirs)

        nextstack = []

        # XXX look for child directory with unkown timestamp

        for child in dirs:
            dateDir = dirdates.get(child.id, None)
            dateTE = TE.get(child.dir, None)
            if (dateTE != None) and ((dateDir == None) or (dateTE < dateDir)):
                dirdates[child.id] = dateTE
            if dateDir is None:
                # we gonna have to process the directory to know max(dates)
                nextstack.append((child, None, path / child.name))
                TE[child.id] = None
            elif dateDir > revision.date:
                # directory seen earlier as part of the outer frontier
                # need to be reset and manage as it was never seen before
                nextstack.append((child, None, path / child.name))
                TE[child.id] = None
            elif dateDir == revision.date:
                # directory of the inner frontier
                # nothing to do here
                pass
            elif dateDir < revision.date:
                # directory of the outer frontier
                # nothing to do here
                pass
            else:
                # should not happen
                print("ERROR")

        if nextstack:
            # we have to proceed recursively
            # we can't know max(dates)
            stack.append((dir, date, path))
            stack += nextstack
            # order in the stack is important ...
        else:
            # otherwise proceed to determine max(dates)

            # XXX we look for blob status

            blobs = [child for child in iter(dir) if isinstance(child, FileEntry)]
            blobdates = provenance.content_get_early_dates(blobs)
            for child in blobs:
                dateBlob = blobdates.get(child.id, None)
                if dateBlob > revision.date:
                    # content already found
                    # but this revision is earliest
                    blobdates[child.id] = None

            # calculate max(dates)
            if (len(blobs) + len(dirs)) == 0:
                # empty dir
                # return revision.dates
                TE[dir.id] = revision.date
                # and we are done
            else:
                maxdates = revision.date
                for bdate in blobdates.values():
                    if date is not None:
                        maxdates = max(maxdates, bdate)
                for ddate in dirdates.values():
                    maxdates = max(maxdates, ddate)
                # about the directory we are processing
                if maxdates < revision.date:
                    # this directory is outside the ischrone graph
                    if stack:
                        # not the root directory
                        # all directories and blobs already known
                        TE[dir.id] = maxdates
                    else:
                        # this is the root directory
                        provenance.directory_add_to_revision(revision, dir, path)
                        if date == None:
                            # should the same as
                            # provenance.directory_get_early_date(dir)==None
                            directory_process_content(
                                provenance,
                                directory=dir,
                                relative=dir,
                                prefix=PosixPath("."),
                            )
                        if date == None or date > maxdates:
                            provenance.directory_set_date_in_isochrone_frontier(
                                dir, maxdates
                            )  # ! make sure insert makes a min

                elif maxdates == revision.date:
                    # the current directory is in the inner isochrone frontier
                    # that s where we can see directory nodes of the outer frontier
                    for child in blobs:
                        dateBlob = blobdates.get(child.id)
                        if dateBlob is None:
                            # unkown or reset
                            provenance.content_set_early_date(child, revision.date)
                            #     ! make sure it makes a min inserting it,
                            #     if it already exists
                            provenance.content_add_to_revision(revision, child, path)
                    for child in dirs:
                        dateDir = dirdates.get(child.id)
                        if dateDir < revision.date:
                            # this child directory is in the outer frontier
                            provenance.directory_add_to_revision(revision, child, path)
                            if provenance.directory_get_early_date(child) == None:
                                # with this implementation you don't known if this is a
                                # new one or a reset
                                directory_process_content(
                                    provenance,
                                    directory=child,
                                    relative=child,
                                    prefix=PosixPath("."),
                                )
                            provenance.directory_set_date_in_isochrone_frontier(
                                child, cdate
                            )  # ! make sure insert = min
