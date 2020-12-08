def revision_process_content(
    provenance: ProvenanceInterface,
    revision: RevisionEntry,
    directory: DirectoryEntry
):
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
            dirs = [child for child in iter(current) if isinstance(child, DirectoryEntry)]

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
                    # GR : ok all childs have a known date

                    if maxdate < revision.date:
                        # The directory belongs to the isochrone frontier of the
                        # current revision, and this is the first time it appears
                        # as such.
                        # FIXME GR : GR do not agree.
                        #          : it means that this directory is outside the isochrone frontier
                        #          : but may be not at the frontier if its parent directory is such that maxdate<revision.date
                        #          : That's why directory of the outer isochrone frontier must be filled when 
                        #         : their parent directory in the inner isochrone frontier are processed. 
                        provenance.directory_set_date_in_isochrone_frontier(current, maxdate)
                        provenance.directory_add_to_revision(revision, current, path)
                        directory_process_content(
                            provenance,
                            directory=current,
                            relative=current,
                            prefix=PosixPath('.')
                        )

                    elif revision.date < maxdate:
                        # This revision is out of order. All the children from
                        # the current directory should be updated yet current
                        # directory does not belong to the isochrone frontier.
                        # FIXME GR 2  :  I think that in this case it is more than updating content timestamp.
                        #               it can lead to new directory node
                        directory_update_content(
                            stack, provenance, revision, current, path,
                            subdirs=dirs, blobs=blobs, blobdates=blobdates)


                    else:
                        # Directory is in the inner frontier and its children
                        # have already been analyzed. Nothing to do.
                        # FIXME: Although we are not updating timestamps here,
                        # I guess we still need to walk the diretory to
                        # fill the content_early_in_revision table, doesn't we?
                        # FIXME GR 3 : yes (see provenance.gr.py and FIXME 1 above)
                        # Otherwise will be missing some blob occurrences.
                        # If so, we should marge this branch with the previous
                        # one.
                        pass

                else:
                    # Al least one child node is known, ie. the directory is
                    # FIXME GR 4 : do you mean unkown ? 
                    #            : if content child nodes are unkown it does matter, 
                    #            : you can process them since they will be added to 
                    #            : the content-revision table, but if a directory 
                    #            : is unkown you have to process them.
                    # not on the isochrone frontier of the current revision.
                    # Its child nodes should be analyzed and current directory
                    # updated before them.
                    # FIXME: I believe the only different between this branche
                    # and the tow 'else' cases above is this push to the stack.
                    # If so, we might refactor this to avoid so many branches.
                    stack.append((current, path))
                    directory_update_content(
                        stack, provenance, revision, current, path,
                        subdirs=dirs, blobs=blobs, blobdates=blobdates)

        elif revision.date < date:
            # The directory has already been seen on the isochrone frontier of
            # a revision, but current revision is earlier. Its children should
            # be updated.
            directory_update_content(stack, provenance, revision, current, path)
            provenance.directory_set_date_in_isochrone_frontier(current, revision.date)

        else:
            # The directory has already been seen on the isochrone frontier of
            # an earlier revision. Just add it to the current revision.
            provenance.directory_add_to_revision(revision, current, path)

