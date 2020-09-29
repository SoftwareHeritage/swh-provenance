import io
import logging
import os
import utils

from graphviz import Digraph
from pathlib import PosixPath
from swh.model.identifiers import identifier_to_str


if __name__ == "__main__":
    """Compact model origin-revision layer utility."""
    # logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(filename='draw.log', level=logging.DEBUG)

    comp_conn = utils.connect('database.conf', 'compact')

    cursor = comp_conn.cursor()
    cursor.execute('''SELECT id FROM revision''')
    for idx in range(1000):
        rev = cursor.fetchone()[0]

        revid = identifier_to_str(rev)
        dot = Digraph(comment=f'Content of revision {revid}')
        dot.node(revid, f'REVISION\n{revid}')

        blobs = dict()
        dirs = dict()
        revcur = comp_conn.cursor()

        ########################################################################
        revcur.execute('''SELECT blob, path FROM content_early_in_rev WHERE rev=%s''',
                          (rev,))
        for blob in revcur.fetchall():
            blobid = identifier_to_str(blob[0])
            blobpath = PosixPath(os.fsdecode(blob[1]))
            if blobid in blobs.keys():
                blobs[blobid][0].append(revid)
                blobs[blobid][1].append(blobpath)
            else:
                blobs[blobid] = ([revid], [blobpath])


        ########################################################################
        revcur.execute('''SELECT dir, path FROM directory_in_rev WHERE rev=%s''',
                          (rev,))
        for dir in revcur.fetchall():
            dirid = identifier_to_str(dir[0])
            dirpath = PosixPath(os.fsdecode(dir[1]))
            if dirid in dirs.keys():
                dirs[dirid][0].append(revid)
                dirs[dirid][1].append(dirpath)
            else:
                dirs[dirid] = ([revid], [dirpath])

            dircur = comp_conn.cursor()
            dircur.execute('''SELECT blob, path FROM content_in_dir WHERE dir=%s''',
                              (dir[0],))
            for blob in dircur.fetchall():
                blobid = identifier_to_str(blob[0])
                blobpath = PosixPath(os.fsdecode(blob[1]))
                if blobid in blobs.keys():
                    blobs[blobid][0].append(dirid)
                    blobs[blobid][1].append(dirpath / blobpath)
                else:
                    blobs[blobid] = ([dirid], [dirpath / blobpath])

        ########################################################################
        for dirid, dirdata in dirs.items():
            dirpaths = "\n".join(map(str, dirdata[1]))
            dot.node(dirid, f'DIRECTORY\n{dirid}\n{dirpaths}')
            for parent in dirdata[0]:
                dot.edge(parent, dirid)

        for blobid, blobdata in blobs.items():
            blobpaths = "\n".join(map(str, blobdata[1]))
            dot.node(blobid, f'CONTENT\n{blobid}\n{blobpaths}')
            for parent in blobdata[0]:
                dot.edge(parent, blobid)

        with io.open(f'content-revision/{revid}.graph', 'w') as outfile:
            outfile.write(dot.source)

        dot.render(f'{revid}.gv', 'content-revision')
        os.remove(f'content-revision/{revid}.gv')

    comp_conn.close()
