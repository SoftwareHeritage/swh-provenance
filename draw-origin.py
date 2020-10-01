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
    cursor.execute('''SELECT id, url FROM origin''')
    for idx in range(1):
        org = cursor.fetchone()

        orgid = str(org[0])
        orgurl = str(org[1])
        dot = Digraph(comment=f'Revision history of origin {orgurl}')
        dot.node(orgid, f'ORIGIN ({orgid})\n{orgurl}')

        revs = dict()
        orgcur = comp_conn.cursor()

        ########################################################################
        orgcur.execute('''SELECT rev FROM revision_in_org WHERE org=%s''',
                          (orgid,))
        for rev in orgcur.fetchall():
            revid = identifier_to_str(rev[0])
            if revid in revs.keys():
                revs[revid].append(orgid)
            else:
                revs[revid] = ([orgid])


            revcur = comp_conn.cursor()
            revcur.execute('''SELECT prev FROM revision_before_rev WHERE next=%s''',
                              (rev[0],))
            for prev in revcur.fetchall():
                previd = identifier_to_str(prev[0])
                if previd in revs.keys():
                    revs[previd].append(revid)
                else:
                    revs[previd] = ([revid])

        ########################################################################
        for revid, nexts in revs.items():
            dot.node(revid, f'REVISION\n{revid}')
            for next in nexts:
                dot.edge(revid, next)

        with io.open(f'revision-origin/{orgid}.graph', 'w') as outfile:
            outfile.write(dot.source)

        dot.render(f'{orgid}.gv', 'revision-origin', view=True)
        os.remove(f'revision-origin/{orgid}.gv')

    comp_conn.close()
