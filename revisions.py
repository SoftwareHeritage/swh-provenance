import io
import logging
import random
import utils
import sys

from iterator import (
    RevisionEntry,
    ArchiveRevisionIterator
)

from swh.model.identifiers import identifier_to_str


def rev_to_csv(revision: RevisionEntry):
    return ','.join([
        identifier_to_str(revision.swhid),
        str(revision.timestamp),
        identifier_to_str(revision.directory)
    ]) + '\n'


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) != 4:
        print('Usage: revisions CONF_FILE DATABASE COUNT')

    database = (sys.argv[1], sys.argv[2])
    count = int(sys.argv[3])

    print(f'{count} {database}')
    data_conn = utils.connect(database[0], database[1])
    revisions = list(ArchiveRevisionIterator(data_conn, limit=count))

    random.shuffle(revisions)
    with io.open('random.csv', 'w') as outfile:
        for rev in revisions:
            outfile.write(rev_to_csv(rev))

    with io.open('ordered.csv', 'w') as outfile:
        for rev in sorted(revisions, key=lambda rev: rev.timestamp):
            outfile.write(rev_to_csv(rev))

    with io.open('reverse.csv', 'w') as outfile:
        for rev in sorted(revisions, key=lambda rev: rev.timestamp, reverse=True):
            outfile.write(rev_to_csv(rev))

    data_conn.close()
