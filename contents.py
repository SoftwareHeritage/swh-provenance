import os
import psycopg2

from swh.model.hashutil import (hash_to_bytes, hash_to_hex)
from swh.provenance.provenance import get_provenance


if __name__ == "__main__":
    conninfo = {
        "host": "localhost",
        "database": "test2",
        "user": "postgres",
        "password": "postgres"
    }
    provenance = get_provenance(conninfo)

    print('content(id, date): ################################################')
    provenance.cursor.execute('''SELECT id, date FROM content''')
    for row in provenance.cursor.fetchall():
        print(f'{hash_to_hex(row[0])}, {row[1]}')
    print('###################################################################')

    print('content_early_in_rev(blob, rev, path): ############################')
    provenance.cursor.execute('''SELECT blob, rev, path FROM content_early_in_rev''')
    for row in provenance.cursor.fetchall():
        print(f'{hash_to_hex(row[0])}, {hash_to_hex(row[1])}, {os.fsdecode(row[2])}')
    print('###################################################################')

    print('content_in_dir(blob, dir, path): ##################################')
    provenance.cursor.execute('''SELECT blob, dir, path FROM content_in_dir''')
    for row in provenance.cursor.fetchall():
        print(f'{hash_to_hex(row[0])}, {hash_to_hex(row[1])}, {os.fsdecode(row[2])}')
    print('###################################################################')

    print('directory(id, date): ##############################################')
    provenance.cursor.execute('''SELECT id, date FROM directory''')
    for row in provenance.cursor.fetchall():
        print(f'{hash_to_hex(row[0])}, {row[1]}')
    print('###################################################################')

    print('directory_in_rev(dir, rev, path): #################################')
    provenance.cursor.execute('''SELECT dir, rev, path FROM directory_in_rev''')
    for row in provenance.cursor.fetchall():
        print(f'{hash_to_hex(row[0])}, {hash_to_hex(row[1])}, {os.fsdecode(row[2])}')
    print('###################################################################')

    print('revision(id, date): ###############################################')
    provenance.cursor.execute('''SELECT id, date FROM revision''')
    for row in provenance.cursor.fetchall():
        print(f'{hash_to_hex(row[0])}, {row[1]}')
    print('###################################################################')
