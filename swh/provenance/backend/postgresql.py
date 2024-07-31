# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
import logging
from typing import List, Optional

import psycopg2.extras
import psycopg2.pool
from swh.core.db import BaseDb
from swh.core.db.common import db_transaction
from swh.core.db.db_utils import swh_db_version
from swh.model.swhids import CoreSWHID, QualifiedSWHID

from swh.provenance.exc import ProvenanceDBError

logger = logging.getLogger(__name__)


class Db(BaseDb):
    """
    PostgreSQL backend for the Software Heritage provenance index.
    """


class PostgresqlProvenance:
    current_version: int = 1

    def __init__(self, db, min_pool_conns=1, max_pool_conns=10):
        try:
            if isinstance(db, psycopg2.extensions.connection):
                self._pool = None
                self._db = Db(db)

                # See comment below
                self._db.cursor().execute("SET TIME ZONE 'UTC'")
            else:
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    min_pool_conns, max_pool_conns, db
                )
                self._db = None
        except psycopg2.OperationalError as e:
            raise ProvenanceDBError(e)

    def get_db(self):
        if self._db:
            return self._db
        else:
            db = Db.from_pool(self._pool)

            # Workaround for psycopg2 < 2.9.0 not handling fractional timezones,
            # which may happen on old revision/release dates on systems configured
            # with non-UTC timezones.
            # https://www.psycopg.org/docs/usage.html#time-zones-handling
            db.cursor().execute("SET TIME ZONE 'UTC'")

            return db

    def put_db(self, db):
        if db is not self._db:
            db.put_conn()

    @contextmanager
    def db(self):
        db = None
        try:
            db = self.get_db()
            yield db
        finally:
            if db:
                self.put_db(db)

    @db_transaction()
    def check_config(self, *, check_write: bool, db: Db, cur=None) -> bool:
        dbversion = swh_db_version(db.conn.dsn)
        if dbversion != self.current_version:
            logger.warning(
                "database dbversion (%s) != %s current_version (%s)",
                dbversion,
                __name__,
                self.current_version,
            )
            return False

        # Check permissions on one of the tables
        check = "INSERT" if check_write else "SELECT"

        cur.execute(
            "select has_table_privilege(current_user, 'content_in_revision', %s)",
            (check,),
        )
        return cur.fetchone()[0]

    @db_transaction()
    def whereis(
        self, swhid: CoreSWHID, *, db: Db, cur=None
    ) -> Optional[QualifiedSWHID]:
        return QualifiedSWHID(
            object_type=swhid.object_type,
            object_id=swhid.object_id,
        )

    @db_transaction()
    def whereare(self, *, swhids: List[CoreSWHID]) -> List[Optional[QualifiedSWHID]]:
        """Given a list SWHID return a list of provenance info:

        See `whereis` documentation for details on the provenance info.
        """
        return [self.whereis(swhid=si) for si in swhids]
