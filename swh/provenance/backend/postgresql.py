# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
import logging
from typing import Any, List, Optional, Union

import psycopg
import psycopg_pool

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

    def __init__(
        self,
        db: Union[str, psycopg.Connection[Any]],
        min_pool_conns: int = 1,
        max_pool_conns: int = 10,
    ):
        self._db: Optional[Db]
        self._pool: Optional[psycopg_pool.ConnectionPool]

        try:
            if isinstance(db, str):
                self._pool = psycopg_pool.ConnectionPool(
                    conninfo=db,
                    min_size=min_pool_conns,
                    max_size=max_pool_conns,
                    open=False,
                )
                self._db = None
                # Wait for the first connection to be ready, and raise the
                # appropriate exception if connection fails
                self._pool.open(wait=True, timeout=1)
            else:
                self._pool = None
                self._db = Db(db)
        except psycopg.OperationalError as e:
            raise ProvenanceDBError(e)

    def get_db(self) -> Db:
        if self._db:
            return self._db
        else:
            assert self._pool is not None
            return Db.from_pool(self._pool)

    def put_db(self, db: Db):
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
        dbversion = swh_db_version(db.conn)
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
