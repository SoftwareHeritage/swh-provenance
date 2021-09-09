# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
from datetime import datetime
import itertools
import logging
from typing import Dict, Generator, Iterable, List, Optional, Set, Union

import psycopg2.extensions
import psycopg2.extras
from typing_extensions import Literal

from swh.core.db import BaseDb
from swh.model.model import Sha1Git

from ..interface import (
    EntityType,
    ProvenanceResult,
    RelationData,
    RelationType,
    RevisionData,
)

LOGGER = logging.getLogger(__name__)


class ProvenanceStoragePostgreSql:
    def __init__(
        self, conn: psycopg2.extensions.connection, raise_on_commit: bool = False
    ) -> None:
        BaseDb.adapt_conn(conn)
        self.conn = conn
        with self.transaction() as cursor:
            cursor.execute("SET timezone TO 'UTC'")
        self._flavor: Optional[str] = None
        self.raise_on_commit = raise_on_commit

    @contextmanager
    def transaction(
        self, readonly: bool = False
    ) -> Generator[psycopg2.extensions.cursor, None, None]:
        self.conn.set_session(readonly=readonly)
        with self.conn:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                yield cur

    @property
    def flavor(self) -> str:
        if self._flavor is None:
            with self.transaction(readonly=True) as cursor:
                cursor.execute("SELECT swh_get_dbflavor() AS flavor")
                self._flavor = cursor.fetchone()["flavor"]
        assert self._flavor is not None
        return self._flavor

    @property
    def denormalized(self) -> bool:
        return "denormalized" in self.flavor

    def content_add(
        self, cnts: Union[Iterable[Sha1Git], Dict[Sha1Git, datetime]]
    ) -> bool:
        return self._entity_set_date("content", cnts)

    def content_find_first(self, id: Sha1Git) -> Optional[ProvenanceResult]:
        sql = "SELECT * FROM swh_provenance_content_find_first(%s)"
        with self.transaction(readonly=True) as cursor:
            cursor.execute(query=sql, vars=(id,))
            row = cursor.fetchone()
        return ProvenanceResult(**row) if row is not None else None

    def content_find_all(
        self, id: Sha1Git, limit: Optional[int] = None
    ) -> Generator[ProvenanceResult, None, None]:
        sql = "SELECT * FROM swh_provenance_content_find_all(%s, %s)"
        with self.transaction(readonly=True) as cursor:
            cursor.execute(query=sql, vars=(id, limit))
            yield from (ProvenanceResult(**row) for row in cursor)

    def content_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, datetime]:
        return self._entity_get_date("content", ids)

    def directory_add(
        self, dirs: Union[Iterable[Sha1Git], Dict[Sha1Git, datetime]]
    ) -> bool:
        return self._entity_set_date("directory", dirs)

    def directory_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, datetime]:
        return self._entity_get_date("directory", ids)

    def entity_get_all(self, entity: EntityType) -> Set[Sha1Git]:
        with self.transaction(readonly=True) as cursor:
            cursor.execute(f"SELECT sha1 FROM {entity.value}")
            return {row["sha1"] for row in cursor}

    def location_add(self, paths: Iterable[bytes]) -> bool:
        if not self.with_path():
            return True
        try:
            values = [(path,) for path in paths]
            if values:
                sql = """
                    INSERT INTO location(path) VALUES %s
                      ON CONFLICT DO NOTHING
                    """
                with self.transaction() as cursor:
                    psycopg2.extras.execute_values(cursor, sql, argslist=values)
            return True
        except:  # noqa: E722
            # Unexpected error occurred, rollback all changes and log message
            LOGGER.exception("Unexpected error")
            if self.raise_on_commit:
                raise
        return False

    def location_get_all(self) -> Set[bytes]:
        with self.transaction(readonly=True) as cursor:
            cursor.execute("SELECT location.path AS path FROM location")
            return {row["path"] for row in cursor}

    def origin_add(self, orgs: Dict[Sha1Git, str]) -> bool:
        try:
            if orgs:
                sql = """
                    INSERT INTO origin(sha1, url) VALUES %s
                      ON CONFLICT DO NOTHING
                    """
                with self.transaction() as cursor:
                    psycopg2.extras.execute_values(
                        cur=cursor, sql=sql, argslist=orgs.items()
                    )
            return True
        except:  # noqa: E722
            # Unexpected error occurred, rollback all changes and log message
            LOGGER.exception("Unexpected error")
            if self.raise_on_commit:
                raise
        return False

    def origin_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, str]:
        urls: Dict[Sha1Git, str] = {}
        sha1s = tuple(ids)
        if sha1s:
            # TODO: consider splitting this query in several ones if sha1s is too big!
            values = ", ".join(itertools.repeat("%s", len(sha1s)))
            sql = f"""
                SELECT sha1, url
                  FROM origin
                  WHERE sha1 IN ({values})
                """
            with self.transaction(readonly=True) as cursor:
                cursor.execute(query=sql, vars=sha1s)
                urls.update((row["sha1"], row["url"]) for row in cursor)
        return urls

    def revision_add(
        self, revs: Union[Iterable[Sha1Git], Dict[Sha1Git, RevisionData]]
    ) -> bool:
        if isinstance(revs, dict):
            data = [(sha1, rev.date, rev.origin) for sha1, rev in revs.items()]
        else:
            data = [(sha1, None, None) for sha1 in revs]
        try:
            if data:
                sql = """
                    INSERT INTO revision(sha1, date, origin)
                      (SELECT V.rev AS sha1, V.date::timestamptz AS date, O.id AS origin
                       FROM (VALUES %s) AS V(rev, date, org)
                       LEFT JOIN origin AS O ON (O.sha1=V.org::sha1_git))
                      ON CONFLICT (sha1) DO
                      UPDATE SET
                        date=LEAST(EXCLUDED.date, revision.date),
                        origin=COALESCE(EXCLUDED.origin, revision.origin)
                    """
                with self.transaction() as cursor:
                    psycopg2.extras.execute_values(cur=cursor, sql=sql, argslist=data)
            return True
        except:  # noqa: E722
            # Unexpected error occurred, rollback all changes and log message
            LOGGER.exception("Unexpected error")
            if self.raise_on_commit:
                raise
        return False

    def revision_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, RevisionData]:
        result: Dict[Sha1Git, RevisionData] = {}
        sha1s = tuple(ids)
        if sha1s:
            # TODO: consider splitting this query in several ones if sha1s is too big!
            values = ", ".join(itertools.repeat("%s", len(sha1s)))
            sql = f"""
                SELECT R.sha1, R.date, O.sha1 AS origin
                  FROM revision AS R
                  LEFT JOIN origin AS O ON (O.id=R.origin)
                  WHERE R.sha1 IN ({values})
                    AND (R.date is not NULL OR O.sha1 is not NULL)
                """
            with self.transaction(readonly=True) as cursor:
                cursor.execute(query=sql, vars=sha1s)
                result.update(
                    (row["sha1"], RevisionData(date=row["date"], origin=row["origin"]))
                    for row in cursor
                )
        return result

    def relation_add(
        self, relation: RelationType, data: Iterable[RelationData]
    ) -> bool:
        rows = [(rel.src, rel.dst, rel.path) for rel in data]
        try:
            if rows:
                rel_table = relation.value
                src_table, *_, dst_table = rel_table.split("_")

                # Put the next three queries in a manual single transaction:
                # they use the same temp table
                with self.transaction() as cursor:
                    cursor.execute("SELECT swh_mktemp_relation_add()")
                    psycopg2.extras.execute_values(
                        cur=cursor,
                        sql="INSERT INTO tmp_relation_add(src, dst, path) VALUES %s",
                        argslist=rows,
                    )
                    sql = "SELECT swh_provenance_relation_add_from_temp(%s, %s, %s)"
                    cursor.execute(query=sql, vars=(rel_table, src_table, dst_table))
            return True
        except:  # noqa: E722
            # Unexpected error occurred, rollback all changes and log message
            LOGGER.exception("Unexpected error")
            if self.raise_on_commit:
                raise
        return False

    def relation_get(
        self, relation: RelationType, ids: Iterable[Sha1Git], reverse: bool = False
    ) -> Set[RelationData]:
        return self._relation_get(relation, ids, reverse)

    def relation_get_all(self, relation: RelationType) -> Set[RelationData]:
        return self._relation_get(relation, None)

    def _entity_get_date(
        self,
        entity: Literal["content", "directory", "revision"],
        ids: Iterable[Sha1Git],
    ) -> Dict[Sha1Git, datetime]:
        dates: Dict[Sha1Git, datetime] = {}
        sha1s = tuple(ids)
        if sha1s:
            # TODO: consider splitting this query in several ones if sha1s is too big!
            values = ", ".join(itertools.repeat("%s", len(sha1s)))
            sql = f"""
                SELECT sha1, date
                  FROM {entity}
                  WHERE sha1 IN ({values})
                    AND date IS NOT NULL
                """
            with self.transaction(readonly=True) as cursor:
                cursor.execute(query=sql, vars=sha1s)
                dates.update((row["sha1"], row["date"]) for row in cursor)
        return dates

    def _entity_set_date(
        self,
        entity: Literal["content", "directory"],
        dates: Union[Iterable[Sha1Git], Dict[Sha1Git, datetime]],
    ) -> bool:
        data = dates if isinstance(dates, dict) else dict.fromkeys(dates)
        try:
            if data:
                sql = f"""
                    INSERT INTO {entity}(sha1, date) VALUES %s
                      ON CONFLICT (sha1) DO
                      UPDATE SET date=LEAST(EXCLUDED.date,{entity}.date)
                    """
                with self.transaction() as cursor:
                    psycopg2.extras.execute_values(cursor, sql, argslist=data.items())
            return True
        except:  # noqa: E722
            # Unexpected error occurred, rollback all changes and log message
            LOGGER.exception("Unexpected error")
            if self.raise_on_commit:
                raise
        return False

    def _relation_get(
        self,
        relation: RelationType,
        ids: Optional[Iterable[Sha1Git]],
        reverse: bool = False,
    ) -> Set[RelationData]:
        result: Set[RelationData] = set()

        sha1s: List[Sha1Git]
        if ids is not None:
            sha1s = list(ids)
            filter = "filter-src" if not reverse else "filter-dst"
        else:
            sha1s = []
            filter = "no-filter"

        if filter == "no-filter" or sha1s:
            rel_table = relation.value
            src_table, *_, dst_table = rel_table.split("_")

            sql = "SELECT * FROM swh_provenance_relation_get(%s, %s, %s, %s, %s)"
            with self.transaction(readonly=True) as cursor:
                cursor.execute(
                    query=sql, vars=(rel_table, src_table, dst_table, filter, sha1s)
                )
                result.update(RelationData(**row) for row in cursor)
        return result

    def with_path(self) -> bool:
        return "with-path" in self.flavor
