# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from functools import wraps
import itertools
import logging
from types import TracebackType
from typing import Dict, Generator, Iterable, List, Optional, Set, Type, Union

import psycopg2.extensions
import psycopg2.extras

from swh.core.db import BaseDb
from swh.core.statsd import statsd
from swh.model.model import Sha1Git

from ..interface import (
    DirectoryData,
    EntityType,
    ProvenanceResult,
    ProvenanceStorageInterface,
    RelationData,
    RelationType,
    RevisionData,
)

LOGGER = logging.getLogger(__name__)

STORAGE_DURATION_METRIC = "swh_provenance_storage_postgresql_duration_seconds"


def handle_raise_on_commit(f):
    @wraps(f)
    def handle(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except BaseException as ex:
            # Unexpected error occurred, rollback all changes and log message
            LOGGER.exception("Unexpected error")
            if self.raise_on_commit:
                raise ex
            return False

    return handle


class ProvenanceStoragePostgreSql:
    def __init__(
        self, page_size: Optional[int] = None, raise_on_commit: bool = False, **kwargs
    ) -> None:
        self.conn_args = kwargs
        self._flavor: Optional[str] = None
        self.page_size = page_size
        self.raise_on_commit = raise_on_commit

    def __enter__(self) -> ProvenanceStorageInterface:
        self.open()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

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

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "close"})
    def close(self) -> None:
        self.conn.close()

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "content_add"})
    @handle_raise_on_commit
    def content_add(self, cnts: Dict[Sha1Git, datetime]) -> bool:
        if cnts:
            sql = """
                INSERT INTO content(sha1, date) VALUES %s
                  ON CONFLICT (sha1) DO
                  UPDATE SET date=LEAST(EXCLUDED.date,content.date)
                """
            page_size = self.page_size or len(cnts)
            with self.transaction() as cursor:
                psycopg2.extras.execute_values(
                    cursor, sql, argslist=cnts.items(), page_size=page_size
                )
        return True

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "content_find_first"})
    def content_find_first(self, id: Sha1Git) -> Optional[ProvenanceResult]:
        sql = "SELECT * FROM swh_provenance_content_find_first(%s)"
        with self.transaction(readonly=True) as cursor:
            cursor.execute(query=sql, vars=(id,))
            row = cursor.fetchone()
        return ProvenanceResult(**row) if row is not None else None

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "content_find_all"})
    def content_find_all(
        self, id: Sha1Git, limit: Optional[int] = None
    ) -> Generator[ProvenanceResult, None, None]:
        sql = "SELECT * FROM swh_provenance_content_find_all(%s, %s)"
        with self.transaction(readonly=True) as cursor:
            cursor.execute(query=sql, vars=(id, limit))
            yield from (ProvenanceResult(**row) for row in cursor)

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "content_get"})
    def content_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, datetime]:
        dates: Dict[Sha1Git, datetime] = {}
        sha1s = tuple(ids)
        if sha1s:
            # TODO: consider splitting this query in several ones if sha1s is too big!
            values = ", ".join(itertools.repeat("%s", len(sha1s)))
            sql = f"""
                SELECT sha1, date
                  FROM content
                  WHERE sha1 IN ({values})
                    AND date IS NOT NULL
                """
            with self.transaction(readonly=True) as cursor:
                cursor.execute(query=sql, vars=sha1s)
                dates.update((row["sha1"], row["date"]) for row in cursor)
        return dates

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "directory_add"})
    @handle_raise_on_commit
    def directory_add(self, dirs: Dict[Sha1Git, DirectoryData]) -> bool:
        data = [(sha1, rev.date, rev.flat) for sha1, rev in dirs.items()]
        if data:
            sql = """
                INSERT INTO directory(sha1, date, flat) VALUES %s
                  ON CONFLICT (sha1) DO
                  UPDATE SET
                    date=LEAST(EXCLUDED.date, directory.date),
                    flat=(EXCLUDED.flat OR directory.flat)
                """
            page_size = self.page_size or len(data)
            with self.transaction() as cursor:
                psycopg2.extras.execute_values(
                    cur=cursor, sql=sql, argslist=data, page_size=page_size
                )
        return True

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "directory_get"})
    def directory_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, DirectoryData]:
        result: Dict[Sha1Git, DirectoryData] = {}
        sha1s = tuple(ids)
        if sha1s:
            # TODO: consider splitting this query in several ones if sha1s is too big!
            values = ", ".join(itertools.repeat("%s", len(sha1s)))
            sql = f"""
                SELECT sha1, date, flat
                  FROM directory
                  WHERE sha1 IN ({values})
                    AND date IS NOT NULL
                """
            with self.transaction(readonly=True) as cursor:
                cursor.execute(query=sql, vars=sha1s)
                result.update(
                    (row["sha1"], DirectoryData(date=row["date"], flat=row["flat"]))
                    for row in cursor
                )
        return result

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "entity_get_all"})
    def entity_get_all(self, entity: EntityType) -> Set[Sha1Git]:
        with self.transaction(readonly=True) as cursor:
            cursor.execute(f"SELECT sha1 FROM {entity.value}")
            return {row["sha1"] for row in cursor}

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "location_add"})
    @handle_raise_on_commit
    def location_add(self, paths: Iterable[bytes]) -> bool:
        if self.with_path():
            values = [(path,) for path in paths]
            if values:
                sql = """
                    INSERT INTO location(path) VALUES %s
                      ON CONFLICT DO NOTHING
                    """
                page_size = self.page_size or len(values)
                with self.transaction() as cursor:
                    psycopg2.extras.execute_values(
                        cursor, sql, argslist=values, page_size=page_size
                    )
        return True

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "location_get_all"})
    def location_get_all(self) -> Set[bytes]:
        with self.transaction(readonly=True) as cursor:
            cursor.execute("SELECT location.path AS path FROM location")
            return {row["path"] for row in cursor}

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "origin_add"})
    @handle_raise_on_commit
    def origin_add(self, orgs: Dict[Sha1Git, str]) -> bool:
        if orgs:
            sql = """
                INSERT INTO origin(sha1, url) VALUES %s
                  ON CONFLICT DO NOTHING
                """
            page_size = self.page_size or len(orgs)
            with self.transaction() as cursor:
                psycopg2.extras.execute_values(
                    cur=cursor,
                    sql=sql,
                    argslist=orgs.items(),
                    page_size=page_size,
                )
        return True

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "open"})
    def open(self) -> None:
        self.conn = BaseDb.connect(**self.conn_args).conn
        BaseDb.adapt_conn(self.conn)
        with self.transaction() as cursor:
            cursor.execute("SET timezone TO 'UTC'")

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "origin_get"})
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

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "revision_add"})
    @handle_raise_on_commit
    def revision_add(
        self, revs: Union[Iterable[Sha1Git], Dict[Sha1Git, RevisionData]]
    ) -> bool:
        if isinstance(revs, dict):
            data = [(sha1, rev.date, rev.origin) for sha1, rev in revs.items()]
        else:
            data = [(sha1, None, None) for sha1 in revs]
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
            page_size = self.page_size or len(data)
            with self.transaction() as cursor:
                psycopg2.extras.execute_values(
                    cur=cursor, sql=sql, argslist=data, page_size=page_size
                )
        return True

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "revision_get"})
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

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "relation_add"})
    @handle_raise_on_commit
    def relation_add(
        self, relation: RelationType, data: Dict[Sha1Git, Set[RelationData]]
    ) -> bool:
        rows = [(src, rel.dst, rel.path) for src, dsts in data.items() for rel in dsts]
        if rows:
            rel_table = relation.value
            src_table, *_, dst_table = rel_table.split("_")
            page_size = self.page_size or len(rows)
            # Put the next three queries in a manual single transaction:
            # they use the same temp table
            with self.transaction() as cursor:
                cursor.execute("SELECT swh_mktemp_relation_add()")
                psycopg2.extras.execute_values(
                    cur=cursor,
                    sql="INSERT INTO tmp_relation_add(src, dst, path) VALUES %s",
                    argslist=rows,
                    page_size=page_size,
                )
                sql = "SELECT swh_provenance_relation_add_from_temp(%s, %s, %s)"
                cursor.execute(query=sql, vars=(rel_table, src_table, dst_table))
        return True

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "relation_get"})
    def relation_get(
        self, relation: RelationType, ids: Iterable[Sha1Git], reverse: bool = False
    ) -> Dict[Sha1Git, Set[RelationData]]:
        return self._relation_get(relation, ids, reverse)

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "relation_get_all"})
    def relation_get_all(
        self, relation: RelationType
    ) -> Dict[Sha1Git, Set[RelationData]]:
        return self._relation_get(relation, None)

    def _relation_get(
        self,
        relation: RelationType,
        ids: Optional[Iterable[Sha1Git]],
        reverse: bool = False,
    ) -> Dict[Sha1Git, Set[RelationData]]:
        result: Dict[Sha1Git, Set[RelationData]] = {}

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
                for row in cursor:
                    src = row.pop("src")
                    result.setdefault(src, set()).add(RelationData(**row))
        return result

    @statsd.timed(metric=STORAGE_DURATION_METRIC, tags={"method": "with_path"})
    def with_path(self) -> bool:
        return "with-path" in self.flavor
