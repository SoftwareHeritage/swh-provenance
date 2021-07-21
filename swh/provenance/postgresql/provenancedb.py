# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
import itertools
import logging
from typing import Dict, Generator, Iterable, Optional, Set, Tuple

import psycopg2
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


class ProvenanceDB:
    def __init__(
        self, conn: psycopg2.extensions.connection, raise_on_commit: bool = False
    ):
        BaseDb.adapt_conn(conn)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn.set_session(autocommit=True)
        self.conn = conn
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # XXX: not sure this is the best place to do it!
        sql = "SET timezone TO 'UTC'"
        self.cursor.execute(sql)
        self._flavor: Optional[str] = None
        self.raise_on_commit = raise_on_commit

    @property
    def flavor(self) -> str:
        if self._flavor is None:
            sql = "SELECT swh_get_dbflavor() AS flavor"
            self.cursor.execute(sql)
            self._flavor = self.cursor.fetchone()["flavor"]
        assert self._flavor is not None
        return self._flavor

    def with_path(self) -> bool:
        return "with-path" in self.flavor

    @property
    def denormalized(self) -> bool:
        return "denormalized" in self.flavor

    def content_set_date(self, dates: Dict[Sha1Git, datetime]) -> bool:
        return self._entity_set_date("content", dates)

    def content_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, datetime]:
        return self._entity_get_date("content", ids)

    def directory_set_date(self, dates: Dict[Sha1Git, datetime]) -> bool:
        return self._entity_set_date("directory", dates)

    def directory_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, datetime]:
        return self._entity_get_date("directory", ids)

    def entity_get_all(self, entity: EntityType) -> Set[Sha1Git]:
        sql = f"SELECT sha1 FROM {entity.value}"
        self.cursor.execute(sql)
        return {row["sha1"] for row in self.cursor.fetchall()}

    def location_get(self) -> Set[bytes]:
        sql = "SELECT encode(location.path::bytea, 'escape') AS path FROM location"
        self.cursor.execute(sql)
        return {row["path"] for row in self.cursor.fetchall()}

    def origin_set_url(self, urls: Dict[Sha1Git, str]) -> bool:
        try:
            if urls:
                sql = """
                    LOCK TABLE ONLY origin;
                    INSERT INTO origin(sha1, url) VALUES %s
                      ON CONFLICT DO NOTHING
                    """
                psycopg2.extras.execute_values(self.cursor, sql, urls.items())
            return True
        except:  # noqa: E722
            # Unexpected error occurred, rollback all changes and log message
            logging.exception("Unexpected error")
            if self.raise_on_commit:
                raise
        return False

    def origin_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, str]:
        urls: Dict[Sha1Git, str] = {}
        sha1s = tuple(ids)
        if sha1s:
            values = ", ".join(itertools.repeat("%s", len(sha1s)))
            sql = f"""
                SELECT sha1, url
                  FROM origin
                  WHERE sha1 IN ({values})
                """
            self.cursor.execute(sql, sha1s)
            urls.update((row["sha1"], row["url"]) for row in self.cursor.fetchall())
        return urls

    def revision_set_date(self, dates: Dict[Sha1Git, datetime]) -> bool:
        return self._entity_set_date("revision", dates)

    def content_find_first(self, id: Sha1Git) -> Optional[ProvenanceResult]:
        sql = "select * from swh_provenance_content_find_first(%s)"
        self.cursor.execute(sql, (id,))
        row = self.cursor.fetchone()
        return ProvenanceResult(**row) if row is not None else None

    def content_find_all(
        self, id: Sha1Git, limit: Optional[int] = None
    ) -> Generator[ProvenanceResult, None, None]:
        sql = "select * from swh_provenance_content_find_all(%s, %s)"
        self.cursor.execute(sql, (id, limit))
        yield from (ProvenanceResult(**row) for row in self.cursor.fetchall())

    def revision_set_origin(self, origins: Dict[Sha1Git, Sha1Git]) -> bool:
        try:
            if origins:
                sql = """
                    LOCK TABLE ONLY revision;
                    INSERT INTO revision(sha1, origin)
                      (SELECT V.rev AS sha1, O.id AS origin
                       FROM (VALUES %s) AS V(rev, org)
                       JOIN origin AS O ON (O.sha1=V.org))
                      ON CONFLICT (sha1) DO
                      UPDATE SET origin=EXCLUDED.origin
                    """
                psycopg2.extras.execute_values(self.cursor, sql, origins.items())
            return True
        except:  # noqa: E722
            # Unexpected error occurred, rollback all changes and log message
            logging.exception("Unexpected error")
            if self.raise_on_commit:
                raise
        return False

    def revision_get(self, ids: Iterable[Sha1Git]) -> Dict[Sha1Git, RevisionData]:
        result: Dict[Sha1Git, RevisionData] = {}
        sha1s = tuple(ids)
        if sha1s:
            values = ", ".join(itertools.repeat("%s", len(sha1s)))
            sql = f"""
                SELECT sha1, date, origin
                  FROM revision
                  WHERE sha1 IN ({values})
                """
            self.cursor.execute(sql, sha1s)
            result.update(
                (row["sha1"], RevisionData(date=row["date"], origin=row["origin"]))
                for row in self.cursor.fetchall()
            )
        return result

    def relation_add(
        self, relation: RelationType, data: Iterable[RelationData]
    ) -> bool:
        try:
            rows = tuple((rel.src, rel.dst, rel.path) for rel in data)
            if rows:
                table = relation.value
                src, *_, dst = table.split("_")

                if src != "origin":
                    # Origin entries should be inserted previously as they require extra
                    # non-null information
                    srcs = tuple(set((sha1,) for (sha1, _, _) in rows))
                    sql = f"""
                        LOCK TABLE ONLY {src};
                        INSERT INTO {src}(sha1) VALUES %s
                          ON CONFLICT DO NOTHING
                        """
                    psycopg2.extras.execute_values(self.cursor, sql, srcs)
                if dst != "origin":
                    # Origin entries should be inserted previously as they require extra
                    # non-null information
                    dsts = tuple(set((sha1,) for (_, sha1, _) in rows))
                    sql = f"""
                        LOCK TABLE ONLY {dst};
                        INSERT INTO {dst}(sha1) VALUES %s
                          ON CONFLICT DO NOTHING
                        """
                    psycopg2.extras.execute_values(self.cursor, sql, dsts)
                joins = [
                    f"INNER JOIN {src} AS S ON (S.sha1=V.src)",
                    f"INNER JOIN {dst} AS D ON (D.sha1=V.dst)",
                ]
                nope = (RelationType.REV_BEFORE_REV, RelationType.REV_IN_ORG)
                selected = ["S.id"]
                if self.denormalized and relation not in nope:
                    selected.append("ARRAY_AGG(D.id)")
                else:
                    selected.append("D.id")

                if self._relation_uses_location_table(relation):
                    locations = tuple(set((path,) for (_, _, path) in rows))
                    sql = """
                        LOCK TABLE ONLY location;
                        INSERT INTO location(path) VALUES %s
                          ON CONFLICT (path) DO NOTHING
                        """
                    psycopg2.extras.execute_values(self.cursor, sql, locations)

                    joins.append("INNER JOIN location AS L ON (L.path=V.path)")
                    if self.denormalized:
                        selected.append("ARRAY_AGG(L.id)")
                    else:
                        selected.append("L.id")
                sql_l = [
                    f"INSERT INTO {table}",
                    f" SELECT {', '.join(selected)}",
                    "  FROM (VALUES %s) AS V(src, dst, path)",
                    *joins,
                ]

                if self.denormalized and relation not in nope:
                    sql_l.append("GROUP BY S.id")
                    sql_l.append(
                        f"""ON CONFLICT ({src}) DO UPDATE
                        SET {dst}=ARRAY(
                          SELECT UNNEST({table}.{dst} || excluded.{dst})),
                        location=ARRAY(
                          SELECT UNNEST({relation.value}.location || excluded.location))
                    """
                    )
                else:
                    sql_l.append("ON CONFLICT DO NOTHING")
                sql = "\n".join(sql_l)
                psycopg2.extras.execute_values(self.cursor, sql, rows)
            return True
        except:  # noqa: E722
            # Unexpected error occurred, rollback all changes and log message
            logging.exception("Unexpected error")
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
            values = ", ".join(itertools.repeat("%s", len(sha1s)))
            sql = f"""
                SELECT sha1, date
                  FROM {entity}
                  WHERE sha1 IN ({values})
                """
            self.cursor.execute(sql, sha1s)
            dates.update((row["sha1"], row["date"]) for row in self.cursor.fetchall())
        return dates

    def _entity_set_date(
        self,
        entity: Literal["content", "directory", "revision"],
        data: Dict[Sha1Git, datetime],
    ) -> bool:
        try:
            if data:
                sql = f"""
                    LOCK TABLE ONLY {entity};
                    INSERT INTO {entity}(sha1, date) VALUES %s
                      ON CONFLICT (sha1) DO
                      UPDATE SET date=LEAST(EXCLUDED.date,{entity}.date)
                    """
                psycopg2.extras.execute_values(self.cursor, sql, data.items())
            return True
        except:  # noqa: E722
            # Unexpected error occurred, rollback all changes and log message
            logging.exception("Unexpected error")
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

        sha1s: Optional[Tuple[Tuple[Sha1Git, ...]]]
        if ids is not None:
            sha1s = (tuple(ids),)
            where = f"WHERE {'S' if not reverse else 'D'}.sha1 IN %s"
        else:
            sha1s = None
            where = ""

        aggreg_dst = self.denormalized and relation in (
            RelationType.CNT_EARLY_IN_REV,
            RelationType.CNT_IN_DIR,
            RelationType.DIR_IN_REV,
        )
        if sha1s is None or sha1s[0]:
            table = relation.value
            src, *_, dst = table.split("_")

            # TODO: improve this!
            if src == "revision" and dst == "revision":
                src_field = "prev"
                dst_field = "next"
            else:
                src_field = src
                dst_field = dst

            if aggreg_dst:
                revloc = f"UNNEST(R.{dst_field}) AS dst"
                if self._relation_uses_location_table(relation):
                    revloc += ", UNNEST(R.location) AS path"
            else:
                revloc = f"R.{dst_field} AS dst"
                if self._relation_uses_location_table(relation):
                    revloc += ", R.location AS path"

            inner_sql = f"""
            SELECT S.sha1 AS src, {revloc}
            FROM {table} AS R
            INNER JOIN {src} AS S ON (S.id=R.{src_field})
            {where}
            """

            if self._relation_uses_location_table(relation):
                loc = "L.path AS path"
            else:
                loc = "NULL AS path"
            sql = f"""
            SELECT CL.src, D.sha1 AS dst, {loc}
            FROM ({inner_sql}) AS CL
            INNER JOIN {dst} AS D ON (D.id=CL.dst)
            """
            if self._relation_uses_location_table(relation):
                sql += "INNER JOIN location AS L ON (L.id=CL.path)"

            self.cursor.execute(sql, sha1s)
            result.update(RelationData(**row) for row in self.cursor.fetchall())
        return result

    def _relation_uses_location_table(self, relation: RelationType) -> bool:
        if self.with_path():
            src = relation.value.split("_")[0]
            return src in ("content", "directory")
        return False
