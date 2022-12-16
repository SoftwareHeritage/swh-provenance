# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor
from concurrent.futures import wait as futures_wait
from contextlib import contextmanager
import hashlib
import logging
from queue import Full, Queue
import sys
import threading
import time

import plyvel

try:
    from systemd.daemon import notify
except ImportError:
    notify = None

from swh.core.db import BaseDb
from swh.provenance.storage.journal import (
    JournalMessage,
    ProvenanceStorageJournalWriter,
)
from swh.storage.backfill import byte_ranges

logger = logging.getLogger(__name__)


status_db_prefix = {
    "content": b"cnt:",
    "revision": b"rev:",
    "directory": b"dir:",
    "content_in_revision": b"CiR:",
    "content_in_directory": b"CiD:",
    "directory_in_revision": b"DiR:",
}


class SimplePB:
    UP = "\033[1A"

    def __init__(self, etype, pos, bits):
        self.vpos = pos
        self.bits = bits
        self.etype = etype
        self.fp = sys.stdout
        self.rows = 0
        self.lastn = 0
        self.range_pos = 0
        self.t0 = time.monotonic()
        self.ts = time.monotonic()

    def mv(self, n):
        self.fp.write("\n" * n + self.UP * -n + "\r")
        self.fp.flush()

    @contextmanager
    def gotopos(self):
        self.mv(self.vpos)
        try:
            yield
        finally:
            self.mv(-self.vpos)

    def write(self, msg):
        with self.gotopos():
            self.fp.write(msg)
            self.fp.flush()

    def update(self, n, start, stop):
        self.rows += n
        self.range_pos = max(self.range_pos, int.from_bytes(stop, "big"))
        dt = time.monotonic() - self.ts
        if dt > 2:
            msg = (
                f"#{self.vpos} {self.etype}: \t"
                f"{hex(self.range_pos)}/{hex(2**self.bits)} "
                f"@{(self.rows-self.lastn)/dt:.2f} row/s     "
            )
            self.write(msg)
            self.lastn = self.rows
            self.ts = time.monotonic()


class Backfiller:
    def __init__(self, conn_args, journal, status_db_path, concurrency=16):
        self.journal = ProvenanceStorageJournalWriter(journal)
        self._db = None
        self.db_config = conn_args
        self.concurrency = concurrency
        self.status_db_path = status_db_path
        self.status_db = plyvel.DB(self.status_db_path, create_if_missing=True)
        self.exiting = threading.Event()
        self.pbs = {}

    def stop(self):
        self.exiting.set()

    @property
    def db(self):
        if self._db is None:
            self._db = BaseDb.connect(**self.db_config)
        return self._db

    def tag_range_done(self, etype, start, stop):
        db = self.status_db.prefixed_db(status_db_prefix[etype])
        db.put(start, stop)
        # TODO: handle range overlap computation properly (maybe?)

    def is_range_done(self, etype, start, stop):
        db = self.status_db.prefixed_db(status_db_prefix[etype])
        oldstop = db.get(start)
        if oldstop is not None:
            if oldstop != stop:
                raise ValueError("Range already exists but with a different stop")
            return True
        return False
        # TODO: handle range overlap computation properly (maybe?)

    def byte_ranges(self, etype, bits):
        for start, stop in byte_ranges(bits):
            try:
                from math import ceil

                if start is None:
                    start = b"\x00" * ceil(bits / 8)
                if stop is None:
                    stop = b"\xff" * 20
                if not self.is_range_done(etype, start, stop):
                    yield (start, stop)
                else:
                    logger.info(
                        "Skipping range %s: [%s, %s)", etype, start.hex(), stop.hex()
                    )
            except Exception as exc:
                logger.error("Argh %s", exc)
                raise

    def run(self):
        queue = Queue(self.concurrency)
        with ThreadPoolExecutor(max_workers=self.concurrency + 1) as pool:
            futures = []
            for i in range(self.concurrency):
                f = pool.submit(self.backfill_worker, queue=queue, exiting=self.exiting)
                futures.append(f)
            futures.append(
                pool.submit(self.queue_producer, queue=queue, exiting=self.exiting)
            )
            futures_wait(futures, return_when=FIRST_EXCEPTION)

    def queue_producer(self, queue, exiting):
        logger.info("Starting the producer worker")
        ranges = [
            ("content", 20),  # ~12k rows per loop, 1M loops
            ("revision", 18),  # ~11k rows per loop, 256k loops
            ("directory", 20),  # ~10k rows per loop, 1M loops
            ("content_in_revision", 24),  # ~11k rows per loop, 16M loops
            ("content_in_directory", 22),  # ~15k rows per loop, 4M loops
            ("directory_in_revision", 25),  # ~12k rows per loop, 32M loops
        ]

        range_gens = [
            (etype, self.byte_ranges(etype, range_bits)) for etype, range_bits in ranges
        ]
        try:
            self.pbs.update(
                {
                    etype: SimplePB(etype, i, bits)
                    for i, (etype, bits) in enumerate(ranges)
                }
            )
        except Exception as exc:
            print("Arghhhh", exc)
            raise
        while range_gens:
            etype, range_gen = range_gens.pop(0)
            try:
                start, stop = next(range_gen)
                range_gens.append((etype, range_gen))
                logger.debug(
                    "Adding range %s: [%s, %s)", etype, start.hex(), stop.hex()
                )
                try:
                    queue.put((etype, start, stop), timeout=1)
                except Full:
                    if exiting.is_set():
                        logger.debug("Exiting producer")
                        return
            except StopIteration:
                logger.info("range generator for %s is over, removing", etype)

    def backfill_worker(self, queue, exiting):
        logger.info("Spawning backfiller worker %s", threading.current_thread().name)
        while not exiting.is_set():
            etype, start, stop = queue.get()
            logger.info("backfill %s [%s, %s)", etype, start.hex(), stop.hex())
            backfiller = getattr(self, f"backfill_{etype}")
            try:
                n = backfiller(start=start, stop=stop)
            except Exception as e:
                print("argh", etype, e, repr(e))
                raise
            self.tag_range_done(etype, start, stop)
            pb = self.pbs.get(etype)
            if pb:
                pb.update(n, start, stop)
            logger.debug("backfill %s [%s, %s) DONE", etype, start.hex(), stop.hex())
            if notify:
                notify("WATCHDOG=1")
        logger.debug("Exiting backfill worker %s", threading.current_thread().name)

    def backfill_content(self, start, stop):
        sql = (
            "select C.sha1, C.date "
            "from content as C "
            "where C.sha1 >= %(start)s and C.sha1 < %(stop)s"
        )
        with self.db.cursor() as cursor:
            cursor.execute(sql, {"start": start, "stop": stop})
            self.journal.journal.write_additions(
                "content", [JournalMessage(sha1, date) for sha1, date in cursor]
            )
            return cursor.rowcount

    def backfill_revision(self, start, stop):
        sql = (
            "select R.sha1, R.date "
            "from revision as R "
            # "  left join origin as O on R.origin=O.id "
            "where R.sha1 >= %(start)s and R.sha1 < %(stop)s"
        )
        with self.db.cursor() as cursor:
            cursor.execute(sql, {"start": start, "stop": stop})
            self.journal.journal.write_additions(
                "revision", [JournalMessage(sha1, date) for sha1, date in cursor]
            )
            return cursor.rowcount

    def backfill_directory(self, start, stop):
        sql = (
            "select D.sha1, D.date "
            "from directory as D "
            "where D.sha1 >= %(start)s and D.sha1 < %(stop)s"
        )
        with self.db.cursor() as cursor:
            cursor.execute(sql, {"start": start, "stop": stop})
            self.journal.journal.write_additions(
                "directory", [JournalMessage(sha1, date) for sha1, date in cursor]
            )
            return cursor.rowcount

    def backfill_content_in_revision(self, start, stop):
        sql = (
            "select C.sha1, R.sha1, L.path, R.date "
            "from content_in_revision as CR "
            "  inner join content as C on CR.content=C.id "
            "  inner join revision as R on CR.revision=R.id "
            "  inner join location as L on CR.location=L.id "
            "where "
            " C.sha1 >= %(start)s and C.sha1 < %(stop)s"
        )
        messages = []
        with self.db.cursor() as cursor:
            cursor.execute(sql, {"start": start, "stop": stop})
            for content_hash, revision_hash, location, date in cursor:
                key = hashlib.sha1(content_hash + revision_hash + location).digest()
                messages.append(
                    JournalMessage(
                        key,
                        {
                            "src": content_hash,
                            "dst": revision_hash,
                            "path": location,
                            "dst_date": date,
                        },
                        add_id=False,
                    )
                )
        self.journal.journal.write_additions("content_in_revision", messages)
        return len(messages)

    def backfill_content_in_directory(self, start, stop):
        sql = (
            "select C.sha1, D.sha1, L.path, D.date "
            "from content_in_directory as CD "
            "  inner join content as C on CD.content=C.id "
            "  inner join directory as D on CD.directory=D.id "
            "  inner join location as L on CD.location=L.id "
            "where "
            " C.sha1 >= %(start)s and C.sha1 < %(stop)s"
        )
        messages = []
        with self.db.cursor() as cursor:
            cursor.execute(sql, {"start": start, "stop": stop})
            for content_hash, directory_hash, location, date in cursor:
                key = hashlib.sha1(content_hash + directory_hash + location).digest()
                messages.append(
                    JournalMessage(
                        key,
                        {
                            "src": content_hash,
                            "dst": directory_hash,
                            "path": location,
                            "dst_date": date,
                        },
                        add_id=False,
                    )
                )
        self.journal.journal.write_additions("content_in_directory", messages)
        return len(messages)

    def backfill_directory_in_revision(self, start, stop):
        sql = (
            "select D.sha1, R.sha1, L.path, R.date "
            "from directory_in_revision as DR "
            "  inner join directory as D on DR.directory=D.id "
            "  inner join revision as R on DR.revision=R.id "
            "  inner join location as L on DR.location=L.id "
            "where "
            " D.sha1 >= %(start)s and D.sha1 < %(stop)s"
        )
        messages = []
        with self.db.cursor() as cursor:
            cursor.execute(sql, {"start": start, "stop": stop})
            for directory_hash, revision_hash, location, date in cursor:
                key = hashlib.sha1(directory_hash + revision_hash + location).digest()
                messages.append(
                    JournalMessage(
                        key,
                        {
                            "src": directory_hash,
                            "dst": revision_hash,
                            "path": location,
                            "dst_date": date,
                        },
                        add_id=False,
                    )
                )
        self.journal.journal.write_additions("directory_in_revision", messages)
        return len(messages)
