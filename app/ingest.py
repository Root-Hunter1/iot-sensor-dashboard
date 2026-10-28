"""UDP ingest server.

Which Python feature does what:

  multiprocessing  N receiver processes share one UDP port (SO_REUSEPORT),
                   so the kernel spreads packets across CPU cores
  asyncio          each receiver runs an event loop. The UDP socket is
                   registered with loop.add_reader() and woken when data arrives
  ctypes + recvmmsg  fastrecv.py reads many datagrams per system call
  queue            multiprocessing.Queue carries batches from receivers to the writer
  threading        a writer thread in the main process moves batches into SQLite

Data flow:

    receiver processes (asyncio + recvmmsg)
        -> parse, validate, batch
        -> mp.Queue
    writer thread (main process)
        -> batched INSERTs into SQLite

Run:
    INGEST_KEY=secret python -m app.ingest --port 9999 --workers 2 --recv recvmmsg
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import multiprocessing as mp
import os
import queue
import signal
import socket
import sqlite3
import threading
import time

from app import storage
from app.fastrecv import make_reader
from app.protocol import MAX_PACKET_BYTES, PacketError, Reading, parse_packet

log = logging.getLogger("ingest")

BATCH_SIZE = 500            # flush when this many readings are waiting
FLUSH_INTERVAL = 0.2        # ...or this many seconds have passed
STATS_INTERVAL = 10.0       # how often a receiver prints its counters
QUEUE_MAXSIZE = 1000        # batches waiting for the writer (back-pressure limit)
RCVBUF_BYTES = 8 * 1024 * 1024  # kernel buffer per socket; bursts are lost when it fills
RECV_VLEN = 64              # datagrams per recvmmsg call
DRAIN_ROUNDS = 16           # read calls per wakeup, so a busy socket cannot starve the loop


class Receiver:
    """Turns raw datagrams into validated Reading batches. It does no network I/O itself."""

    def __init__(self, reader, out_q, shared_key: str, worker_id: int):
        self.reader = reader
        self.out_q = out_q
        self.shared_key = shared_key
        self.worker_id = worker_id
        self.batch: list[Reading] = []
        self.received = 0
        self.bad = 0
        self.queued = 0
        self.dropped = 0

    def on_readable(self) -> None:
        """Called by the asyncio loop when the socket has data."""
        for _ in range(DRAIN_ROUNDS):
            datagrams = self.reader.read()
            if not datagrams:
                return
            self.received += len(datagrams)
            for data in datagrams:
                try:
                    self.batch.append(parse_packet(data, self.shared_key))
                except PacketError:
                    self.bad += 1
            if len(self.batch) >= BATCH_SIZE:
                self.flush()

    def flush(self) -> None:
        if not self.batch:
            return
        try:
            self.out_q.put_nowait(self.batch)
            self.queued += len(self.batch)
        except queue.Full:
            # Writer is behind. Drop instead of blocking the event loop.
            self.dropped += len(self.batch)
            log.warning("worker %d: writer behind, dropped %d readings",
                        self.worker_id, len(self.batch))
        self.batch = []


def make_socket(host: str, port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):  # not available on Windows
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    # Larger receive buffer absorbs bursts. Linux caps this at net.core.rmem_max.
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RCVBUF_BYTES)
    sock.bind((host, port))
    sock.setblocking(False)
    return sock


async def _run_receiver(worker_id: int, host: str, port: int, shared_key: str,
                        out_q, stop_event, recv_kind: str) -> None:
    loop = asyncio.get_running_loop()
    sock = make_socket(host, port)
    # bufsize is one byte more than the limit, so an oversized packet is detected
    reader = make_reader(sock, recv_kind, RECV_VLEN, MAX_PACKET_BYTES + 1)
    rx = Receiver(reader, out_q, shared_key, worker_id)
    loop.add_reader(sock.fileno(), rx.on_readable)
    log.info("receiver %d listening on %s:%d (reader: %s)",
             worker_id, host, port, reader.kind)

    last_stats = time.monotonic()
    try:
        while not stop_event.is_set():
            await asyncio.sleep(FLUSH_INTERVAL)
            rx.flush()
            if time.monotonic() - last_stats >= STATS_INTERVAL:
                log.info("receiver %d: received=%d queued=%d bad=%d dropped=%d",
                         worker_id, rx.received, rx.queued, rx.bad, rx.dropped)
                last_stats = time.monotonic()
    finally:
        loop.remove_reader(sock.fileno())
        rx.flush()
        sock.close()
        log.info("receiver %d stopped", worker_id)


def receiver_process(worker_id: int, host: str, port: int, shared_key: str,
                     out_q, stop_event, recv_kind: str) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # The main process decides when to stop us, through stop_event.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    asyncio.run(_run_receiver(worker_id, host, port, shared_key, out_q, stop_event, recv_kind))


class WriterThread(threading.Thread):
    """Takes batches from the queue and writes them to SQLite.

    A SQLite connection can only be used by the thread that created it, so the
    connection is opened inside run(). A None item tells the thread to stop.
    """

    def __init__(self, in_q, db_path: str):
        super().__init__(name="db-writer", daemon=True)
        self.in_q = in_q
        self.db_path = db_path
        self.written = 0
        self.failed = 0

    def run(self) -> None:
        conn = storage.connect(self.db_path)
        try:
            while True:
                batch = self.in_q.get()
                if batch is None:
                    break
                try:
                    self.written += storage.insert_many(conn, batch)
                except sqlite3.Error as exc:
                    self.failed += len(batch)
                    log.error("database write failed, dropping %d readings: %s", len(batch), exc)
        finally:
            conn.close()
            log.info("writer thread stopped: %d written, %d failed", self.written, self.failed)


def _on_sigterm(signum, frame) -> None:
    # Turn SIGTERM (sent by systemd/Docker) into the same clean path as Ctrl+C.
    raise KeyboardInterrupt


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="UDP sensor ingest server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) // 2),
                        help="number of receiver processes")
    parser.add_argument("--recv", choices=["recvmmsg", "simple"], default="recvmmsg",
                        help="recvmmsg (Linux, batched) or simple (one recv per packet)")
    parser.add_argument("--db", default=os.environ.get("SENSOR_DB", "data/sensors.db"))
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, _on_sigterm)

    shared_key = os.environ.get("INGEST_KEY", "")
    if not shared_key:
        log.warning("INGEST_KEY is not set: any sender can publish data")

    workers = args.workers
    if workers > 1 and not hasattr(socket, "SO_REUSEPORT"):
        log.warning("SO_REUSEPORT is not available on this OS, using 1 receiver")
        workers = 1

    os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)
    storage.connect(args.db).close()  # create the schema once, before forking

    out_q: mp.Queue = mp.Queue(maxsize=QUEUE_MAXSIZE)
    stop_event = mp.Event()

    writer = WriterThread(out_q, args.db)
    writer.start()

    receivers = [
        mp.Process(target=receiver_process,
                   args=(i, args.host, args.port, shared_key, out_q, stop_event, args.recv),
                   name=f"receiver-{i}")
        for i in range(workers)
    ]
    for r in receivers:
        r.start()
    log.info("ingest running: %d receiver process(es) on UDP %s:%d, db=%s",
             workers, args.host, args.port, args.db)

    try:
        while not stop_event.is_set() and any(r.is_alive() for r in receivers):
            time.sleep(0.5)
        if not stop_event.is_set():
            log.error("all receiver processes have exited")
    except KeyboardInterrupt:
        log.info("stopping...")
    finally:
        stop_event.set()                     # receivers flush and exit on their own
        for r in receivers:
            r.join(timeout=5)
            if r.is_alive():
                r.terminate()
        try:
            out_q.put(None, timeout=5)       # the writer drains the queue, then stops
        except queue.Full:
            pass
        writer.join(timeout=10)


if __name__ == "__main__":
    main()
