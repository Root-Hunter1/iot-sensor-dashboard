import json
import os
import queue
import socket
import tempfile
import time
import select
import unittest

from app import storage
from app.fastrecv import make_reader
from app.ingest import Receiver, WriterThread, make_socket
from app.protocol import Reading


def pkt(**overrides) -> bytes:
    base = {"device_id": "sensor-01", "temp_c": 25.0, "humidity": 50.0,
            "ts_ms": 1700000000000, "key": "k"}
    base.update(overrides)
    return json.dumps(base).encode("utf-8")


class ReceiverTests(unittest.TestCase):
    def test_valid_packets_are_batched_and_bad_ones_counted(self):
        sock = make_socket("127.0.0.1", 0)
        port = sock.getsockname()[1]
        out = queue.Queue()
        rx = Receiver(make_reader(sock, "recvmmsg", 64, 513), out, "k", worker_id=1)

        tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        packets = [
            pkt(),
            pkt(device_id="b"),
            pkt(key="WRONG"),          # bad key
            b"garbage",                # not JSON
            pkt(device_id="c"),
        ]
        for p in packets:
            tx.sendto(p, ("127.0.0.1", port))

        deadline = time.time() + 3
        while rx.received < len(packets) and time.time() < deadline:
            select.select([sock], [], [], 0.2)
            rx.on_readable()

        self.assertEqual(rx.received, 5)
        self.assertEqual(rx.bad, 2)
        rx.flush()
        batch = out.get_nowait()
        self.assertEqual([r.device_id for r in batch], ["sensor-01", "b", "c"])

        tx.close()
        sock.close()


class WriterThreadTests(unittest.TestCase):
    def test_writes_batches_and_stops_on_sentinel(self):
        tmp = tempfile.TemporaryDirectory()
        db = os.path.join(tmp.name, "writer.db")
        q = queue.Queue()
        q.put([Reading("a", 20.0, 40.0, 1000), Reading("a", 21.0, 41.0, 2000)])
        q.put([Reading("b", 30.0, 60.0, 1500)])
        q.put(None)

        writer = WriterThread(q, db)
        writer.start()
        writer.join(5)
        self.assertFalse(writer.is_alive())
        self.assertEqual(writer.written, 3)

        conn = storage.connect(db)
        self.assertEqual(len(storage.history(conn, "a", since_ms=0)), 2)
        self.assertEqual(len(storage.history(conn, "b", since_ms=0)), 1)
        conn.close()
        tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
