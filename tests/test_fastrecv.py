import select
import socket
import sys
import time
import unittest

from app.fastrecv import RecvMMsgReader, make_reader


def udp_pair():
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx.bind(("127.0.0.1", 0))
    rx.setblocking(False)
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return rx, tx, rx.getsockname()[1]


def collect(reader, sock, expected, timeout=3.0):
    got = []
    deadline = time.time() + timeout
    while len(got) < expected and time.time() < deadline:
        select.select([sock], [], [], 0.2)
        got.extend(reader.read())
    return got


class ReaderTests(unittest.TestCase):
    def _roundtrip(self, kind):
        rx, tx, port = udp_pair()
        try:
            messages = [f"msg-{i}".encode() for i in range(10)]
            for m in messages:
                tx.sendto(m, ("127.0.0.1", port))
            # vlen=4 forces several recvmmsg calls for 10 messages
            reader = make_reader(rx, kind, vlen=4, bufsize=64)
            got = collect(reader, rx, len(messages))
            self.assertEqual(sorted(got), sorted(messages))
            return reader
        finally:
            rx.close()
            tx.close()

    def test_simple_reader(self):
        self.assertEqual(self._roundtrip("simple").kind, "simple")

    @unittest.skipUnless(sys.platform.startswith("linux"), "recvmmsg is Linux-only")
    def test_recvmmsg_reader(self):
        reader = self._roundtrip("recvmmsg")
        self.assertIsInstance(reader, RecvMMsgReader)

    def test_empty_socket_returns_empty_list(self):
        rx, tx, _ = udp_pair()
        try:
            self.assertEqual(make_reader(rx, "simple").read(), [])
            self.assertEqual(make_reader(rx, "recvmmsg").read(), [])
        finally:
            rx.close()
            tx.close()

    def test_oversized_datagram_is_truncated(self):
        rx, tx, port = udp_pair()
        try:
            tx.sendto(b"x" * 100, ("127.0.0.1", port))
            reader = make_reader(rx, "recvmmsg", vlen=4, bufsize=64)
            got = collect(reader, rx, 1)
            self.assertEqual(len(got[0]), 64)
        finally:
            rx.close()
            tx.close()


if __name__ == "__main__":
    unittest.main()
