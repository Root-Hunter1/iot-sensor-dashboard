import os
import tempfile
import unittest

from app import storage
from app.protocol import Reading


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.conn = storage.connect(os.path.join(self.tmp.name, "test.db"))

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def test_insert_latest_history_devices(self):
        storage.insert_many(self.conn, [
            Reading("a", 20.0, 40.0, 1000),
            Reading("a", 21.0, 41.0, 2000),
            Reading("b", 30.0, 60.0, 1500),
        ])

        latest = {r["device_id"]: r for r in storage.latest_per_device(self.conn)}
        self.assertEqual(latest["a"]["temp_c"], 21.0)
        self.assertEqual(latest["a"]["ts_ms"], 2000)
        self.assertEqual(latest["b"]["temp_c"], 30.0)

        hist = storage.history(self.conn, "a", since_ms=0)
        self.assertEqual([r["ts_ms"] for r in hist], [1000, 2000])  # oldest first

        self.assertEqual(storage.history(self.conn, "a", since_ms=1500)[0]["ts_ms"], 2000)
        self.assertEqual(storage.devices(self.conn), ["a", "b"])

    def test_empty_batch_is_noop(self):
        self.assertEqual(storage.insert_many(self.conn, []), 0)
        self.assertEqual(storage.latest_per_device(self.conn), [])


if __name__ == "__main__":
    unittest.main()
