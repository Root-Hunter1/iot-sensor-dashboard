import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from app import storage
from app.protocol import Reading
from app.web import make_server


class WebTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db = os.path.join(cls.tmp.name, "web.db")
        conn = storage.connect(cls.db)
        now = int(time.time() * 1000)
        storage.insert_many(conn, [
            Reading("sensor-01", 24.0, 55.0, now - 5000),
            Reading("sensor-01", 24.5, 56.0, now),
        ])
        conn.close()
        cls.server = make_server("127.0.0.1", 0, cls.db)
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}"
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.tmp.cleanup()

    def get(self, path):
        try:
            with urllib.request.urlopen(self.base + path) as res:
                return res.status, res.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()

    def test_index_page(self):
        status, body = self.get("/")
        self.assertEqual(status, 200)
        self.assertIn(b"IoT Sensor Dashboard", body)

    def test_latest(self):
        status, body = self.get("/api/latest")
        self.assertEqual(status, 200)
        data = json.loads(body)
        self.assertEqual(data[0]["device_id"], "sensor-01")
        self.assertEqual(data[0]["temp_c"], 24.5)

    def test_devices(self):
        _, body = self.get("/api/devices")
        self.assertEqual(json.loads(body), ["sensor-01"])

    def test_history(self):
        status, body = self.get("/api/history?device_id=sensor-01&minutes=60")
        self.assertEqual(status, 200)
        self.assertEqual(len(json.loads(body)), 2)

    def test_history_validation(self):
        self.assertEqual(self.get("/api/history")[0], 400)
        self.assertEqual(self.get("/api/history?device_id=x&minutes=abc")[0], 400)

    def test_unknown_path(self):
        self.assertEqual(self.get("/nope")[0], 404)


if __name__ == "__main__":
    unittest.main()
