import json
import unittest

from app.protocol import PacketError, parse_packet


def packet(**overrides) -> bytes:
    base = {"device_id": "sensor-01", "temp_c": 25.5, "humidity": 50, "ts_ms": 1700000000000}
    base.update(overrides)
    return json.dumps(base).encode("utf-8")


class ParsePacketTests(unittest.TestCase):
    def test_valid_packet(self):
        r = parse_packet(packet())
        self.assertEqual(r.device_id, "sensor-01")
        self.assertEqual(r.temp_c, 25.5)
        self.assertEqual(r.humidity, 50.0)
        self.assertEqual(r.ts_ms, 1700000000000)

    def test_missing_timestamp_uses_server_time(self):
        data = json.loads(packet())
        del data["ts_ms"]
        r = parse_packet(json.dumps(data).encode(), now_ms=42)
        self.assertEqual(r.ts_ms, 42)

    def test_invalid_packets_are_rejected(self):
        cases = {
            "not json": b"hello",
            "json but not object": b"[1, 2]",
            "temp too high": packet(temp_c=999),
            "temp is a string": packet(temp_c="25"),
            "bool is not a number": packet(temp_c=True),
            "nan": packet(humidity=float("nan")),
            "infinity": packet(humidity=float("inf")),
            "humidity below zero": packet(humidity=-1),
            "empty device id": packet(device_id=""),
            "device id is number": packet(device_id=5),
            "device id too long": packet(device_id="x" * 65),
            "negative timestamp": packet(ts_ms=-1),
            "oversized packet": b"x" * 513,
        }
        for name, data in cases.items():
            with self.subTest(name):
                with self.assertRaises(PacketError):
                    parse_packet(data)

    def test_shared_key(self):
        self.assertIsNotNone(parse_packet(packet(key="s3cret"), shared_key="s3cret"))
        with self.assertRaises(PacketError):
            parse_packet(packet(key="wrong"), shared_key="s3cret")
        with self.assertRaises(PacketError):
            parse_packet(packet(), shared_key="s3cret")


if __name__ == "__main__":
    unittest.main()
