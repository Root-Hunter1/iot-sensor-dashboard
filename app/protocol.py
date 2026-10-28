"""Parse and validate sensor packets.

A packet is one UDP datagram containing a JSON object:

    {"device_id": "sensor-01", "temp_c": 27.5, "humidity": 61.2,
     "ts_ms": 1730000000000, "key": "shared-secret"}

`ts_ms` and `key` are optional. Everything in this module is pure
(no sockets, no database), so it is easy to test.
"""
from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass

MAX_PACKET_BYTES = 512
MAX_DEVICE_ID_LEN = 64
TEMP_RANGE = (-60.0, 100.0)
HUMIDITY_RANGE = (0.0, 100.0)


class PacketError(ValueError):
    """Raised when a datagram is not a valid sensor reading."""


@dataclass(frozen=True, slots=True)
class Reading:
    device_id: str
    temp_c: float
    humidity: float
    ts_ms: int


def _number(obj: dict, field: str, lo: float, hi: float) -> float:
    value = obj.get(field)
    # bool is a subclass of int in Python, so reject it explicitly
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PacketError(f"'{field}' must be a number")
    if not math.isfinite(value) or not (lo <= value <= hi):
        raise PacketError(f"'{field}' out of range")
    return float(value)


def parse_packet(data: bytes, shared_key: str = "", now_ms: int | None = None) -> Reading:
    if len(data) > MAX_PACKET_BYTES:
        raise PacketError("packet too large")

    try:
        obj = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PacketError("not valid UTF-8 JSON") from exc

    if not isinstance(obj, dict):
        raise PacketError("payload must be a JSON object")

    if shared_key and obj.get("key") != shared_key:
        raise PacketError("bad or missing key")

    device_id = obj.get("device_id")
    if not isinstance(device_id, str) or not (0 < len(device_id) <= MAX_DEVICE_ID_LEN):
        raise PacketError("invalid device_id")

    ts_ms = obj.get("ts_ms")
    if ts_ms is None:
        ts_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    elif isinstance(ts_ms, bool) or not isinstance(ts_ms, int) or ts_ms <= 0:
        raise PacketError("invalid ts_ms")

    return Reading(
        device_id=device_id,
        temp_c=_number(obj, "temp_c", *TEMP_RANGE),
        humidity=_number(obj, "humidity", *HUMIDITY_RANGE),
        ts_ms=ts_ms,
    )
