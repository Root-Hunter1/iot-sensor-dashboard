"""Fake sensors for testing. Sends random-walk temperature/humidity over UDP.

Run:
    python simulator.py --port 9999 --devices 3
Only send data to your own ingest server.
"""
import argparse
import json
import os
import random
import socket
import time


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def main() -> None:
    parser = argparse.ArgumentParser(description="Fake IoT sensor sender")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--devices", type=int, default=3)
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between rounds")
    args = parser.parse_args()

    key = os.environ.get("INGEST_KEY", "")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    state = {
        f"sensor-{i:02d}": [random.uniform(22, 30), random.uniform(40, 70)]
        for i in range(1, args.devices + 1)
    }
    print(f"sending {len(state)} fake sensors to {args.host}:{args.port} (Ctrl+C to stop)")

    try:
        while True:
            for device_id, values in state.items():
                values[0] = clamp(values[0] + random.uniform(-0.3, 0.3), -10, 50)
                values[1] = clamp(values[1] + random.uniform(-1.0, 1.0), 0, 100)
                payload = {
                    "device_id": device_id,
                    "temp_c": round(values[0], 2),
                    "humidity": round(values[1], 2),
                    "ts_ms": int(time.time() * 1000),
                }
                if key:
                    payload["key"] = key
                sock.sendto(json.dumps(payload).encode("utf-8"), (args.host, args.port))
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("stopped")


if __name__ == "__main__":
    main()
