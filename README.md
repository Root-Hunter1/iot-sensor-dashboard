<p align="center">
  <img src="images (1).jpeg" width="100%" alt="Root-Hunter1 banner">
</p>

# IoT Sensor Dashboard

Sensors send small JSON packets over UDP. An ingest server validates them and
stores them in SQLite. A web dashboard shows the latest value of each device and
a live history chart.

```
sensor --UDP--> receiver processes --mp.Queue--> writer thread --> SQLite --> web server --> browser
                (asyncio + recvmmsg)             (threading)                 (threads)
```

## Where each feature is used

| Feature | Where | What it does |
|---|---|---|
| **asyncio** | `app/ingest.py` `_run_receiver()` | Each receiver runs an event loop. The UDP socket is registered with `loop.add_reader()` and the loop wakes when data arrives. |
| **ctypes** | `app/fastrecv.py` | Loads libc's `recvmmsg()` and defines the C structs `iovec`, `msghdr`, `mmsghdr`. |
| **recvmmsg** | `app/fastrecv.py` `RecvMMsgReader` | Reads up to 64 datagrams in one system call. Falls back to plain `recv` on non-Linux systems. |
| **multiprocessing** | `app/ingest.py` `main()` | Starts N receiver processes. They share one UDP port through `SO_REUSEPORT`, so the kernel spreads packets across CPU cores. |
| **threading** | `WriterThread` in `app/ingest.py` | A thread in the main process takes batches from the queue and writes them to SQLite. The web server also uses threads (`ThreadingHTTPServer`). |
| **queue** | `multiprocessing.Queue` | Carries batches from receiver processes to the writer thread. |

## Requirements

- Python 3.10+. Standard library only, so there is nothing to `pip install`.
- Linux is recommended. `recvmmsg` and multiple receivers use Linux/BSD features.
  On Windows, the code runs with one receiver and plain `recv`.

## Quick start

Open three terminals in the project folder.

```bash
# 1. Ingest server (UDP port 9999)
export INGEST_KEY=change-me
python -m app.ingest --port 9999 --workers 2 --recv recvmmsg

# 2. Dashboard (http://127.0.0.1:8000)
python -m app.web --port 8000

# 3. Fake sensors, so you can see data without hardware
export INGEST_KEY=change-me
python simulator.py --port 9999 --devices 3
```

Open http://127.0.0.1:8000 in a browser.

`--recv simple` uses one `recv()` call per packet. It is useful for comparing
with `recvmmsg`.

## Sending data from a real device

Send one UDP datagram per reading, containing a JSON object:

```json
{"device_id": "sensor-01", "temp_c": 27.5, "humidity": 61.2, "key": "change-me"}
```

- `device_id`: text, up to 64 characters. Required.
- `temp_c`: number from -60 to 100. Required.
- `humidity`: number from 0 to 100. Required.
- `ts_ms`: Unix time in milliseconds. Optional. If missing, the server uses its own clock.
- `key`: must equal the `INGEST_KEY` environment variable on the server.

Invalid packets (bad JSON, wrong key, out-of-range values, more than 512 bytes)
are counted as `bad` and never stored.

Python example for a device:

```python
import json, socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
msg = {"device_id": "sensor-01", "temp_c": 27.5, "humidity": 61.2, "key": "change-me"}
sock.sendto(json.dumps(msg).encode(), ("SERVER_IP", 9999))
```

## Dashboard API

| Endpoint | Description |
|---|---|
| `GET /api/devices` | list of device IDs |
| `GET /api/latest` | latest reading of each device |
| `GET /api/history?device_id=X&minutes=60` | readings from the last N minutes (max 1440), up to 2000 points |

A device is **online** if its last reading is less than 60 seconds old.

## Configuration

| Setting | How to set | Default |
|---|---|---|
| Shared key | env `INGEST_KEY` | empty (no check, warning printed) |
| Database file | `--db` or env `SENSOR_DB` | `data/sensors.db` |
| Receiver processes | `--workers` | half the CPU cores |
| Receive method | `--recv recvmmsg` or `simple` | `recvmmsg` |

## Tests

```bash
python -m unittest discover -s tests -t . -v
```

18 tests cover packet validation, the database, the HTTP API, the ctypes reader
(against both readers), the receiver pipeline, and the writer thread.

## Measured results

These were measured on a Linux sandbox with **one CPU core**. Sender, receivers,
writer and kernel all share that core, so they do not show what a multi-core
machine would do.

**Reader cost, per packet** (time spent inside the read call only):

| Reader | µs per packet |
|---|---|
| `simple` (one recv per packet) | about 1.0 |
| `recvmmsg` via ctypes, arena buffer | about 0.8 |

The first ctypes version gave each message its own buffer and was slower than
`simple` (about 1.5 µs). Copying from a single shared buffer fixed that. Python
overhead per packet is roughly the same size as the syscall savings, so the
gain here is modest. It is larger in C code, such as `recvmmsg ctypes/udp_pg.py`
in the original repo.

**Sustained load:** 5,000 packets/s for 4 seconds: 20,000 of 20,000 stored, no loss.

**Burst load:** 200,000 packets sent as fast as Python can, on the single core:
about 40–50% of packets were dropped by the kernel (`RcvbufErrors`) before the
receivers read them. On this one-core machine the sender competes with the
receivers, so this number mostly reflects CPU sharing.

To check loss on your own machine:

```bash
grep Udp: /proc/net/snmp   # watch RcvbufErrors before and after a test
```

To allow larger kernel buffers on Linux:

```bash
sudo sysctl -w net.core.rmem_max=8388608
```

## Security notes

- The shared key is sent in plain text inside each UDP packet. Anyone who can see
  the traffic can read it and send fake data. Use it only on a trusted network, or
  add per-device HMAC signatures before exposing the server to the internet.
- UDP has no delivery guarantee. Lost packets are not retried.
- The dashboard has no login. Keep it on `127.0.0.1` or behind an authenticated proxy.

## Project layout

```
app/
  protocol.py   packet parsing and validation (pure functions)
  fastrecv.py   ctypes recvmmsg reader and plain recv fallback
  storage.py    SQLite schema and queries
  ingest.py     asyncio receivers, multiprocessing, writer thread
  web.py        dashboard server (JSON API, threaded)
  static/       dashboard page
simulator.py    fake sensors for testing
tests/          unit tests
```

## Next steps

- Docker and docker-compose, with the key in a `.env` file
- Per-device HMAC signing instead of one shared key
- PostgreSQL, using the approach in `udp_pg.py` from the original repo
- Alerts when a temperature crosses a limit
