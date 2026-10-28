"""Dashboard server: a small JSON API and the single-page UI.

Uses only the standard library so there is nothing extra to install.
For internet-facing deployment, put it behind a reverse proxy (nginx, Caddy).

Run:
    python -m app.web --port 8000 --db data/sensors.db
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from app import storage

INDEX_HTML = Path(__file__).parent / "static" / "index.html"
MAX_HISTORY_MINUTES = 1440
MAX_HISTORY_POINTS = 2000

log = logging.getLogger("web")


class DashboardHandler(BaseHTTPRequestHandler):
    db_path: str = ""  # set by make_server()

    def do_GET(self) -> None:
        url = urlparse(self.path)
        try:
            if url.path == "/":
                self._send(200, INDEX_HTML.read_bytes(), "text/html; charset=utf-8")
            elif url.path == "/api/devices":
                self._json(200, self._query(storage.devices))
            elif url.path == "/api/latest":
                self._json(200, self._query(storage.latest_per_device))
            elif url.path == "/api/history":
                self._history(parse_qs(url.query))
            else:
                self._json(404, {"error": "not found"})
        except ValueError as exc:
            self._json(400, {"error": str(exc)})
        except sqlite3.Error:
            log.exception("database error")
            self._json(500, {"error": "database error"})

    def _history(self, params: dict) -> None:
        device_id = params.get("device_id", [""])[0]
        if not device_id:
            raise ValueError("device_id is required")
        try:
            minutes = int(params.get("minutes", ["60"])[0])
        except ValueError:
            raise ValueError("minutes must be an integer") from None
        minutes = min(max(minutes, 1), MAX_HISTORY_MINUTES)
        since_ms = int(time.time() * 1000) - minutes * 60_000
        rows = self._query(lambda conn: storage.history(conn, device_id, since_ms, MAX_HISTORY_POINTS))
        self._json(200, rows)

    def _query(self, fn):
        conn = storage.connect(self.db_path)
        try:
            return fn(conn)
        finally:
            conn.close()

    def _json(self, status: int, payload) -> None:
        self._send(status, json.dumps(payload).encode("utf-8"), "application/json")

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args) -> None:  # keep the console quiet
        pass


def make_server(host: str, port: int, db_path: str) -> ThreadingHTTPServer:
    handler = type("Handler", (DashboardHandler,), {"db_path": db_path})
    return ThreadingHTTPServer((host, port), handler)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="IoT sensor dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--db", default=os.environ.get("SENSOR_DB", "data/sensors.db"))
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    storage.connect(args.db).close()  # make sure the schema exists

    server = make_server(args.host, args.port, args.db)
    log.info("dashboard at http://%s:%d", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
