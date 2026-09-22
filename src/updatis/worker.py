from __future__ import annotations

import json
import os
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from updatis.health import check_metadata


class HealthHandler(BaseHTTPRequestHandler):
    ready = False

    def do_GET(self) -> None:  # noqa: N802
        healthy = self.path == "/health/live" or (self.path == "/health/ready" and self.ready)
        self.send_response(200 if healthy else 503)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ready" if healthy else "not_ready"}).encode())

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    config_path = os.getenv("UPDATIS_RUNTIME_CONFIG", "/etc/updatis/runtime.json")
    check_metadata(config_path)
    HealthHandler.ready = True
    server = ThreadingHTTPServer(("127.0.0.1", 8081), HealthHandler)
    stop = threading.Event()

    def shutdown(*_: object) -> None:
        HealthHandler.ready = False
        stop.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    server.serve_forever(poll_interval=0.5)
    server.server_close()


if __name__ == "__main__":
    main()
