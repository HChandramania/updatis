from __future__ import annotations

import json
import os
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from updatis.health import check_metadata
from updatis.config.loader import load_pipeline_config, load_runtime_config
from updatis.config.secrets import SecretResolver
from updatis.db.connection import create_metadata_engine
from updatis.intake.repository import IntakeRepository
from updatis.kafka.consumer import run_consumer


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
    server = ThreadingHTTPServer(("127.0.0.1", 8081), HealthHandler)
    stop = threading.Event()

    def shutdown(*_: object) -> None:
        HealthHandler.ready = False
        stop.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    server_thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.5}, daemon=True)
    server_thread.start()
    capture_enabled = os.getenv("UPDATIS_CAPTURE_ENABLED", "false").lower() == "true"
    pipeline_path = os.getenv("UPDATIS_PIPELINE_CONFIG")
    if not capture_enabled:
        HealthHandler.ready = True
        stop.wait()
    else:
        if not pipeline_path:
            raise RuntimeError("UPDATIS_PIPELINE_CONFIG is required when capture is enabled")
        runtime = load_runtime_config(config_path)
        pipeline = load_pipeline_config(pipeline_path)
        resolver = SecretResolver(runtime.secrets_directory)
        engine = create_metadata_engine(runtime.metadata, resolver)
        run_consumer(pipeline, IntakeRepository(engine),
                     os.getenv("UPDATIS_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"), stop,
                     lambda value: setattr(HealthHandler, "ready", value))
    server.shutdown()
    server.server_close()


if __name__ == "__main__":
    main()
