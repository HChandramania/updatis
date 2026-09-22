from __future__ import annotations

import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Response, status

from updatis.health import check_metadata


def create_app(runtime_path: str | None = None) -> FastAPI:
    config_path = runtime_path or os.getenv("UPDATIS_RUNTIME_CONFIG", "/etc/updatis/runtime.json")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        check_metadata(config_path)
        app.state.ready = True
        yield

    app = FastAPI(title="Updatis", version="0.1.0-a", lifespan=lifespan)

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    def ready(response: Response) -> dict[str, str]:
        try:
            check_metadata(config_path)
        except Exception:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {"status": "not_ready"}
        return {"status": "ready"}

    return app


app = create_app()


def main() -> None:
    uvicorn.run("updatis.api:app", host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
