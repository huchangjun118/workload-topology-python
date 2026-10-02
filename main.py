"""Entry point: run the workload topology browsing service."""

from __future__ import annotations

import uvicorn

from topology.api import create_app

app = create_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
