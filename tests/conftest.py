"""Shared pytest fixtures."""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest

from tests.server import AppServer, Dispatch

_SECRET_ENV = (
    "AIRFLOW_USERNAME",
    "AIRFLOW_PASSWORD",
    "AIRFLOW_TOKEN",
    "SLACK_WEBHOOK_URL",
    "RUNCHECK_WEBHOOK_URL",
    "RUNCHECK_STATE_FILE",
)


@pytest.fixture(autouse=True)
def _clear_runcheck_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _SECRET_ENV:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def serve() -> Iterator[Callable[[Dispatch], tuple[str, AppServer]]]:
    servers: list[AppServer] = []

    def start(dispatch: Dispatch) -> tuple[str, AppServer]:
        server = AppServer(dispatch)
        servers.append(server)
        return server.start(), server

    yield start
    for server in servers:
        server.stop()
