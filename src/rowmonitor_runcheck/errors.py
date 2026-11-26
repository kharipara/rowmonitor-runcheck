"""Exit-2 errors. Check failures are alert problems, not these exceptions."""

from __future__ import annotations


class ConfigError(Exception):
    """Local usage or configuration is wrong."""


class WebhookError(Exception):
    """A configured webhook could not be delivered."""


class AirflowAPIError(Exception):
    """The Airflow API could not answer a check. Becomes an alert."""

    def __init__(self, code: str, summary: str, dag_id: str | None = None) -> None:
        super().__init__(summary)
        self.code = code
        self.summary = summary
        self.dag_id = dag_id
