"""One factual reason to alert. The fingerprint is local dedupe state, not history."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Problem:
    code: str
    summary: str
    check: str
    fingerprint: str
    details: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        body: dict[str, object] = {
            "code": self.code,
            "summary": self.summary,
            "check": self.check,
        }
        body.update(self.details)
        return body
