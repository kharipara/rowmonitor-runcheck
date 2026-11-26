"""Optional local file so an unchanged problem set is not posted again.

The file stores fingerprints of the last alert. It is not a run history.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from rowmonitor_runcheck.errors import ConfigError

_STATE_VERSION = 1


def load_fingerprints(path: Path) -> set[str]:
    if path.exists() and path.is_dir():
        raise ConfigError(f"state file path is a directory: {path}")
    if not path.exists():
        return set()
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        print(f"warning: ignoring unreadable state file {path}", file=sys.stderr)
        return set()
    fingerprints = data.get("fingerprints") if isinstance(data, dict) else None
    if not isinstance(fingerprints, list):
        print(f"warning: ignoring invalid state file {path}", file=sys.stderr)
        return set()
    return {str(item) for item in fingerprints}


def save_fingerprints(path: Path, fingerprints: set[str]) -> None:
    if path.exists() and path.is_dir():
        raise ConfigError(f"state file path is a directory: {path}")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ConfigError(f"cannot create state file directory: {exc.strerror or exc}") from exc
    payload = {"version": _STATE_VERSION, "fingerprints": sorted(fingerprints)}
    temporary = path.with_name(path.name + ".tmp")
    try:
        temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        _restrict(temporary)
        os.replace(temporary, path)
        _restrict(path)
    except OSError as exc:
        raise ConfigError(f"cannot write state file: {exc.strerror or exc}") from exc


def _restrict(path: Path) -> None:
    try:
        os.chmod(path, 0o600)
    except OSError:
        return
