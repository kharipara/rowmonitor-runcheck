"""README and source stay inside the free-tool limits."""

from __future__ import annotations

import re
from pathlib import Path

from rowmonitor_runcheck import __version__
from rowmonitor_runcheck.cli import main

ROOT = Path(__file__).resolve().parents[1]


def test_version_matches_pyproject() -> None:
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert f'version = "{__version__}"' in text


def test_readme_upgrade_links_use_readme_medium() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "https://rowmonitor.com/airflow?utm_source=runcheck&utm_medium=readme" in readme
    assert "https://rowmonitor.com/signup?utm_source=runcheck&utm_medium=readme" in readme
    assert "MIT" in readme
    assert "MWAA" in readme
    assert "Cloud Composer" in readme
    assert "Astronomer" in readme


def test_package_source_only_links_to_the_cli_upgrade_urls() -> None:
    urls: set[str] = set()
    for path in (ROOT / "src").rglob("*.py"):
        urls.update(re.findall(r"https://[^\s\"']+", path.read_text(encoding="utf-8")))
    assert urls == {
        "https://rowmonitor.com/airflow?utm_source=runcheck&utm_medium=cli",
        "https://rowmonitor.com/signup?utm_source=runcheck&utm_medium=cli",
    }


def test_help_and_version_exit_0(capsys) -> None:
    assert main(["--help"]) == 0
    assert main(["--version"]) == 0
    output, _errors = capsys.readouterr()
    assert "rowmonitor-runcheck" in output
    assert __version__ in output
    assert main(["nope"]) == 2
