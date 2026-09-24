"""Release note checks for tagged publishing."""

import subprocess
import sys
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_release_notes.py"
CHANGELOG = """# Changelog

## [Unreleased](https://github.com/openghg/openghg/compare/0.19.0...HEAD)

<!-- towncrier release notes start -->

## [0.20.0] - 2026-09-24

### Added

- Towncrier setup.
"""


@pytest.mark.parametrize(
    ("changelog", "fragment", "expected", "message"),
    [
        (CHANGELOG, None, 0, ""),
        (CHANGELOG, "1302.feature", 1, "Unassembled Towncrier fragments"),
        (CHANGELOG.replace("## [0.20.0]", "## [0.19.0]"), None, 1, "no Towncrier entry"),
        (
            CHANGELOG.replace("<!-- towncrier", "### Added\n\n- Old note.\n\n<!-- towncrier"),
            None,
            1,
            "pre-Towncrier Unreleased",
        ),
    ],
    ids=["ready", "pending-fragment", "missing-version", "legacy-unreleased"],
)
def test_release_notes_gate(
    tmp_path: Path, changelog: str, fragment: str | None, expected: int, message: str
) -> None:
    (tmp_path / "CHANGELOG.md").write_text(changelog, encoding="utf-8")
    fragments = tmp_path / "newsfragments"
    fragments.mkdir()
    (fragments / "README.md").write_text("Guide\n", encoding="utf-8")
    if fragment:
        (fragments / fragment).write_text("News\n", encoding="utf-8")

    result = subprocess.run([sys.executable, str(SCRIPT), "0.20.0"], cwd=tmp_path, capture_output=True)
    assert result.returncode == expected
    assert message.encode() in result.stderr
