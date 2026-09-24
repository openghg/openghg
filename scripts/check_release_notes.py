"""Check that a tagged OpenGHG release has assembled Towncrier notes."""

import re
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2 or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", sys.argv[1]):
        raise SystemExit("usage: check_release_notes.py VERSION (for example, 0.20.0)")

    version = sys.argv[1]
    changelog = Path("CHANGELOG.md").read_text(encoding="utf-8")
    heading = rf"(?m)^## \[{re.escape(version)}\] - \d{{4}}-\d{{2}}-\d{{2}}$"
    if not re.search(heading, changelog):
        raise SystemExit(f"CHANGELOG.md has no Towncrier entry for {version}")

    pending = sorted(
        path.name
        for path in Path("newsfragments").iterdir()
        if path.is_file() and path.name not in {"README.md", ".gitkeep", "template.md"}
    )
    if pending:
        raise SystemExit("Unassembled Towncrier fragments: " + ", ".join(pending))

    unreleased = re.search(
        r"(?ms)^## \[Unreleased\][^\n]*\n(.*?)^<!-- towncrier release notes start -->",
        changelog,
    )
    if unreleased is None or unreleased.group(1).strip():
        raise SystemExit("Review and clear the pre-Towncrier Unreleased notes before tagging")


if __name__ == "__main__":
    main()
