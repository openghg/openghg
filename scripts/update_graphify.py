#!/usr/bin/env python3
"""Regenerate or verify the committed Graphify code graph."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


GRAPHIFY_VERSION = "0.9.55"
GRAPHIFY_PYTHON_VERSION = "3.12"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
GRAPH_PATH = REPOSITORY_ROOT / "graphify-out" / "graph.json"


def canonical_graph(path: Path) -> bytes:
    """Return stable JSON bytes for a generated Graphify graph."""
    graph = json.loads(path.read_text(encoding="utf-8"))
    graph.pop("built_at_commit", None)

    def sort_key(item: object) -> str:
        """Produce a deterministic key for one graph item."""
        return json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    for key in ("nodes", "edges", "links", "hyperedges"):
        if key in graph:
            graph[key].sort(key=sort_key)
    return (json.dumps(graph, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode()


def build_graph() -> bytes:
    """Build a fresh code graph using the pinned Graphify version."""
    if shutil.which("uvx") is None:
        raise SystemExit("uvx is required; install uv before updating the Graphify graph.")

    with tempfile.TemporaryDirectory(prefix="openghg-graphify-") as temp_dir:
        output_root = Path(temp_dir)
        subprocess.run(
            [
                "uvx",
                "--python",
                GRAPHIFY_PYTHON_VERSION,
                "--from",
                f"graphifyy=={GRAPHIFY_VERSION}",
                "graphify",
                "extract",
                str(REPOSITORY_ROOT),
                "--out",
                str(output_root),
                "--code-only",
                "--no-cluster",
                "--no-viz",
            ],
            cwd=REPOSITORY_ROOT,
            check=True,
        )
        return canonical_graph(output_root / "graphify-out" / "graph.json")


def write_graph(content: bytes) -> None:
    """Atomically replace the committed graph with generated content."""
    GRAPH_PATH.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=GRAPH_PATH.parent, delete=False) as temp_file:
        temp_file.write(content)
        temp_path = Path(temp_file.name)
    os.replace(temp_path, GRAPH_PATH)


def main() -> int:
    """Update the graph, failing in check mode if committed output was stale."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if regeneration changes the committed graph",
    )
    args = parser.parse_args()

    generated = build_graph()
    changed = not GRAPH_PATH.exists() or GRAPH_PATH.read_bytes() != generated

    if args.check and changed:
        print(
            "Graphify output is stale. Run `python scripts/update_graphify.py`, "
            "then commit graphify-out/graph.json.",
            file=sys.stderr,
        )
        return 1
    if changed:
        write_graph(generated)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
