#!/usr/bin/env python3
"""Query Pyright or ty language servers with symbol-oriented commands."""

from __future__ import annotations

import argparse
import ast
from functools import cache
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from typing import Any, cast
from urllib.parse import unquote, urlparse

PYRIGHT_VERSION = "1.1.413"
TY_VERSION = "0.0.79"


def backend_command(backend: str) -> list[str]:
    """Return the installed or on-demand language-server command."""
    if backend == "ty":
        if shutil.which("ty"):
            return ["ty", "server"]
        if shutil.which("uvx"):
            return ["uvx", "--from", f"ty=={TY_VERSION}", "ty", "server"]
        raise RuntimeError("ty requires an installed 'ty' executable or 'uvx'")
    if shutil.which("pyright-langserver"):
        return ["pyright-langserver", "--stdio"]
    if shutil.which("npx"):
        return [
            "npx",
            "--yes",
            "--package",
            f"pyright@{PYRIGHT_VERSION}",
            "pyright-langserver",
            "--stdio",
        ]
    raise RuntimeError("Pyright requires an installed 'pyright-langserver' or Node.js with 'npx'")


def doctor() -> int:
    """Report whether the default and optional backends can start."""
    print(f"Python: ready ({sys.version.split()[0]})")
    if path := shutil.which("pyright-langserver"):
        print(f"Pyright: ready ({path}) [default]")
        pyright_ready = True
    elif shutil.which("npx"):
        print(f"Pyright: ready via npx (pyright@{PYRIGHT_VERSION}) [default; downloads on first use]")
        pyright_ready = True
    else:
        print("Pyright: missing (install pyright-langserver or Node.js with npx) [default]")
        pyright_ready = False

    if path := shutil.which("ty"):
        print(f"ty: ready ({path}) [optional]")
    elif shutil.which("uvx"):
        print(f"ty: ready via uvx (ty=={TY_VERSION}) [optional; downloads on first use]")
    else:
        print("ty: unavailable (install ty or uvx) [optional]")
    return 0 if pyright_ready else 2


class LspClient:
    """Minimal synchronous JSON-RPC client for one-shot Python LSP queries."""

    def __init__(self, root: Path, backend: str) -> None:
        """Start and initialize a language server for the workspace."""
        self.backend = backend
        command = backend_command(backend)
        self.root = root.resolve()
        self.process = subprocess.Popen(
            command,
            cwd=self.root,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None if os.environ.get("PYTHON_CODE_INTEL_DEBUG") else subprocess.DEVNULL,
        )
        self.next_id = 1
        self.request(
            "initialize",
            {
                "processId": None,
                "clientInfo": {"name": "python-code-intelligence", "version": "0.1"},
                "rootUri": self.root.as_uri(),
                "workspaceFolders": [{"uri": self.root.as_uri(), "name": self.root.name}],
                "capabilities": {
                    "workspace": {"symbol": {}, "workspaceFolders": True},
                    "textDocument": {
                        "definition": {},
                        "references": {},
                        "hover": {"contentFormat": ["plaintext", "markdown"]},
                        "callHierarchy": {},
                        "typeHierarchy": {},
                    },
                },
            },
        )
        self.notify("initialized", {})

    def _write(self, message: dict[str, Any]) -> None:
        assert self.process.stdin is not None
        payload = json.dumps(message, separators=(",", ":")).encode()
        self.process.stdin.write(f"Content-Length: {len(payload)}\r\n\r\n".encode() + payload)
        self.process.stdin.flush()

    def _read(self) -> dict[str, Any]:
        assert self.process.stdout is not None
        length = None
        while True:
            line = self.process.stdout.readline()
            if not line:
                raise RuntimeError(f"{self.backend} language server exited unexpectedly")
            if line in (b"\r\n", b"\n"):
                break
            name, value = line.decode().split(":", 1)
            if name.lower() == "content-length":
                length = int(value.strip())
        if length is None:
            raise RuntimeError("LSP response omitted Content-Length")
        return cast(dict[str, Any], json.loads(self.process.stdout.read(length)))

    def request(self, method: str, params: Any) -> Any:
        """Send a JSON-RPC request and return its result."""
        request_id = self.next_id
        self.next_id += 1
        self._write({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        while True:
            message = self._read()
            if message.get("id") == request_id and "method" not in message:
                if "error" in message:
                    raise RuntimeError(message["error"].get("message", str(message["error"])))
                return message.get("result")
            if "id" in message and "method" in message:
                self._answer_server_request(message)

    def _answer_server_request(self, message: dict[str, Any]) -> None:
        method = message["method"]
        result: Any
        if method == "workspace/configuration":
            result = []
            for item in message.get("params", {}).get("items", []):
                section = item.get("section")
                if section == "python":
                    result.append({"pythonPath": sys.executable})
                elif section == "python.analysis":
                    result.append({"diagnosticMode": "workspace", "indexing": True})
                else:
                    result.append({})
        elif method == "workspace/workspaceFolders":
            result = [{"uri": self.root.as_uri(), "name": self.root.name}]
        else:
            result = None
        self._write({"jsonrpc": "2.0", "id": message["id"], "result": result})

    def notify(self, method: str, params: Any) -> None:
        """Send a JSON-RPC notification without waiting for a response."""
        self._write({"jsonrpc": "2.0", "method": method, "params": params})

    def open_document(self, location: dict[str, Any]) -> None:
        """Notify the server that a source document is open."""
        uri, _ = position(location)
        path = file_path(uri)
        self.notify(
            "textDocument/didOpen",
            {
                "textDocument": {
                    "uri": uri,
                    "languageId": "python",
                    "version": 1,
                    "text": path.read_text(),
                }
            },
        )

    def close(self) -> None:
        """Shut down the language server process."""
        if self.process.poll() is not None:
            return
        try:
            self.request("shutdown", None)
            self.notify("exit", None)
            self.process.wait(timeout=2)
        except (BrokenPipeError, RuntimeError, subprocess.TimeoutExpired):
            self.process.terminate()


def file_path(uri: str) -> Path:
    """Convert a file URI to a local path."""
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ValueError(f"unsupported URI: {uri}")
    return Path(unquote(parsed.path))


def position(location: dict[str, Any]) -> tuple[str, dict[str, int]]:
    """Extract the URI and starting position from an LSP location."""
    target = location.get("targetUri") or location["uri"]
    selected = location.get("targetSelectionRange") or location["range"]
    return target, selected["start"]


def location_of(item: dict[str, Any]) -> dict[str, Any]:
    """Return the source location attached to a workspace symbol."""
    location = item.get("location")
    if not location or "range" not in location:
        raise RuntimeError(f"language server returned no source range for {item.get('name', 'symbol')}")
    return cast(dict[str, Any], location)


def relative_path(path: Path, root: Path) -> str:
    """Display a path relative to the workspace when possible."""
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def source_line(path: Path, line: int) -> str:
    """Read one stripped source line, returning an empty string on failure."""
    try:
        return path.read_text(errors="replace").splitlines()[line].strip()
    except (IndexError, OSError):
        return ""


def format_location(location: dict[str, Any], root: Path, label: str | None = None) -> str:
    """Format an LSP location as a concise source reference."""
    uri, start = position(location)
    path = file_path(uri)
    suffix = f"  {label}" if label else ""
    snippet = source_line(path, start["line"])
    if snippet:
        suffix += f"  {snippet}"
    return f"{relative_path(path, root)}:{start['line'] + 1}:{start['character'] + 1}{suffix}"


@cache
def qualified_names(path: Path) -> tuple[tuple[str, int, str], ...]:
    """Return names with definition lines and lexical qualification."""
    try:
        tree = ast.parse(path.read_text())
    except (OSError, SyntaxError, UnicodeDecodeError):
        return ()
    output: list[tuple[str, int, str]] = []

    def visit(body: list[ast.stmt], parents: list[str]) -> None:
        for node in body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                label = ".".join([*parents, node.name])
                output.append((node.name, node.lineno - 1, label))
                visit(node.body, [*parents, node.name])

    visit(tree.body, [])
    return tuple(output)


def symbol_label(item: dict[str, Any]) -> str:
    """Build the most specific available label for a workspace symbol."""
    container = item.get("containerName")
    if container:
        return f"{container}.{item['name']}"
    location = location_of(item)
    start = location["range"]["start"]["line"]
    end = location["range"]["end"]["line"]
    matches = [
        label
        for name, line, label in qualified_names(file_path(location["uri"]))
        if name == item["name"] and start <= line <= end
    ]
    return matches[0] if len(matches) == 1 else item["name"]


def symbols(client: LspClient, query: str, path_filter: str | None) -> list[dict[str, Any]]:
    """Search workspace symbols and optionally filter their paths."""
    items = client.request("workspace/symbol", {"query": query}) or []
    for _ in range(10 if client.backend == "pyright" else 0):
        if items:
            break
        time.sleep(0.25)
        items = client.request("workspace/symbol", {"query": query}) or []
    if path_filter:
        items = [
            item
            for item in items
            if path_filter in relative_path(file_path(location_of(item)["uri"]), client.root)
        ]
    return items


def resolve_symbol(client: LspClient, query: str, path_filter: str | None) -> dict[str, Any]:
    """Resolve a user query to exactly one workspace symbol."""
    items = symbols(client, query.rsplit(".", 1)[-1], path_filter)
    tail = query.rsplit(".", 1)[-1]
    exact = [item for item in items if symbol_label(item) == query]
    if not exact:
        exact = [item for item in items if item["name"] == tail]
    folded = query.casefold()
    if not exact:
        exact = [item for item in items if symbol_label(item).casefold() == folded]
    if not exact:
        exact = [item for item in items if item["name"].casefold() == tail.casefold()]
    candidates = exact or items
    if len(candidates) == 1:
        return candidates[0]
    if not candidates:
        raise RuntimeError(f"no symbol matched {query!r}")
    choices = "\n".join(
        "  " + format_location(location_of(item), client.root, symbol_label(item)) for item in candidates[:20]
    )
    raise RuntimeError(f"symbol {query!r} is ambiguous; use a dotted name or --path:\n{choices}")


def text_document_params(item: dict[str, Any]) -> dict[str, Any]:
    """Build position-oriented LSP parameters for a workspace symbol."""
    location = location_of(item)
    uri, start = position(location)
    path = file_path(uri)
    lines = path.read_text(errors="replace").splitlines()
    last_line = min(location["range"]["end"]["line"] + 1, len(lines))
    pattern = re.compile(rf"\b{re.escape(item['name'])}\b")
    for line_number in range(start["line"], last_line):
        match = pattern.search(lines[line_number])
        if match:
            start = {"line": line_number, "character": match.start()}
            break
    return {"textDocument": {"uri": uri}, "position": start}


def hierarchy(
    client: LspClient,
    item: dict[str, Any],
    prepare_method: str,
    query_method: str,
) -> list[tuple[dict[str, Any], str | None]]:
    """Prepare a symbol and query one call or type hierarchy direction."""
    prepared = client.request(prepare_method, text_document_params(item)) or []
    if not prepared:
        return []
    results = client.request(query_method, {"item": prepared[0]}) or []
    output = []
    for result in results:
        target = result.get("from") or result.get("to") or result
        output.append(({"uri": target["uri"], "range": target["selectionRange"]}, target["name"]))
    return output


def print_hover(value: Any) -> None:
    """Print hover content in its server-provided text format."""
    if not value:
        return
    contents = value.get("contents", value)
    if isinstance(contents, dict):
        print(contents.get("value", ""))
    elif isinstance(contents, list):
        print("\n".join(x.get("value", "") if isinstance(x, dict) else str(x) for x in contents))
    else:
        print(contents)


def parse_args() -> argparse.Namespace:
    """Parse and validate command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="workspace root (default: cwd)")
    parser.add_argument("--backend", choices=("pyright", "ty"), default="pyright")
    parser.add_argument("--path", help="keep symbols whose relative path contains this text")
    parser.add_argument("--limit", type=int, default=50, help="maximum output rows (default: 50)")
    parser.add_argument(
        "command",
        choices=(
            "doctor",
            "search",
            "definition",
            "references",
            "callers",
            "callees",
            "hover",
            "supertypes",
            "subtypes",
        ),
    )
    parser.add_argument("symbol", nargs="?")
    args = parser.parse_args()
    if args.command != "doctor" and not args.symbol:
        parser.error(f"the {args.command} command requires a symbol")
    return args


def main() -> int:
    """Run one semantic query and print concise results."""
    args = parse_args()
    if args.command == "doctor":
        return doctor()
    if args.backend == "pyright" and args.command in {"supertypes", "subtypes"}:
        print(f"error: {args.command} requires --backend ty", file=sys.stderr)
        return 2
    try:
        client = LspClient(args.root, args.backend)
    except (OSError, RuntimeError) as error:
        print(f"error: could not start {args.backend}: {error}", file=sys.stderr)
        return 2
    try:
        assert args.symbol is not None
        results: list[tuple[dict[str, Any], str | None]]
        if args.command == "search":
            results = [
                (location_of(item), symbol_label(item)) for item in symbols(client, args.symbol, args.path)
            ]
        else:
            item = resolve_symbol(client, args.symbol, args.path)
            client.open_document(location_of(item))
            params = text_document_params(item)
            if args.command == "definition":
                results = [
                    (location, None) for location in client.request("textDocument/definition", params) or []
                ]
            elif args.command == "references":
                params["context"] = {"includeDeclaration": False}
                results = [
                    (location, None) for location in client.request("textDocument/references", params) or []
                ]
            elif args.command == "hover":
                print_hover(client.request("textDocument/hover", params))
                return 0
            else:
                methods = {
                    "callers": ("textDocument/prepareCallHierarchy", "callHierarchy/incomingCalls"),
                    "callees": ("textDocument/prepareCallHierarchy", "callHierarchy/outgoingCalls"),
                    "supertypes": ("textDocument/prepareTypeHierarchy", "typeHierarchy/supertypes"),
                    "subtypes": ("textDocument/prepareTypeHierarchy", "typeHierarchy/subtypes"),
                }
                results = hierarchy(client, item, *methods[args.command])
        for location, label in results[: args.limit]:
            print(format_location(location, client.root, label))
        return 0
    except (KeyError, OSError, RuntimeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
