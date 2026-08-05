# Agent Instructions for OpenGHG

Scope: the whole repository.

OpenGHG is a Python package for greenhouse gas data processing, standardisation,
storage, retrieval, analysis, and plotting. Limit each change to the minimum
files required to satisfy the task. Do not refactor unrelated code in the same
edit. Keep changes scientific-data safe and consistent with nearby code and
tests.

## Token-Efficient Workflow

1. Start with `graphify-out/` for orientation before broad source searches.
  Query it surgically; do not load or paste the full JSON files.
2. Use `graphify-out/manifest.json` to find candidate files. Top-level keys are
   repository paths, and values contain generated hashes/metadata.
3. Use `graphify-out/graph.json` to inspect modules, symbols, imports, and
   likely impact paths. Useful fields are `nodes`, `links`, `source_file`,
   `source_location`, `label`, `relation`, `source`, and `target`.
4. After graphify orientation, read only the source, tests, and config directly
   relevant to the task. Use `rg` inside likely paths instead of scanning the
   whole repository by default.
5. Treat `graphify-out/` as generated context. Do not edit it unless explicitly
   asked.
6. This file distills `.github/copilot-instructions.md`; do not re-read that
   file, `README.md`, or `CHANGELOG.md` during routine orientation unless the
   task specifically concerns them.

Useful graphify queries:

```bash
jq -r 'keys[] | select(test("standardise|store|retrieve"))' graphify-out/manifest.json
jq -r '.nodes[] | select(.source_file | test("^openghg/standardise/")) | [.id, .label, .source_file, .source_location] | @tsv' graphify-out/graph.json
jq -r '.links[] | select(.source_file == "openghg/store/_obssurface.py") | [.relation, .source, .target, .source_location] | @tsv' graphify-out/graph.json
```

## Project Map

- `openghg/`: package source.
- `tests/`: pytest suite mirroring package areas; test data lives under
  `tests/data/`.
- `openghg/standardise/`: incoming scientific dataset parsing and
  standardisation.
- `openghg/store/`, `openghg/storage/`, `openghg/objectstore/`: stored data,
  metadata, indexing, local object stores, and zarr handling.
- `openghg/dataobjects/`: user-facing data object classes.
- `openghg/retrieve/`, `openghg/analyse/`, `openghg/transform/`,
  `openghg/util/`: retrieval, analysis, transformation, and shared helpers.

## Coding Rules

- Preserve public APIs, metadata keys, file formats, and xarray/pandas behaviour
  unless the task explicitly changes a contract.
- Match existing helper APIs, data conventions, validation style, and test
  fixtures before introducing new patterns.
- Prefer typed, explicit helpers over duplicated parsing or validation logic.
- Keep package and test data paths relative to package/test helper APIs. Avoid
  hard-coded absolute paths.
- Use Black formatting with `line-length = 110`.

- Add or update focused tests beside the affected code when behaviour changes.
- Do not include generated caches, local object stores, virtual environments, or
  `graphify-out/` changes unless explicitly requested.

## Environment

Prefer `uv` for agent-run setup and commands. Fallback order:

1. Use `uv` (preferred).
2. If `uv` is unavailable, use an already-active Conda or Micromamba environment.
3. Do not create new Pixi environments.

```bash
uv sync --extra dev
uv run python -c "import openghg"
```

For optional documentation work:

```bash
uv sync --all-extras
uv run python -c "import openghg"
```

Do not create Pixi environments during automated agent runs; Pixi setup can halt
or take too long. Use Conda or Micromamba only if `uv` is unavailable and the
task cannot be validated otherwise.

## Validation

Run, at minimum, `compileall` and the single test file most directly covering
the changed code. Add `black --check` and `flake8` if formatting or style was
touched. If a change affects typing, include `mypy` for the affected module.

```bash
uv run python -m compileall -q openghg
uv run black --check openghg
uv run flake8 openghg
uv run mypy openghg
uv run pytest tests/path/to/test_file.py
```

If `uv run` is unavailable but an environment is already active, run the same
tools directly. If a validation check reports failures unrelated to the
current change (pre-existing issues), note them in the report but do not fix
them unless the task explicitly asks for it.

Do not trigger GitHub Actions CI workflows. Run only the local validation
commands listed in the Validation section.

Special test handling:

- Tests marked `cfchecks` require `--run-cfchecks`.
- Tests marked `icos` require `--run-icos` and may use the network.
- Tests marked `xesmf` require optional regridding dependencies.
- Pytest has a default timeout of 300 seconds in `pyproject.toml`.

## Reporting

When finishing, state the focused files changed and the exact checks run. If a
relevant check could not run because of dependencies, network access, or compiled
system libraries, say so explicitly.
