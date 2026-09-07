# Claude Guidance for OpenGHG

OpenGHG is a Python package for greenhouse gas data processing, standardisation,
storage, retrieval, analysis, and plotting. Limit each change to the minimum
files required to satisfy the task. Do not refactor unrelated code in the same
edit. Keep changes scientific-data safe and aligned with nearby module and
test patterns.

## Work Efficiently

- Start with `graphify-out/` before broad repository searches. Query it with
  `jq`; do not load `graphify-out/graph.json` wholesale because it is large.
- `graphify-out/manifest.json` indexes repository paths. Use it to find likely
  files quickly.
- `graphify-out/graph.json` contains `nodes` and `links` for symbols, imports,
  relations, and source locations. Use it for impact analysis, then read the
  actual source and tests before editing.
- After graphify orientation, use `rg` only in likely directories and open the
  smallest relevant file set.
- Treat `graphify-out/` as generated. Do not edit it unless explicitly asked.
- This file distills `.github/copilot-instructions.md`; do not re-read that
  file, `README.md`, or `CHANGELOG.md` during routine orientation unless the
  task specifically concerns them.

Useful queries:

```bash
jq -r 'keys[] | select(test("standardise|store|retrieve"))' graphify-out/manifest.json
jq -r '.nodes[] | select(.source_file | test("^openghg/standardise/")) | [.id, .label, .source_file, .source_location] | @tsv' graphify-out/graph.json
jq -r '.links[] | select(.source_file == "openghg/store/_obssurface.py") | [.relation, .source, .target, .source_location] | @tsv' graphify-out/graph.json
```

## Repository Map

- `openghg/`: package source.
- `tests/`: pytest suite and `tests/data/` fixtures.
- `openghg/standardise/`: parsing and standardising incoming scientific data.
- `openghg/store/`, `openghg/storage/`, `openghg/objectstore/`: persisted data,
  metadata, indexing, object stores, and zarr handling.
- `openghg/dataobjects/`: user-facing data object classes.
- `openghg/retrieve/`, `openghg/analyse/`, `openghg/transform/`,
  `openghg/util/`: retrieval, analysis, transformation, and shared helpers.

## Code Expectations

- Preserve public APIs, metadata keys, file formats, and xarray/pandas behaviour
  unless the requested task changes them.
- Follow existing helper APIs, validators, fixtures, and data conventions.
- Prefer typed, explicit helpers over duplicated parsing or validation logic.
- Keep data paths relative to package/test helper APIs; avoid hard-coded
  absolute paths.
- Format with Black, `line-length = 110`.
- Add focused tests next to affected code when behaviour changes.
- Keep generated caches, local object stores, virtual environments, and
  `graphify-out/` out of changes unless explicitly requested.

## Environment And Checks

Use `uv` first. Fallback order:

1. Use `uv` (preferred).
2. If `uv` is unavailable, use an already-active Conda or Micromamba environment.
3. Do not create new Pixi environments.

Never run `sudo` commands.

```bash
uv sync --extra dev
uv run python -c "import openghg"
```

For documentation extras:

```bash
uv sync --all-extras
uv run python -c "import openghg"
```

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

Tests marked `cfchecks` require `--run-cfchecks`; `icos` tests require
`--run-icos` and may use the network; `xesmf` tests need optional regridding
dependencies. Pytest timeout defaults to 300 seconds.

When reporting back, list the files changed and exact checks run. If a
relevant check could not run because of dependencies, network access, or
compiled system libraries, say so. If a validation check reports failures
unrelated to the current change (pre-existing issues), note them in the report
but do not fix them unless the task explicitly asks for it.

Do not trigger GitHub Actions CI workflows. Run only the local validation
commands listed in the Validation section.
