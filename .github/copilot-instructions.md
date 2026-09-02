# OpenGHG Copilot Instructions

OpenGHG is a Python package for greenhouse gas data processing, standardisation,
storage, retrieval, analysis, and plotting. Keep changes small, scientific-data
safe, and consistent with the surrounding module patterns.

## First Checks

- Use `graphify-out/` for a quick map of the repository before broad searches:
  - `graphify-out/graph.json` Maps how modules, components, and files import one another. Data flow and impact analysis.
  - `graphify-out/manifest.json` Defines the project name, description, version, and global entry points. API surfaces and asset paths(configs).
- Treat `graphify-out/` as generated context. Do not edit it unless explicitly
  asked.
- After graphify gives orientation, read the source and tests that are directly
  relevant to the requested change. Do not rely on graphify metadata alone for
  code edits.

## Project Layout

- `openghg/` is the package source.
- `tests/` mirrors the package structure and contains test data under
  `tests/data/`.
- `openghg/standardise/` parses and standardises incoming scientific datasets.
- `openghg/store/`, `openghg/storage/`, and `openghg/objectstore/` handle stored
  data and metadata.
- `openghg/dataobjects/` contains user-facing data object classes.
- `openghg/retrieve/`, `openghg/analyse/`, `openghg/transform/`, and
  `openghg/util/` contain retrieval, analysis, transformation, and shared helper
  code.

## Coding Guidance

- Match existing public APIs, metadata keys, xarray/pandas conventions, and test
  helper patterns before adding new ones.
- Preserve backwards compatibility for data formats and metadata unless the task
  explicitly changes a contract.
- Prefer typed, explicit helpers over duplicating parsing or validation logic.
- Keep package data paths relative to the package or test helper APIs. Avoid
  hard-coded absolute paths.
- The project uses Ruff for linting and formatting with `line-length = 110`.
- Add or update focused tests next to the affected module when behaviour changes.

## Environment

Use `uv` for agent-run environment setup:

```bash
uv sync --extra dev
python -c "import openghg"
```

If dependencies for optional documentation work are needed:

```bash
uv sync --all-extras
python -c "import openghg"
```

Do not create Pixi environments during automated agent runs; Pixi environment
creation can halt or take too long. Use Conda or Micromamba only if uv is
unavailable and the task cannot be validated otherwise.

## Validation

Run the smallest useful validation for the change. Prefer targeted tests over the
full test suite.

```bash
uv run python -m compileall -q openghg
uv run ruff format --check openghg
uv run ruff check openghg
uv run mypy openghg
uv run pytest tests/path/to/test_file.py
```

If `uv run` is unavailable but the environment is already active, run the same
tools directly:

```bash
python -m compileall -q openghg
ruff format --check openghg
ruff check openghg
mypy openghg
pytest tests/path/to/test_file.py
```

Special test handling:

- Tests marked `cfchecks` require `--run-cfchecks`.
- Tests marked `icos` require `--run-icos` and may use the network.
- Tests marked `xesmf` require optional regridding dependencies.
- Pytest has a default timeout of 300 seconds in `pyproject.toml`.

## Pull Request Behaviour

- Explain which targeted tests or checks were run.
- If a relevant check could not run because dependencies, network access, or
  compiled system libraries were unavailable, say so explicitly.
- Do not include generated caches, local object stores, virtual environments, or
  graphify output in code changes unless the task explicitly asks for them.
