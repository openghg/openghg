# OpenGHG Agent Instructions

Use the least tokens that still solve the task: read narrowly, edit narrowly, answer briefly.

## Safety

- Work only inside the `/openghg` repo. Do not request or use write permissions outside it.
- Do not commit, push, tag, release, or alter git history unless explicitly told.
- Preserve user changes. Check `git status --short` before edits when relevant.
- Prefer local context over broad search. Use network only when required.

## Update Style

- Use graphify to map dependencies, affected files, and validation scope before non-trivial edits.
- Use caveman for updates: simple, direct, minimal abstractions, no unrelated refactors.
- Follow existing code patterns and public APIs.
- Keep comments and docs concise; add them only when they remove real ambiguity.

## Repo Basics

- Package: `openghg/`
- Tests: `tests/`
- Docs: `doc/`
- Config: `pyproject.toml`, `mypy.ini`, `.pre-commit-config.yaml`, `tox.ini`
- CI: `.github/workflows/workflow.yaml`

## Environment

Preferred setup:

```bash
micromamba create --name openghg_dev python=3.12 -y
micromamba activate openghg_dev
uv sync --all-extras
```

Fallback:

```bash
conda env create -f environment-dev.yaml
pip install -e ".[dev]"
```

## Validate

Run only checks relevant to the change:

```bash
black --check openghg/
flake8 openghg/ --count --statistics
mypy --python-version 3.12 openghg/
pytest tests/path_or_file.py
```

Prefer targeted pytest files or directories. Avoid full suite, docs build, tox, `--run-icos`, and `--run-cfchecks` unless directly needed.

Useful smoke checks:

```bash
python -m openghg --help
python -c "import openghg"
python -m py_compile path/to/file.py
```

## Output

- Lead with what changed and how it was validated.
- Mention skipped checks or blockers briefly.
- Keep final responses short and efficient.
