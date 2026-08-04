# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`dagster-dataframely` — a Dagster plugin for [dataframely](https://github.com/Quantco/dataframely) (Polars schema validation).

## Commands

All tooling runs through `uv`; the dev dependency group is installed with `uv sync`.

```sh
uv run pytest                      # full test suite
uv run pytest path/to/test.py::test_name   # single test
uv run ruff check --fix            # lint
uv run ruff format                 # format
uv run ty check                    # type check
uv run rumdl check --fix           # markdown lint
uv run prek run --all-files        # every hook, as CI/commit would
uv lock --check                    # verify uv.lock matches pyproject.toml
```

## Conventions

- **Hooks run via `prek`, not `pre-commit`** — config lives in `prek.toml` (TOML, not `.pre-commit-config.yaml`).
  Install with `uv run prek install --hook-type pre-commit --hook-type commit-msg`.
- **Commit messages must be Conventional Commits** (`--strict`), enforced at `commit-msg`.
- **Ruff runs `select = ["ALL"]`.**
  Expect docstrings (Google convention) and full type annotations on new library code.
  Per-file ignores in `ruff.toml` already carve out `tests/`, `scripts/`, `notebooks/`, `cli/`, `defs/`, and `definitions.py`.
- **`dataframely.rule` is registered as a classmethod decorator** in `[lint.pep8-naming]`, and Dagster's `Config`/`ConfigurableResource`/`ConfigurableIOManager` plus the `@asset`/`@op`/`@asset_check` decorators are marked runtime-evaluated for `flake8-type-checking` — so their annotations must stay out of `if TYPE_CHECKING` blocks.
- **Markdown is written sentence-per-line** (`rumdl.toml` sets `reflow-mode = "sentence-per-line"`, Quarto flavor).
  Keep one sentence per line in `.md` files.
- Build backend is `uv_build` with a `src/` layout; the package ships `py.typed`.
- Target Python is 3.12 (`.python-version`, `requires-python = ">=3.12"`).
