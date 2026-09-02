# CLAUDE.md

Guidance for agents working in this repo.

## Writing

Prose here follows ISO 24495-1.
One idea per sentence, active voice, common words, the point first.

Docstrings follow [numpydoc](https://numpydoc.readthedocs.io/en/latest/format.html), enforced by `convention = "numpy"` in `ruff.toml`.

### Examples use markdown fences, not doctest prompts

**This overrides the global rule.**
[Great Docs](https://posit-dev.github.io/great-docs/user-guide/writing-docstrings.html) builds this package's site on Quarto and asks for ` ```{python} ` executable cells, which it runs at build time and embeds the output of.
Doctest is not its syntax.

The fences say ` ```python ` for now rather than ` ```{python} `, because ruff's `docstring-code-format` only reaches the plain spelling.
Adding the braces buys execution and costs that formatting, so do both in one pass once the site is wired up.
Drop the `# 'value'` comments in the same pass: Quarto prints the real return value, and the comment would then say it twice.

Hovering a fence in Zed renders `&nbsp;` wherever the code is indented.
That is a pyrefly bug, not something to write around.
Its `textDocument/hover` converts the whole docstring to markdown and replaces leading whitespace with `&nbsp;`, with no awareness of fences.
Doctest prompts dodge it because they start with a non-space character, which is not a reason to go back to them.

## Agent skills

Per-repo configuration for the mattpocock engineering skills.

The user-level skills: dagster-expert, polars, dataframely and the polars MCP are also relevant.

### Issue tracker

Issues live as GitHub issues in `ozanozbeker/dagster-dataframely`, driven by the `gh` CLI.
See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles, using the default label strings.
See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `CONTEXT.md` and `docs/adr/` at the repo root.
See `docs/agents/domain.md`.
