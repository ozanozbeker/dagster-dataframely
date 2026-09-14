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

The fences say ` ```{python} `, and a docstring `Examples` block executes when the site builds.
That was the trade: braces buy execution and give up ruff's `docstring-code-format`, which only reaches the plain spelling.
So an `Examples` block is the one body of Python in this repo no formatter reads, and it is on you to keep it formatted.
Write no `# 'value'` comment: Quarto prints the real return value, and the comment would say it twice.

`README.md` is the exception and keeps plain ` ```python `, because GitHub and PyPI do not know the `{python}` info string and render it unhighlighted.
A pre-render script braces the build directory's copy instead.
ADR-0009.

Hovering a fence in Zed renders `&nbsp;` wherever the code is indented.
That is a pyrefly bug, not something to write around.
Its `textDocument/hover` converts the whole docstring to markdown and replaces leading whitespace with `&nbsp;`, with no awareness of fences.
Doctest prompts dodge it because they start with a non-space character, which is not a reason to go back to them.

## Naming

`CONTEXT.md` decides which word a thing gets.
These two rules decide the shape of the identifier.

### Name a function for its product

A function that returns a value is named after the value, not after what it does.
`check_specs` returns check specs, `quarantine_frame` returns the quarantine frame, `delegating_writer` returns a writer, `frame_and_result` returns a frame and a returned result.

Where the product has no name, name it rather than reaching for a verb: `described_rules` returns `DescribedRule`s, so the function is its record's own name in snake case.
A verb name says the function returns nothing, so `validate_quarantine_key` either raises or passes.

No prefix: the annotation carries the type, and the prefix is a word the reader skips.
D401 keeps every summary imperative, so `described_rules` still opens "Return the schema's validation rules".

Avoid `get_*`, `build_*`, `make_*`, `compute_*`.

### Name a transformer with a participle

A function handed a thing that hands back the same thing changed is named by the participle of what changed.
`_addressed` returns check results carrying the address, `_suffixed` returns key parts with the last suffixed, `_checked` returns a value the setting allows.

Where only part of what it was handed changes, the participle overclaims.
Borrow Polars' `with_*` instead: `with_returned_fields` hands back the same results with three fields on one of them.

Avoid `apply_*`, `add_*`, `enrich_*`.

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

Single-context: `CONTEXT.md`, `docs/adr/` and `docs/out-of-scope/` at the repo root.
See `docs/agents/domain.md`.

## Where prose goes

Five homes, and a sentence belongs in exactly one.

- `README.md` is a landing page and a quick start, and it is also PyPI's long description and the docs site's landing page.
  What the package is, the one example, install, and links out.
- `user_guide/` is how to use it, one `.qmd` page per topic.
  Every behaviour, every setting, every error, with worked examples.
  Every code cell executes when the site builds, so an example that stopped being true fails the build.
- `ARCHITECTURE.md` is how the parts fit, for someone about to change one.
  It is not published to the site: it addresses a maintainer, not a user.
- Docstrings and comments are why the code is the way it is.
  A reader can reconstruct usage from the signature and the guide; they cannot reconstruct a measurement, a declined alternative, or why a private upstream API is pinned.
- `docs/adr/` is a decision that was hard to reverse, and `docs/research/` is the measurement behind one.

So a docstring does not teach the feature, and the guide does not argue the implementation.
A numpydoc `Parameters` entry earns its place only when the name and the annotation do not already carry it, which `convention = "numpy"` leaves to you by disabling D417.
