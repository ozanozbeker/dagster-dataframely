# CLAUDE.md

Guidance for agents working in this repo.

## Writing

Prose here follows ISO 24495-1.
One idea per sentence, active voice, common words, the point first.
This covers docstrings, comments, the docs, config comments and error messages.

Docstrings follow [numpydoc](https://numpydoc.readthedocs.io/en/latest/format.html), enforced by `convention = "numpy"` in `ruff.toml`.

### Say what happens, without figures of speech

No metaphors, analogies or personification.
Code does not refuse, decide, know, learn, ask, answer or promise: it raises, sets, reads, receives, returns and guarantees.
Rows do not land, arrive, ride or survive: they are written, passed, copied and kept.
`CONTEXT.md` lists the words to avoid and what to write instead.

Use the word Dagster, Polars or Dataframely already uses, in the meaning it has there.
A step is Dagster's, a member is Dataframely's, and Polars collects a plan.
Coin a term only for something the code names, such as a function, a class or a setting, and define it in `CONTEXT.md`.

### Keep prose short

Across `src/`, docstrings and comments stay under 40% of the characters.

- A private function's docstring is its summary line.
  Add a sentence only for a reason that the name, the annotation and the body do not show.
- A module docstring is at most three sentences: what the module holds, how it relates to the modules beside it, and the one constraint a maintainer must know.
- A comment is one line, and only for a reason the code does not show.
  No history, and no restating the code.
- A test's docstring is one sentence naming the behaviour under test.
- A measurement goes in `docs/research/`, and a declined design goes in `docs/out-of-scope/` or an ADR.
  The docstring links to it in one sentence.

### Examples use markdown fences, not doctest prompts

**This overrides the global rule.**
[Great Docs](https://posit-dev.github.io/great-docs/user-guide/writing-docstrings.html) builds this package's site on Quarto and expects ` ```{python} ` executable cells: it runs them at build time and embeds their output.
Doctest is not its syntax.

Quarto executes a fence only when its info string is `{python}`.
Where the braces would cause a problem, the build adds them instead of the author (ADR-0009).

A `user_guide/` page has the braces in source.
GitHub does not render a `.qmd`, and `ruff.toml` maps the extension to markdown, so ruff formats its chunks either way.

A diagram needs the braces too: Quarto draws only a ` ```{mermaid} ` cell, and a plain ` ```mermaid ` fence appears on the site as source text.

`README.md` and docstrings keep plain fences, and a pre-render script adds the braces to the build directory's copies.
GitHub and PyPI render the README and do not recognize the `{python}` info string, so a braced fence there loses its highlighting.
Ruff's `docstring-code-format` formats only plain fences in a docstring, so nothing would format a braced one.

Ruff formats a chunk but does not lint it, so nothing reports an unused import or an ambiguous name inside a chunk.

Do not add a `# 'value'` comment to an example: Quarto prints the real return value, so the comment would repeat it.

Hovering a fence in Zed shows `&nbsp;` wherever the code is indented.
That is a pyrefly bug, so do not work around it.
Pyrefly's `textDocument/hover` converts the whole docstring to markdown and replaces leading whitespace with `&nbsp;`, including inside fences.
Doctest prompts avoid the bug because they start with a non-space character, but that is not a reason to go back to them.

## Naming

`CONTEXT.md` lists the word for each thing.
These two rules set how an identifier is built.

### Name a function for what it returns

A function that returns a value is named after the value, not after what it does.
`check_specs` returns check specs, `quarantine_frame` returns the quarantine frame, `delegating_writer` returns a writer, `frame_and_result` returns a frame and a returned result.

Where the value has no name, give it one instead of using a verb: `described_rules` returns `DescribedRule`s, so the function is the record's name in snake case.
A verb name means the function returns nothing: `validate_quarantine_key` either raises or returns `None`.

No prefix: the annotation already gives the type.
D401 still requires an imperative summary, so `described_rules` starts with "Return the schema's validation rules".

Avoid `get_*`, `build_*`, `make_*`, `compute_*`.

### Name a transformer with a participle

A function that takes a value and returns it changed is named by the participle of the change.
`_addressed` returns check results with the quarantine address added, `_suffixed` returns key parts with a suffix on the last one, `_checked` returns a value the setting allows.

Where only part of the value changes, a participle is misleading.
Use Polars' `with_*` prefix instead: `with_returned_fields` returns the same results with three fields set on one of them.

Avoid `apply_*`, `add_*`, `enrich_*`.

## Design priority

End user first, maintainer legibility second, and convenience for hand-wiring is never a goal.
ADR-0001 records the last of these: hand-wiring never changes the decorator's design.

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

Four places, and each sentence goes in exactly one.

- `README.md` is a landing page and a quick start, and it is also PyPI's long description and the docs site's landing page.
  What the package is, the one example, install, and links out.
  Its code blocks are copies of `demo/` modules that a prek hook keeps in sync, so edit the demo module and let the hook rewrite the README.
- `user_guide/` is how to use the package, one `.qmd` page per topic.
  Every behaviour, every setting, every error, with worked examples.
  Every code cell runs when the site builds, so an example that no longer works fails the build.
- Docstrings and comments give the reason the code is the way it is, within the limits in "Keep prose short".
- `docs/adr/` records a decision that was hard to reverse, and `docs/research/` records the measurement behind one.

So a docstring does not explain how to use a feature, and the guide does not explain the implementation.
Write a numpydoc `Parameters` entry only when the name and the annotation do not already explain the parameter.
`convention = "numpy"` disables D417, so ruff does not require one.
