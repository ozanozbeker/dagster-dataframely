# CLAUDE.md

Guidance for agents working in this repo.

## Writing

Prose here follows ISO 24495-1.
Write one idea per sentence, in the active voice, with common words and the point first.
This covers docstrings, comments, the docs, config comments and error messages.

Docstrings follow [numpydoc](https://numpydoc.readthedocs.io/en/latest/format.html), enforced by `convention = "numpy"` in `ruff.toml`.

### Say what happens, without figures of speech

Do not use metaphors, analogies or personification.
Code does not refuse, decide, know, learn, ask, answer or promise: it raises, sets, reads, receives, returns and guarantees.
Rows do not land, arrive, ride or survive: the package writes, passes, copies and keeps them.
`CONTEXT.md` lists the words to avoid and what to write instead.

Give every sentence a verb, including a heading's description or a caption: "The decorator, and the spec that puts the error rows in the graph" becomes "`dd.asset` builds an asset, and `build_error_rows_spec` adds its error rows to the asset graph".

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
- A declined design goes in `docs/out-of-scope/`, and a measurement taken before 1.0 is in `docs/pre-1.0.md`.
  The docstring links to it in one sentence.

### Examples use markdown fences, not doctest prompts

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

An example imports its schema and frames from `dd.data`, which the package ships, so it runs for any user from an editor hover.
The demo and the guide import the same names, so every screenshot, guide cell and docstring shows one `OrdersSchema`.
Define a schema inline only for a rule `OrdersSchema` lacks.

Hovering a fence in Zed shows `&nbsp;` in front of every indented line.
That is a pyrefly bug, so do not work around it.
Pyrefly's `textDocument/hover` converts the whole docstring to markdown and replaces leading whitespace with `&nbsp;`, including inside fences.
Doctest prompts avoid the bug because they start with a non-space character, but that is not a reason to go back to them.

## Naming

`CONTEXT.md` lists the word for each thing.
These rules set the shape of an identifier.

### Name a function with a verb

Every function that returns something is named with a verb, as the tidyverse style guide, Dagster and dagster-dbt name theirs.
Three tiers set the verb.

1. A function that returns a Dagster object is `build_*`: `build_rule_checks`, `build_asset_check_specs`, `build_sample_metadata`.
   Dagster uses the prefix for the same thing, in `build_column_schema_change_checks` and `build_op_context`.
2. A function that returns a plain value is a verb from the upstream vocabulary where one exists.
   `get_*` derives a value from a schema, as dagster-dbt's `DagsterDbtTranslator.get_code_version` does: `get_code_version`, `get_check_name_for_rule`.
   `evaluate_*` reads data and reports pass or fail, from Dagster's `AssetCheckEvaluation`: `evaluate_checks`.
   `render_*` and `summarize_*` make text, `stream_*` yields Dagster results, from dagster-dbt's `.stream()`.
3. A noun names only a constructor of a record this package defines: `DescribedRule(...)`, `DirectInvocationResult(...)`.
   A function that makes one such record from something else is a classmethod in Polars' form: `DirectInvocationResult.from_invocation`, `DescribedRule.from_rule`.

A function that returns nothing is a verb too: `validate_error_rows_key` raises or returns `None`.

### A schema ends in `Schema`

A `dy.Schema` subclass ends in `Schema`, and a `dy.Collection` subclass does not, as Dataframely names its own: `OrdersSchema`, `Invoices`.
The asset keeps the bare noun: `orders` is the asset, `OrdersSchema` is its schema.
This holds in `src/`, `tests/`, `demo/` and the guide.

### Change part of a value with `with_*`

A function that returns the same value with part of it changed uses Polars' prefix: `with_returned_fields` returns the same results with three fields set on one of them.
Everything else that transforms a value takes the verb of the change: `_add_address`, `_separate_warned_and_error_rows`, `_to_snake_case`.

Avoid a participle as a function name (`_addressed`, `_suffixed`), and avoid `apply_*`, `enrich_*`, `make_*`, `compute_*`.

## Tests

A test file groups its tests into classes when it asserts against more than one target.
A file with one target stays flat.

A target is what a group of tests asserts against, such as the check specs of an `AssetsDefinition` or a run with no valid rows.

Name a class `Test` plus its target, such as `TestCheckSpecs` or `TestSkip`.
Its docstring is one sentence naming the target.

Only tests go in a class.
Fixtures, helpers and schemas stay at module level, because the classes in a file share most of them.

## Design priority

End user first, maintainer legibility second, and convenience for hand-wiring is never a goal.
ADR-0001 records the last of these: hand-wiring never changes the decorator's design.

## Agent skills

This section configures the mattpocock engineering skills for this repo.

The user-level skills: dagster-expert, polars, dataframely and the polars MCP are also relevant.

### Issue tracker

Issues live as GitHub issues in `ozanozbeker/dagster-dataframely`, driven by the `gh` CLI.
See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles, using the default label strings.
See `docs/agents/triage-labels.md`.

### Domain docs

This repo is single-context: `CONTEXT.md`, `docs/pre-1.0.md` and `docs/out-of-scope/` sit at the repo root.
See `docs/agents/domain.md`.

## Where prose goes

Four places, and each sentence goes in exactly one.

- `README.md` is a landing page and a quick start, and it is also PyPI's long description and the docs site's landing page.
  What the package is, the one example, install, and links out.
  Its code blocks are copies of `demo/` modules that a prek hook keeps in sync, so edit the demo module and let the hook rewrite the README.
- `user_guide/` is how to use the package, one `.qmd` page per topic, in two sections that `great-docs.yml` lists as pkgdown lists `articles:`.
  "Get started" is the tour, named after the package as a pkgdown vignette is.
  "Guide" pages are read once, in order.
  "Details" pages are looked up: Settings, Naming, Errors, Hand-wiring, Upgrading.
  Every code cell runs when the site builds, so an example that no longer works fails the build.
- Docstrings are the reference pages, in pkgdown's shape: every owned parameter, `Returns`, `Raises`, one `Examples` block that runs, and the reason the code is the way it is, within the limits in "Keep prose short".
  The numpydoc hook in `prek.toml` enforces the shape.
- `docs/pre-1.0.md` records the decisions and the measurements the package was built on.
  It is closed at 1.0, so a new decision goes in the module that implements it.

So a docstring shows the one call and the guide tells the story, and the guide does not explain the implementation.
