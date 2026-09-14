# 9. Every fence stays plain markdown and the build braces it

Accepted, 2026-09-14. Scope is the documentation site (#120), not the package.

## Context

Great Docs generates the site's landing page from `README.md`.
Quarto decides whether a fence executes by its info string: ` ```{python} ` runs at build time and embeds its output, ` ```python ` renders static.
So the README's three Python blocks execute only if the source carries braces.

The README is not only a site page.
`pyproject.toml` sets `readme = "README.md"`, so it is also PyPI's long description, and it is what GitHub renders on the repository's front page.
Neither of those knows `{python}` as a language.
Braced fences lose syntax highlighting on both, and the first block a reader meets is the package's flagship example.

That is the whole trade: the three most-copied blocks in the repository are either proven or highlighted, and the obvious spellings cannot give both.

## Decision

**Every fence in the repository stays plain ` ```python `, and a pre-render script braces the build directory's copies.**

`great-docs.yml` takes a top-level `pre_render:` key, which Great Docs copies into the generated `_quarto.yml` as Quarto's native `project: pre-render:`.
Great Docs generates `great-docs/index.qmd` from the README before Quarto renders, so the script finds the copy already there and rewrites it in place.
The file on disk is never touched.

GitHub and PyPI keep highlighting.
The site executes all three blocks, so a raising cell fails the build, which is the gate #83 asked for.

**A docstring is the second surface, for a different reason.**
Great Docs renders docstrings as the reference pages, so an `Examples` block only executes if it is braced. But ruff's `docstring-code-format` reaches the plain spelling only, so bracing one in the source takes it out of the formatter's reach: measured by mangling code inside a braced fence and having `ruff format` report the file already formatted, under `preview` too.
The script rewrites each reference page's `## Examples {.doc-examples}` section and nothing else, because Great Docs emits the function signature as its own ` ```python ` block and executing a signature would fail the build.

The guide's pages are the exception that needs no script: a `.qmd` is not rendered by GitHub, and `ruff.toml` maps the extension to markdown so the formatter reaches its chunks in the source spelling.

## Consequences

**A reader will try to fix this.**
A plain fence in a repository whose docs site executes everything else looks like an oversight.
That is what this file is for.

**The rewrite is unconditional.**
Every Python fence in the README executes, because the README has no example that should not run.
A block that must stay static would need the script to learn an exception, and the honest move then is to ask whether it belongs in the README at all.

**The gate proves less for the README than for the guide.**
Its three blocks declare a schema, decorate two assets and construct a `dg.Definitions`; none runs a materialization.
So this catches an import break or a decorator signature change, and says nothing about what a run emits.

**Losing the key degrades silently, so the script checks its own work.**
`pre_render` appears in the template `great-docs config` generates and in the repository's own feature list, but not in the published configuration reference, and Great Docs is 0.17.0.
It writes `project: pre-render` into the generated `_quarto.yml` only when the list is non-empty, so a renamed key, or `pre-render:` typed for `pre_render:`, omits the hook with no warning: every cell renders static and the build stays green.
Nothing in Quarto or Great Docs can catch that, so the script counts the Python fences in `README.md` and fails unless the landing page ends with that many executable cells.
The same count catches the likelier failure, one fence gaining an attribute the pattern does not match, which a check against zero could not see.

**`rumdl` stays quiet on the README.**
A braced fence in a `.md` file trips MD040, since rumdl reads `{python}` as a missing language there rather than as a chunk.
Leaving the source plain sidesteps that without a config exception.
The guide's `.qmd` pages trip MD078 instead and answer it with `#| label:` on every chunk.

## Alternatives rejected

**Brace the README in source.**
Simplest, and it needs no script.
Rejected because it degrades the two surfaces most readers actually use to pay for one they mostly do not.

**Leave the README plain and cover its blocks with a test.**
`tests/test_readme.py` extracting and executing three blocks, which is what #83 originally proposed before #120 folded it in.
Rejected because the pre-render script gets the same proof out of the build that already runs, and a second executor is a second thing to keep true.

**Leave the README plain and accept the blocks are unproven.**
Rejected: the package's most-copied example would be the only code in the repository nothing verifies.

**Supply a separate `index.qmd` as the landing page.**
Great Docs prefers a root `index.qmd` over the README when one exists.
Rejected because the flagship example would then live in two files, and they would drift.
