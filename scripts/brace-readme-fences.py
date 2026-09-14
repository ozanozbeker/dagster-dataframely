# A build script Quarto runs, not package code: `scripts/` is deliberately not a package
# (INP001), and what this writes to stdout is its interface, because Quarto shows it in
# the build log (T201). A file-level exception rather than one in `ruff.toml`, so it stays
# attached to the one file that earns it.
# ruff: noqa: INP001, T201
"""Make the landing page's Python fences executable, without touching `README.md`.

Quarto decides whether a fence runs by its info string: ` ```{python} ` executes and embeds
its output, ` ```python ` renders static. `README.md` is also PyPI's long description and
GitHub's front page, and neither of those knows `{python}` as a language, so bracing the
source would cost syntax highlighting on the two surfaces most readers actually use.

Great Docs generates `index.qmd` from `README.md` before Quarto renders, and runs this as
Quarto's `project: pre-render:` hook, with the build directory as the working directory. So
the copy is already there to rewrite and the file on disk is never touched. ADR-0009.

Every Python fence is rewritten, because the README has no example that should not run. A
block that has to stay static would need an exception here, and the honest question then is
whether it belongs in the README at all.
"""

import pathlib
import re
import sys

FENCE = re.compile(r"^(\s*)```python\s*$", re.MULTILINE)


def main() -> int:
    """Brace every Python fence in the generated landing page.

    Returns
    -------
    A process exit code: non-zero when the landing page rewrote to no executable cell,
    which means Great Docs changed how it generates one and the site would otherwise
    ship the blocks unproven.
    """
    index = pathlib.Path("index.qmd")
    if not index.exists():
        print(f"{__file__}: no index.qmd in {pathlib.Path.cwd()}", file=sys.stderr)
        return 1

    text = index.read_text()
    braced, count = FENCE.subn(r"\1```{python}", text)
    if count == 0:
        print(f"{__file__}: no ```python fence in index.qmd", file=sys.stderr)
        return 1

    index.write_text(braced)
    print(f"{__file__}: braced {count} fences in index.qmd")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
