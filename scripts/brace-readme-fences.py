# A build script Quarto runs, not package code: `scripts/` is deliberately not a package
# (INP001), and what this writes to stdout is its interface, because Quarto shows it in
# the build log (T201). A file-level exception rather than one in `ruff.toml`, so it stays
# attached to the one file that earns it.
# ruff: noqa: INP001, T201
"""Brace the generated landing page's Python fences, leaving `README.md` alone. ADR-0009.

Quarto's pre-render hook, run with the build directory as the working directory, so the
copy Great Docs generated from `README.md` is already there and the file on disk is never
touched.

The count is checked against `README.md` rather than against zero. A guard that only fires
when *every* fence was missed cannot see the likelier failure, one fence gaining an
attribute the pattern does not match, which would ship that block static on a green build.
Checking the total also makes a second pass a no-op instead of an error, which is what
`quarto preview` does on every file change.
"""

import pathlib
import re

# `[ \t]` rather than `\s`, which in MULTILINE mode backtracks across the newline that ends
# the fence line and swallows a following blank line.
PLAIN = re.compile(r"^([ \t]*)```python[ \t]*\r?$", re.MULTILINE)
BRACED = re.compile(r"^[ \t]*```\{python\}[ \t]*\r?$", re.MULTILINE)

INDEX = pathlib.Path("index.qmd")
README = pathlib.Path("../README.md")


def main() -> int:
    """Brace every Python fence in the generated landing page.

    Returns
    -------
    A process exit code. Non-zero leaves Quarto's render failed, and means the landing page
    ended with fewer executable cells than `README.md` has Python fences: either Great Docs
    changed how it generates one, or a fence is spelled in a way `PLAIN` does not match.
    """
    text = INDEX.read_text(encoding="utf-8")
    expected = len(PLAIN.findall(README.read_text(encoding="utf-8")))
    braced, count = PLAIN.subn(r"\1```{python}", text)

    total = len(BRACED.findall(braced))
    if total != expected:
        print(f"{__file__}: {total} executable cells, {expected} in README.md")
        return 1

    INDEX.write_text(braced, encoding="utf-8", newline="")
    print(f"{__file__}: braced {count} of {expected} fences in {INDEX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
