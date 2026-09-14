# A build script Quarto runs, not package code: `scripts/` is deliberately not a package
# (INP001), and what this writes to stdout is its interface, because Quarto shows it in
# the build log (T201). A file-level exception rather than one in `ruff.toml`, so it stays
# attached to the one file that earns it.
# ruff: noqa: INP001, T201
"""Brace the generated pages' Python fences, leaving every source file plain. ADR-0009.

Quarto's pre-render hook, run with the build directory as the working directory, so the
pages Great Docs generated are already there and nothing on disk outside it is touched.

Two surfaces, one reason. `README.md` is also PyPI's long description and GitHub's front
page. A docstring is read by ruff, whose `docstring-code-format` reaches the plain spelling
only. Both keep plain fences and get braces here instead.

Only the reference pages' `Examples` sections are rewritten, never the whole page: Great
Docs emits the function signature as its own ` ```python ` block, and executing a signature
would fail the build.
"""

import pathlib
import re

# `[ \t]` rather than `\s`, which in MULTILINE mode backtracks across the newline that ends
# the fence line and swallows a following blank line.
PLAIN = re.compile(r"^([ \t]*)```python[ \t]*\r?$", re.MULTILINE)
BRACED = re.compile(r"^[ \t]*```\{python\}[ \t]*\r?$", re.MULTILINE)
EXAMPLES = "## Examples {.doc-examples}"

INDEX = pathlib.Path("index.qmd")
README = pathlib.Path("../README.md")
REFERENCE = pathlib.Path("reference")


def read(path: pathlib.Path) -> str:
    """Read a generated page as UTF-8, whatever the process locale says."""
    return path.read_text(encoding="utf-8")


def write(path: pathlib.Path, text: str) -> None:
    """Write a generated page as UTF-8, without translating its line endings."""
    path.write_text(text, encoding="utf-8", newline="")


def landing_page() -> int:
    """Brace the landing page, and report how many fences did not survive the trip.

    Returns
    -------
    The count missing against `README.md`. Checking the total rather than checking for zero
    is what catches the likelier failure, one fence gaining an attribute the pattern does
    not match, and what makes a second pass a no-op instead of an error.
    """
    braced, count = PLAIN.subn(r"\1```{python}", read(INDEX))
    expected = len(PLAIN.findall(read(README)))
    total = len(BRACED.findall(braced))
    if total != expected:
        print(f"{INDEX}: {total} executable cells, {expected} fences in README.md")
        return expected - total
    write(INDEX, braced)
    print(f"{INDEX}: braced {count} of {expected}")
    return 0


def reference_pages() -> int:
    """Brace each reference page's `Examples`, leaving its signature block alone.

    Returns
    -------
    The count of plain fences still sitting under an `Examples` heading, which means the
    pattern did not match one and that example would ship static.
    """
    missed = 0
    for page in sorted(REFERENCE.glob("*.qmd")):
        head, heading, tail = read(page).partition(EXAMPLES)
        if not heading:
            continue
        braced, count = PLAIN.subn(r"\1```{python}", tail)
        if PLAIN.search(braced):
            print(f"{page}: a fence under {EXAMPLES} is still plain")
            missed += 1
            continue
        write(page, head + heading + braced)
        if count:
            print(f"{page}: braced {count}")
    return missed


if __name__ == "__main__":
    raise SystemExit(min(landing_page() + reference_pages(), 1))
