# ruff: noqa: INP001, T201
"""Add braces to the Python fences in Great Docs' generated pages (ADR-0009).

Quarto runs this before rendering, in the build directory, so it changes no source file.
In a reference page it changes only the `Examples` section, because Great Docs writes the signature as a ` ```python ` block too, and running a signature fails the build.
"""

import pathlib
import re

# `[ \t]`, not `\s`: in MULTILINE mode `\s*` also matches the newline after the fence line and
# a blank line after it, which the substitution would then delete.
PLAIN = re.compile(r"^([ \t]*)```python[ \t]*\r?$", re.MULTILINE)
BRACED = re.compile(r"^[ \t]*```\{python\}[ \t]*\r?$", re.MULTILINE)
# Any Python fence, including one with attributes that `PLAIN` does not match.
PYTHON = re.compile(r"^[ \t]*```[ \t]*\{?\.?(?:python|py)\b", re.MULTILINE)
EXAMPLES = "## Examples {.doc-examples}"

INDEX = pathlib.Path("index.qmd")
README = pathlib.Path("../README.md")
REFERENCE = pathlib.Path("reference")


def read(path: pathlib.Path) -> str:
    """Read a generated page as UTF-8, whatever the process locale is."""
    return path.read_text(encoding="utf-8")


def write(path: pathlib.Path, text: str) -> None:
    """Write a generated page as UTF-8, without translating its line endings."""
    path.write_text(text, encoding="utf-8", newline="")


def landing_page() -> int:
    """Add braces to the landing page's fences.

    Returns
    -------
    The number of `README.md` fences that are not executable cells on the page.
    Comparing totals, not checking that no plain fence remains, detects a fence that gained an attribute the pattern does not match, and lets a second run succeed.
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
    """Add braces to each reference page's `Examples` fences, not to its signature block.

    Returns
    -------
    The number of pages with a plain fence left under `Examples`, where the example would not run.
    """
    missed = 0
    for page in sorted(REFERENCE.glob("*.qmd")):
        head, heading, tail = read(page).partition(EXAMPLES)
        if not heading:
            continue
        braced, count = PLAIN.subn(r"\1```{python}", tail)
        if len(PYTHON.findall(braced)) != len(BRACED.findall(braced)):
            print(f"{page}: a Python fence under {EXAMPLES} has no braces")
            missed += 1
            continue
        write(page, head + heading + braced)
        if count:
            print(f"{page}: braced {count}")
    return missed


if __name__ == "__main__":
    raise SystemExit(min(landing_page() + reference_pages(), 1))
