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
HEADING = re.compile(r"^(#+) ")
EXAMPLES = re.compile(r"^#+ Examples \{\.doc-examples\}")

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
    """Add braces to the fences under each `Examples` heading of a reference page, at any depth.

    A class page has an `## Examples` section and a `#### Examples` section per method, with a
    signature block between them, so this walks the headings instead of splitting the page once.

    Returns
    -------
    The number of pages with a plain fence left under an `Examples` heading, where the example would not run.
    """
    missed = 0
    for page in sorted(REFERENCE.glob("*.qmd")):
        lines = read(page).split("\n")
        depth = 0  # the `Examples` heading's level while inside its section, else 0
        count = 0
        for i, line in enumerate(lines):
            heading = HEADING.match(line)
            if heading:
                level = len(heading.group(1))
                if depth and level <= depth:
                    depth = 0
                if EXAMPLES.match(line):
                    depth = level
                continue
            if depth:
                lines[i], n = PLAIN.subn(r"\1```{python}", line)
                count += n
                if n == 0 and PYTHON.match(line) and not BRACED.match(line):
                    print(
                        f"{page}: a Python fence under an Examples heading has no braces"
                    )
                    missed += 1
        if missed:
            continue
        write(page, "\n".join(lines))
        if count:
            print(f"{page}: braced {count}")
    return missed


if __name__ == "__main__":
    raise SystemExit(min(landing_page() + reference_pages(), 1))
