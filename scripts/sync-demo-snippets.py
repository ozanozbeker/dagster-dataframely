# A repo tool prek runs, not package code: `scripts/` is deliberately not a package (INP001),
# and what this writes to stdout is its interface, because prek shows it (T201).
# ruff: noqa: INP001, T201
"""Copy each demo module into the markdown code block that names it.

A block opts in with `<!-- snippet: demo/src/.../module.py -->` on the line above its fence.
The README cannot include a file at render time, because GitHub and PyPI render it as plain
markdown, so the code is copied in and this keeps the copy true. The demo's tests then cover
what the README shows, and the values in its screenshots match the code beside them.

A module is copied whole, minus three things a reader has no use for: its docstring, its
imports from the demo package itself, and any `# demo:` comment. Whole modules rather than
named regions, so the demo carries no markers and reads like the pipeline it is.

Like a formatter, it rewrites what drifted and exits 1, so the commit that caused the drift
stops until the rewrite is staged with it.
"""

import ast
import pathlib
import re
import sys

MARKER = re.compile(r"^<!-- snippet: (\S+) -->$")
OPTION = re.compile(r"^#\| ")
PACKAGE = "dagster_dataframely_demo"


def demo_code(path: pathlib.Path) -> str:
    """Return a demo module's source as a reader should see it."""
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    dropped: set[int] = set()
    body = tree.body
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
    ):
        dropped.update(range(body[0].lineno - 1, body[0].end_lineno or body[0].lineno))
    for node in body:
        modules: list[str] = []
        if isinstance(node, ast.ImportFrom):
            modules = [node.module or ""]
        elif isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        if any(module.split(".")[0] == PACKAGE for module in modules):
            dropped.update(range(node.lineno - 1, node.end_lineno or node.lineno))
    kept = [
        line
        for number, line in enumerate(source.splitlines())
        if number not in dropped and not line.strip().startswith("# demo:")
    ]
    return re.sub(r"\n{4,}", "\n\n\n", "\n".join(kept)).strip("\n")


def synced(text: str, root: pathlib.Path) -> str:
    """Return `text` with every marked code block replaced by the module it names."""
    lines = text.split("\n")
    out: list[str] = []
    i = 0
    while i < len(lines):
        out.append(lines[i])
        marker = MARKER.match(lines[i])
        i += 1
        if not marker:
            continue
        opener = lines[i]
        out.append(opener)
        i += 1
        # Quarto cell options belong to the page, not the module, so they stay.
        while i < len(lines) and OPTION.match(lines[i]):
            out.append(lines[i])
            i += 1
        while i < len(lines) and not lines[i].startswith("```"):
            i += 1
        out.extend(demo_code(root / marker.group(1)).split("\n"))
        out.append(lines[i])
        i += 1
    return "\n".join(out)


def main() -> int:
    """Sync every opted-in block, and report the files that drifted.

    Returns
    -------
    A process exit code: 1 when any file was rewritten, so the hook stops the commit.
    """
    root = pathlib.Path(__file__).resolve().parent.parent
    drifted = 0
    for page in [root / "README.md", *sorted((root / "user_guide").glob("*.qmd"))]:
        text = page.read_text(encoding="utf-8")
        new = synced(text, root)
        if new != text:
            page.write_text(new, encoding="utf-8", newline="")
            print(f"synced {page.relative_to(root)}")
            drifted += 1
    return min(drifted, 1)


if __name__ == "__main__":
    sys.exit(main())
