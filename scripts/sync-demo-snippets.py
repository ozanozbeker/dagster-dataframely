# `scripts/` is not a package (INP001), and prek shows what this prints (T201).
# ruff: noqa: INP001, T201
"""Copy each demo module into the markdown code block that names it.

GitHub and PyPI cannot include a file in the README, so the README holds copies of the
tested demo modules. This script copies whole modules, not marked regions, so the demo
modules need no markers.
"""

import ast
import pathlib
import re
import sys

MARKER = re.compile(r"^<!-- snippet: (\S+) -->$")
OPTION = re.compile(r"^#\| ")
PACKAGE = "dagster_dataframely_demo"


def demo_code(path: pathlib.Path) -> str:
    """Return a demo module's source without its docstring, demo-package imports and `# demo:` comments."""
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
        # Quarto cell options are part of the page, not the module, so this loop keeps them.
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
    """Rewrite every marked block that differs from its module, and print each file rewritten.

    Returns
    -------
    The exit code: 1 when this script rewrote a file, so prek fails the commit.
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
