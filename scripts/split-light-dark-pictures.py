# ruff: noqa: INP001, T201
"""Replace each `<picture>` on the landing page with a light-mode and a dark-mode image.

A `<picture>` source follows the reader's system theme, which GitHub also uses, but the docs site has its own theme toggle.
The toggle sets the `quarto-light` or `quarto-dark` class, and Quarto hides `.dark-content` or `.light-content` to match.
"""

import pathlib
import re

PICTURE = re.compile(
    r'<picture><source media="\(prefers-color-scheme: dark\)" srcset="(?P<dark>[^"]+)"\s*/?>'
    r'<img src="(?P<light>[^"]+)"(?P<attrs>[^>]*?)\s*/?></picture>'
)
LIGHT = 'class="light-content"'

INDEX = pathlib.Path("index.qmd")
README = pathlib.Path("../README.md")


def images(match: re.Match[str]) -> str:
    """Return the light-mode and the dark-mode image for one `<picture>`."""
    attrs = match["attrs"]
    return (
        f'<img src="{match["light"]}"{attrs} {LIGHT}>'
        f'<img src="{match["dark"]}"{attrs} class="dark-content">'
    )


if __name__ == "__main__":
    split, count = PICTURE.subn(images, INDEX.read_text(encoding="utf-8"))
    expected = len(PICTURE.findall(README.read_text(encoding="utf-8")))
    # Counting light images, not replacements, lets a second run over the same page succeed.
    total = split.count(LIGHT)
    if total != expected:
        print(f"{INDEX}: {total} light images, {expected} pictures in README.md")
        raise SystemExit(1)
    INDEX.write_text(split, encoding="utf-8", newline="")
    print(f"{INDEX}: split {count} of {expected}")
