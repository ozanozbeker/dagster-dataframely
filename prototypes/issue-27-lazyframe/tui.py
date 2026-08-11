# ruff: noqa: C901, D101, D102, D103, FURB110, I001, INP001, PERF401, PLW1510, RET508, S603, T201
"""PROTOTYPE for issue #27. The terminal shell around logic.py.

Throwaway by design: raw-mode keys, ANSI paint, nothing beyond the project's
own dependencies. Every measurement runs in a child process via runner.py;
that file says why. This file holds no logic worth lifting.
"""

import json
import subprocess
import sys
import tempfile
import termios
import tty
from dataclasses import dataclass, field
from pathlib import Path

import dataframely as dy
import polars as pl

# pyrefly: ignore [missing-import]
import logic

HERE = Path(__file__).resolve().parent
RUNNER = HERE / "runner.py"
ROOT = Path(tempfile.gettempdir()) / "PROTOTYPE-issue27-wipe-me"

BOLD = "\x1b[1m"
DIM = "\x1b[2m"
RESET = "\x1b[0m"
CLEAR = "\x1b[2J\x1b[H"

STRATEGY_KEYS = {
    "0": "eager",
    "1": "naive",
    "2": "collect_all",
    "3": "temp_land",
    "4": "sink_only",
}


@dataclass
class State:
    size_i: int = 0
    fail_i: int = 2
    up_i: int = 0
    schema_i: int = 0
    # pyrefly: ignore [implicit-any-type-argument]
    runs: list[dict] = field(default_factory=list)
    panel: list[str] = field(default_factory=list)
    note: str = "no runs yet. [a] races all five strategies at the current config"

    @property
    def rows(self) -> int:
        return logic.SIZES[self.size_i]

    @property
    def fail_pct(self) -> float:
        return logic.FAIL_PCTS[self.fail_i]

    @property
    def upstream(self) -> str:
        return logic.UPSTREAMS[self.up_i]

    @property
    def schema(self) -> str:
        return list(logic.SCHEMAS)[self.schema_i]


def render(state: State) -> str:
    out = [CLEAR.rstrip("\n")]
    out.append(
        # pyrefly: ignore [bad-argument-type]
        f"{BOLD}issue #27 lazy strategy workbench{RESET}   "
        f"{DIM}PROTOTYPE, wipe {ROOT}{RESET}"
    )
    out.append(
        # pyrefly: ignore [bad-argument-type]
        f"config: rows {BOLD}{state.rows:,}{RESET}   fail {BOLD}{state.fail_pct}%{RESET}   "
        f"upstream {BOLD}{state.upstream}{RESET}   schema {BOLD}{state.schema}{RESET}   "
        f"{DIM}polars {pl.__version__}  dataframely {dy.__version__}{RESET}"
    )
    # pyrefly: ignore [bad-argument-type]
    out.append(f"{DIM}note:{RESET} {state.note}")
    out.append("")

    header = (
        f"{'strategy':<12}{'rows':>11}{'fail%':>7}{'up':>7}{'schema':>8}"
        f"{'total s':>9}{'peak MB':>9}{'base MB':>9}"
        f"{'good':>11}{'quar':>10}{'rc':>8}{'ok':>4}"
    )
    out.append(f"{DIM}{header}{RESET}")
    if not state.runs:
        out.append(f"{DIM}(empty){RESET}")
    for m in state.runs[-12:]:
        delta = round(m["rss_peak_mb"] - m["rss_base_mb"], 1)
        ok = "y" if m["consistent"] else "NO"
        out.append(
            # pyrefly: ignore [bad-argument-type]
            f"{m['strategy']:<12}{m['rows']:>11,}{m['fail_pct']:>7}{m['upstream']:>7}"
            f"{m.get('schema', 'pk'):>8}"
            f"{m['total_s']:>9.2f}{delta:>9.1f}{m['rss_base_mb']:>9.1f}"
            f"{m['good_rows']:>11,}{m['quar_rows']:>10,}{m['row_count_via']:>8}{ok:>4}"
        )
    out.append("")

    for line in state.panel[:9]:
        # pyrefly: ignore [bad-argument-type]
        out.append(f"  {line}")
    if state.panel:
        out.append("")

    out.append(
        f"{BOLD}[s]{RESET}{DIM}ize{RESET} {BOLD}[f]{RESET}{DIM}ail%{RESET} "
        f"{BOLD}[u]{RESET}{DIM}pstream{RESET} {BOLD}[k]{RESET}{DIM}ey on/off{RESET}   run: "
        f"{BOLD}[0]{RESET}{DIM}eager{RESET} {BOLD}[1]{RESET}{DIM}naive{RESET} "
        f"{BOLD}[2]{RESET}{DIM}collect_all{RESET} {BOLD}[3]{RESET}{DIM}temp_land{RESET} "
        f"{BOLD}[4]{RESET}{DIM}sink_only{RESET} {BOLD}[a]{RESET}{DIM}ll{RESET}   "
        f"{BOLD}[r]{RESET}{DIM}ead demo{RESET} {BOLD}[w]{RESET}{DIM}art demo{RESET} "
        f"{BOLD}[x]{RESET}{DIM} clear{RESET} {BOLD}[q]{RESET}{DIM}uit{RESET}"
    )
    return "\n".join(out)


def paint(state: State) -> None:
    print(render(state), flush=True)


def read_key() -> str:
    if not sys.stdin.isatty():
        ch = sys.stdin.read(1)
        return ch if ch else "q"  # EOF quits, so a piped smoke test terminates
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
    return ch


# pyrefly: ignore [implicit-any-type-argument]
def detail_lines(m: dict) -> list[str]:
    phases = "  ".join(
        f"{name} {seconds:.2f}s" for name, seconds in m["phases"].items()
    )
    counts = (
        "  ".join(f"{name}={count:,}" for name, count in m["counts"].items()) or "none"
    )
    landed = f"  landed {m['land_bytes']:,} (temp, deleted)" if m["land_bytes"] else ""
    return [
        f"{BOLD}{m['strategy']}{RESET}  {phases}",
        f"rule counts: {counts}",
        f"bytes: good {m['good_bytes']:,}  quarantine {m['quar_bytes']:,}{landed}",
        f"row_count via {m['row_count_via']}; good+quar == rows: {'yes' if m['consistent'] else 'NO'}",
    ]


def launch(state: State, strategy: str) -> None:
    src = logic.source_path(ROOT, state.rows, state.fail_pct)
    if not src.exists():
        state.note = f"generating {src.name} ..."
        paint(state)
        logic.ensure_source(ROOT, state.rows, state.fail_pct)
    state.note = (
        f"running {strategy} at {state.rows:,} rows, {state.upstream} upstream ..."
    )
    paint(state)
    cfg = {
        "strategy": strategy,
        "rows": state.rows,
        "fail_pct": state.fail_pct,
        "upstream": state.upstream,
        "schema": state.schema,
        "source": str(src),
        "work_dir": str(ROOT / "work"),
    }
    proc = subprocess.run(
        [sys.executable, str(RUNNER), json.dumps(cfg)],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        tail = (
            proc.stderr.strip().splitlines()[-1] if proc.stderr.strip() else "no stderr"
        )
        state.note = f"{strategy} failed: {tail}"
        return
    m = json.loads(proc.stdout)
    state.runs.append(m)
    state.panel = detail_lines(m)
    state.note = f"{strategy} done in {m['total_s']:.2f}s"


def main() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    state = State()
    while True:
        paint(state)
        key = read_key()
        if key in ("q", "\x03", "\x04"):
            break
        elif key == "s":
            state.size_i = (state.size_i + 1) % len(logic.SIZES)
        elif key == "f":
            state.fail_i = (state.fail_i + 1) % len(logic.FAIL_PCTS)
        elif key == "u":
            state.up_i = (state.up_i + 1) % len(logic.UPSTREAMS)
        elif key == "k":
            state.schema_i = (state.schema_i + 1) % len(logic.SCHEMAS)
        elif key in STRATEGY_KEYS:
            launch(state, STRATEGY_KEYS[key])
        elif key == "a":
            for strategy in STRATEGY_KEYS.values():
                launch(state, strategy)
        elif key == "r":
            good = ROOT / "work" / "good.parquet"
            state.panel = (
                logic.read_demo(good)
                if good.exists()
                else ["run a strategy first: the demo scans its good output"]
            )
            state.note = "read demo"
        elif key == "w":
            state.panel = logic.wart_demo(ROOT)
            state.note = "missing-file wart, modeled on UPathIOManager's catch site"
        elif key == "x":
            state.runs.clear()
            state.panel.clear()
            state.note = "cleared"
    print(f"\nsources and outputs cached under {ROOT}; wipe whenever.")


if __name__ == "__main__":
    main()
