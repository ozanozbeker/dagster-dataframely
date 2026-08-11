# ruff: noqa: D103, INP001, T201
"""PROTOTYPE for issue #27. Child-process entry: one measurement, JSON out.

A fresh process per measurement because `ru_maxrss` is a high-water mark for
the whole process. In one long-lived process the first `collect_all` at ten
million rows would set a ceiling every later run inherits, and the memory
column would compare nothing. The baseline is captured after imports, so the
delta the TUI shows is the strategy's own working set.
"""

import json
import resource
import sys

# pyrefly: ignore [missing-import]
import logic


def _rss_mb() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # ru_maxrss is bytes on macOS and kilobytes on Linux.
    divisor = 1_000_000 if sys.platform == "darwin" else 1_000
    return round(peak / divisor, 1)


def main() -> None:
    cfg = json.loads(sys.argv[1])
    base = _rss_mb()
    result = logic.run(cfg)
    result["rss_base_mb"] = base
    result["rss_peak_mb"] = _rss_mb()
    # stdout carries exactly one JSON document; everything else goes to stderr.
    print(json.dumps(result))


if __name__ == "__main__":
    main()
