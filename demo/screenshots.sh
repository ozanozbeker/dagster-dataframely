#!/bin/bash
# Reshoot every image in `assets/images/` against a running `dg dev`.
#
# Run `uv run dg dev` in this directory, materialize the five selections the README lists,
# then run this from here. It writes straight into `../assets/images/`, which Great Docs
# copies into the site verbatim, so nothing has to be listed under Quarto's `resources:`.
#
# Headless Chrome rather than the browser in an editor pane: the pane caps a capture at its
# own pixel size, and `--force-device-scale-factor=2` is what makes the text hold up when
# the image is scaled to a docs column. Every Dagster view below is reachable by URL, so
# nothing here has to drive a click.
#
# Coordinates are device pixels, so twice the CSS width passed to `--window-size`. Dagster's
# left nav ends at device x=480 at every width used here, which is the one number to
# re-measure if a Dagster release moves it.
set -euo pipefail
cd "$(dirname "$0")"

CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
BASE=http://localhost:3000
ASSETS="$BASE/assets"
OUT=../assets/images
NAV=480
RAW=$(mktemp -d)
trap 'rm -rf "$RAW"' EXIT

[ -x "$CHROME" ] || { echo "Chrome not found at $CHROME" >&2; exit 1; }
curl -sfo /dev/null "$BASE" || { echo "Nothing serving $BASE. Start \`uv run dg dev\` first." >&2; exit 1; }

# name url css_width css_height crop_top crop_height
shoot() {
  "$CHROME" --headless=new --disable-gpu --hide-scrollbars \
    --force-device-scale-factor=2 --window-size="$3,$4" \
    --virtual-time-budget=25000 --screenshot="$RAW/$1.png" "$2" 2>/dev/null
  sips --cropOffset "$5" "$NAV" -c "$6" "$(( $3 * 2 - NAV ))" \
    "$RAW/$1.png" --out "$OUT/$1.png" >/dev/null
  echo "  $1"
}

echo "Capturing:"
shoot quarantine-checks        "$ASSETS/quarantined_orders?view=checks"          1600 1000    0 2000
shoot columns-tab              "$ASSETS/orders"                                  1600 1400    0 2560
shoot check-list               "$ASSETS/orders?view=checks"                      1600 1400    0 2000
shoot severity-error           "$ASSETS/strict_orders?view=checks"               1600 1000    0 2000
shoot quarantine-lineage       "$ASSETS/quarantined_orders?view=lineage"         1600  900    0 1700
shoot split-checks             "$ASSETS/unfiltered_orders?view=checks"           1600 1000    0 1500
shoot partitions-grid          "$ASSETS/regional_orders?view=partitions&partition=2026-08-05%7Cus" 1600 900 0 1700
shoot materialization-metadata "$ASSETS/orders?view=events"                      2100 1500    0 2900
shoot returned-result-metadata "$ASSETS/annotated_orders?view=events"            2100 1200    0 1750
# The invalid-row keys sit below five statistics tables, so this one is cropped to the band
# rather than shot from the top.
shoot invalid-by-rules         "$ASSETS/quarantined_orders?view=events"          2100 2050 2480  900

# The two error messages come off the failed run rather than an asset page: a check's own
# page never shows the raised error, and the band below leaves the stack trace's local
# filesystem paths out of frame. The run id changes every session, so it is looked up.
RUN=$(curl -sf "$BASE/graphql" -H 'content-type: application/json' \
  --data '{"query":"{runsOrError(filter:{statuses:[FAILURE]},limit:1){... on Runs{results{runId}}}}"}' \
  | sed -n 's/.*"runId":"\([^"]*\)".*/\1/p')
if [ -n "$RUN" ]; then
  shoot error-column-schema    "$BASE/runs/$RUN?logs=step%3Amistyped_orders"      1700 1000 1240  450
  # Unfiltered, unlike the one above: `strict_orders` is the last step to fail, so the
  # whole-run log is already scrolled to its message, and filtering to that step scrolls
  # somewhere else.
  shoot error-validation-abort "$BASE/runs/$RUN"                                  1700 1000  980  240
else
  echo "  no failed run found; materialize \`group:\"failure/*\"\` and rerun for the error images" >&2
fi

# A flat dark UI screenshot holds far fewer than 256 colours, so an adaptive palette is
# visually lossless and takes each file from roughly 500KB to under 200KB. That matters:
# prek's check-added-large-files hook refuses anything over 500KB.
echo "Quantizing:"
uv run --project .. --group docs python - "$OUT" <<'PY'
import pathlib
import sys

from PIL import Image

for path in sorted(pathlib.Path(sys.argv[1]).glob("*.png")):
    before = path.stat().st_size
    image = Image.open(path).convert("RGB")
    image.quantize(colors=256, method=Image.Quantize.MEDIANCUT).save(path, optimize=True)
    print(f"  {path.name:34} {before // 1024:>4}KB -> {path.stat().st_size // 1024:>4}KB")
PY
