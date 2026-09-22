#!/bin/bash
# Reshoot every image in `assets/images/` from a fresh run of the pipeline.
#
# Starts `dg dev` on an instance of its own, materializes the pipeline the way the README walks
# through it, shoots each view with headless Chrome, and shuts it all down. It writes straight
# into `../assets/images/`, which Great Docs copies into the site verbatim.
#
# Headless Chrome rather than a browser you are looking at: a capture has to be 2x for the text
# to survive being scaled into a docs column, and every Dagster view below is reachable by URL.
#
# Coordinates are device pixels, so twice the CSS width passed to `--window-size`. Dagster's left
# nav ends at device x=480 at every width used here, which is the one number to re-measure if a
# Dagster release moves it.
set -euo pipefail
cd "$(dirname "$0")"

CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PORT=3000
BASE=http://localhost:$PORT
ASSETS="$BASE/assets"
OUT=../assets/images
NAV=480

[ -x "$CHROME" ] || { echo "Chrome not found at $CHROME" >&2; exit 1; }
if curl -sfo /dev/null "$BASE"; then
  echo "Something already serves $BASE. Stop it first." >&2
  exit 1
fi

# The demo and the library share one `.venv`, and a plain `uv run` would sync it to one project
# and delete the other's packages: the webserver from under a running `dg dev`, or the Pillow the
# quantize step needs. So sync everything once, then run with `--no-sync`.
(cd .. && uv sync --all-packages --group docs --quiet)

# `dg dev` and every `dg launch` share one instance through DAGSTER_HOME, so the runs launched
# here are the ones the webserver shows. Left unset, `dg dev` builds a private one.
export DAGSTER_HOME
DAGSTER_HOME=$(mktemp -d)
# The first-run prompt covers every page a fresh browser opens, and a scripted reshoot is not
# usage worth reporting.
cat >"$DAGSTER_HOME/dagster.yaml" <<'YAML'
nux:
  enabled: false
telemetry:
  enabled: false
YAML
RAW=$(mktemp -d)
rm -rf storage
# Job control gives `dg dev` a process group of its own, so the trap can stop the webserver,
# daemon and code server it spawns. A signal to `uv run` alone leaves them running.
set -m
uv run --no-sync dg dev --port "$PORT" >"$RAW/dg.log" 2>&1 &
DG=$!
set +m
# `|| true` because the group is usually gone by the KILL, and under `set -e` that failed kill
# would become the script's exit status.
trap 'kill -TERM -- -"$DG" 2>/dev/null || true; sleep 3; kill -KILL -- -"$DG" 2>/dev/null || true; rm -rf "$DAGSTER_HOME" "$RAW"' EXIT
until curl -sfo /dev/null "$BASE/server_info"; do sleep 1; done

# Three of the silver loads fail by design, so a failed run is not an error here. In process,
# because the default executor spawns a Python per step, which made each launch take ~30s.
launch() {
  uv run --no-sync dg launch --config-json '{"execution":{"config":{"in_process":{}}}}' "$@" \
    >>"$RAW/launch.log" 2>&1 || true
}

echo "Materializing:"
launch --assets "group:bronze"
launch --assets "key:orders,key:customers,key:marketing_orders,key:warehouse_orders,key:high_value_orders,key:orders_snapshot"
for day in 01 02 03 04 05; do
  launch --assets "key:daily_orders" --partition "2026-08-$day"
  for region in apac eu us; do
    launch --assets "key:regional_orders" --partition "2026-08-$day|$region"
  done
done
launch --assets "key:weekly_orders"

latest_failure() {
  curl -sf "$BASE/graphql" -H 'content-type: application/json' \
    --data '{"query":"{runsOrError(filter:{statuses:[FAILURE]},limit:1){... on Runs{results{runId}}}}"}' \
    | sed -n 's/.*"runId":"\([^"]*\)".*/\1/p'
}
# One failing asset per run. A run's log opens scrolled to its failure, and with one failure
# per run that position is fixed, which is what lets the two error bands be constants.
launch --assets "key:partner_orders"
PARTNER_RUN=$(latest_failure)
launch --assets "key:finance_orders"
FINANCE_RUN=$(latest_failure)
launch --assets "key:legacy_orders"

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
shoot quarantine-checks        "$ASSETS/marketing_orders?view=checks"             1600 1000    0 2000
shoot columns-tab              "$ASSETS/orders"                                    1600 1400    0 2560
shoot check-list               "$ASSETS/orders?view=checks"                        1600 1400    0 2000
shoot severity-error           "$ASSETS/finance_orders?view=checks"                1600 1000    0 2000
shoot quarantine-lineage       "$ASSETS/marketing_orders?view=lineage"             1600  900    0 1700
shoot split-checks             "$ASSETS/warehouse_orders?view=checks"              1600 1000    0 1500
shoot partitions-grid          "$ASSETS/regional_orders?view=partitions&partition=2026-08-01%7Cus" 1600 900 0 1700
shoot materialization-metadata "$ASSETS/orders?view=events"                        2100 1500    0 2900
shoot returned-result-metadata "$ASSETS/orders_snapshot?view=events"               2100 1200    0 1750
# The invalid-row keys sit below five statistics tables, so this one is cropped to the band
# rather than shot from the top.
shoot invalid-by-rules         "$ASSETS/marketing_orders?view=events"              2100 2050 2660  805

# The two error messages come off the failed runs rather than an asset page: a check's own
# page never shows the raised error. Each band ends above `Stack Trace:`, whose frames carry
# the local filesystem paths of whoever ran this.
shoot error-column-schema    "$BASE/runs/$PARTNER_RUN?logs=step%3Apartner_orders"  1700 1000 1258  416
shoot error-validation-abort "$BASE/runs/$FINANCE_RUN"                             1700 1000  945  216

# A flat dark UI screenshot holds far fewer than 256 colours, so an adaptive palette is
# visually lossless and takes each file from roughly 500KB to under 200KB. That matters:
# prek's check-added-large-files hook refuses anything over 500KB.
echo "Quantizing:"
uv run --no-sync python - "$OUT" <<'PY'
import pathlib
import sys

from PIL import Image

for path in sorted(pathlib.Path(sys.argv[1]).glob("*.png")):
    before = path.stat().st_size
    image = Image.open(path).convert("RGB")
    image.quantize(colors=256, method=Image.Quantize.MEDIANCUT).save(path, optimize=True)
    print(f"  {path.name:34} {before // 1024:>4}KB -> {path.stat().st_size // 1024:>4}KB")
PY
