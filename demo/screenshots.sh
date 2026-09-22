#!/bin/bash
# Regenerate every image in `../assets/images/` from a fresh run of the demo pipeline.
# Headless Chrome captures at 2x, so the text stays readable when a docs page scales the image down.
# Crop coordinates are device pixels, which are twice the CSS pixels passed to `--window-size`.
set -euo pipefail
cd "$(dirname "$0")"

CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PORT=3000
BASE=http://localhost:$PORT
ASSETS="$BASE/assets"
OUT=../assets/images
# Where Dagster's left nav ends at every width below. Re-measure it after a Dagster release that
# moves the nav.
NAV=480

[ -x "$CHROME" ] || { echo "Chrome not found at $CHROME" >&2; exit 1; }
if curl -sfo /dev/null "$BASE"; then
  echo "Something already serves $BASE. Stop it first." >&2
  exit 1
fi

# The demo and the library share one `.venv`, and a plain `uv run` would remove the other
# project's packages, such as the webserver or Pillow. So sync everything once and use `--no-sync`.
(cd .. && uv sync --all-packages --group docs --quiet)

# `dg dev` and `dg launch` share one instance through DAGSTER_HOME, so the webserver shows these
# runs. Without it, `dg dev` creates a temporary instance of its own.
export DAGSTER_HOME
DAGSTER_HOME=$(mktemp -d)
# The first-run dialog covers every page a fresh browser opens, and a scripted run is not usage
# worth reporting.
cat >"$DAGSTER_HOME/dagster.yaml" <<'YAML'
nux:
  enabled: false
telemetry:
  enabled: false
YAML
RAW=$(mktemp -d)
rm -rf storage
# `set -m` gives `dg dev` its own process group, so the trap can stop the webserver, daemon and
# code server it starts. A signal to `uv run` alone leaves them running.
set -m
uv run --no-sync dg dev --port "$PORT" >"$RAW/dg.log" 2>&1 &
DG=$!
set +m
# `|| true`, because the group has usually exited before the KILL, and under `set -e` that failed
# kill would become the script's exit status.
trap 'kill -TERM -- -"$DG" 2>/dev/null || true; sleep 3; kill -KILL -- -"$DG" 2>/dev/null || true; rm -rf "$DAGSTER_HOME" "$RAW"' EXIT
until curl -sfo /dev/null "$BASE/server_info"; do sleep 1; done

# Three silver assets fail on purpose, so a failed run is not an error here. `in_process`, because
# the default executor starts a Python process per step, which takes about 30s per launch.
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
# One failing asset per run: a run's log opens scrolled to its failure, so with one failure that
# position never changes and the two error crops can be constants.
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
# The invalid-row keys are below five statistics tables, so this crop starts partway down the page.
shoot invalid-by-rules         "$ASSETS/marketing_orders?view=events"              2100 2050 2660  805

# The error messages come from the failed runs, because a check's own page does not show the
# raised error. Each crop ends above `Stack Trace:`, whose stack frames show local file paths.
shoot error-column-schema    "$BASE/runs/$PARTNER_RUN?logs=step%3Apartner_orders"  1700 1000 1258  416
shoot error-validation-abort "$BASE/runs/$FINANCE_RUN"                             1700 1000  945  216

# The screenshots have far fewer than 256 colours, so a 256-colour palette loses nothing visible.
# It cuts each file from about 500KB to under 200KB, and check-added-large-files rejects a file
# over 500KB.
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
