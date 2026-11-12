#!/usr/bin/env bash
# Runs every measurement and writes results/results.json.
# The browser render step needs a real browser: when it starts, open the printed URL
# and keep the tab visible for ~15 s. The page posts its numbers back to the gateway.
set -euo pipefail
cd "$(dirname "$0")/.."
RAW=results/raw
mkdir -p "$RAW"
BIN=gateway/target/release/downrange
(cd gateway && cargo build --release)
(cd console && npm run build)

echo "== recorded ISS session"
(cd gateway && ./target/release/downrange bench analyze --rec ../recordings/iss-session.rec --out ../$RAW/iss_session.json)

echo "== fan-out throughput (synthetic load)"
(cd gateway && ./target/release/downrange bench throughput --clients 4 --secs 8 \
  --points 1000x100,2000x250,4000x500,8000x500,8000x1000,10000x1000,12000x1000 --out ../$RAW/throughput.json)

echo "== stalled client"
(cd gateway && ./target/release/downrange bench slow --channels 1000 --hz 100 --secs 20 --cap 20000 --out ../$RAW/slow_client.json)

echo "== command faults"
(cd gateway && ./target/release/downrange bench commands --n 1000 --seed 7 --out ../$RAW/commands.json)

if [ "${SKIP_BROWSER:-0}" != "1" ]; then
  for spec in ${RENDER_SPECS:-64x100 128x50}; do
    lanes="${spec%x*}"; hz="${spec#*x}"
    port=$((8800 + lanes))
    rm -f "$RAW/browser_render.json"
    (cd gateway && ./target/release/downrange serve --source synth --synth-channels "$lanes" --synth-hz "$hz" \
      --port "$port" --static ../console/dist --results ../$RAW --audit /dev/null) &
    sleep 1
    echo "Open http://127.0.0.1:$port/?bench=1&lanes=$lanes&secs=10&window=30 in a browser tab and leave it visible"
    for _ in $(seq 1 180); do
      [ -f "$RAW/browser_render.json" ] && break
      sleep 1
    done
    curl -s -X POST "http://127.0.0.1:$port/shutdown" >/dev/null || true
    if [ -f "$RAW/browser_render.json" ]; then
      mv "$RAW/browser_render.json" "$RAW/browser_render_${lanes}ch_${hz}hz.json"
    else
      echo "no browser result for $spec"
    fi
    sleep 1
  done
fi

python3 scripts/collect_results.py
