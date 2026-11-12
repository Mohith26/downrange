#!/usr/bin/env bash
# Build and run the gateway with the console on http://127.0.0.1:8787
#   ./run.sh          live ISS feed (also records the session to recordings/)
#   ./run.sh replay   replay the bundled recorded session (add a speed, e.g. ./run.sh replay 10)
#   ./run.sh synth    synthetic load (benchmark data, not real telemetry)
# Stop it with: curl -X POST http://127.0.0.1:8787/shutdown
set -euo pipefail
cd "$(dirname "$0")"
MODE="${1:-iss}"
PORT="${PORT:-8787}"

(cd gateway && cargo build --release)
(cd console && npm install --no-audit --no-fund && npm run build)

cd gateway
case "$MODE" in
  iss)
    REC="../recordings/session-$(date -u +%Y%m%dT%H%M%SZ).rec"
    echo "Recording this session to $REC"
    exec ./target/release/downrange serve --source iss --port "$PORT" --static ../console/dist --record-to "$REC"
    ;;
  replay)
    exec ./target/release/downrange serve --source replay --speed "${2:-1}" --port "$PORT" --static ../console/dist
    ;;
  synth)
    exec ./target/release/downrange serve --source synth --synth-channels "${2:-64}" --synth-hz "${3:-100}" --port "$PORT" --static ../console/dist
    ;;
  *)
    echo "usage: ./run.sh [iss|replay [speed]|synth [channels] [hz]]" >&2
    exit 2
    ;;
esac
