#!/usr/bin/env bash
# Start the seven pipeline processes with a restart-on-exit loop each (oms_ems hosts OMS + EMS).
# Usage: source run.bash (sets ALPACA_* / REDIS_URL / ALPACA_DB_PATH), then scripts/run_all.sh
# Stop:  Ctrl-C (kills the whole process group) or scripts/run_all.sh stop
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GEN="$ROOT/generated"
LOGDIR="$ROOT/.logs"; mkdir -p "$LOGDIR"
PIDFILE="$LOGDIR/run_all.pids"

if [[ "${1:-}" == "stop" ]]; then
  [[ -f "$PIDFILE" ]] && xargs -r kill < "$PIDFILE" 2>/dev/null; rm -f "$PIDFILE"; echo stopped; exit 0
fi
: "${ALPACA_API_KEY_ID:?set ALPACA_API_KEY_ID (see run.bash.example)}"
: "${ALPACA_API_SECRET_KEY:?set ALPACA_API_SECRET_KEY}"
export REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"
export ALPACA_DB_PATH="${ALPACA_DB_PATH:-$ROOT/alpaca.db}"
export PYTHONUNBUFFERED=1

supervise() {  # supervise <name> -- restart if the process ever exits (runtime.run_forever already restarts internally)
  local name="$1"
  while true; do
    echo "[run_all] starting $name" >&2
    (cd "$GEN" && python "$name.py" ${SYMBOLS:-}) >>"$LOGDIR/$name.log" 2>&1
    echo "[run_all] $name exited ($?); restarting in 5s" >&2; sleep 5
  done
}
: > "$PIDFILE"
for w in subscriber trade_stream oms_ems crossing algo_engine persister webgui; do
  supervise "$w" & echo $! >> "$PIDFILE"
done
echo "[run_all] workers started; logs in $LOGDIR; UI on http://${WEBGUI_HOST:-127.0.0.1}:${WEBGUI_PORT:-5000}"
trap 'xargs -r kill < "$PIDFILE" 2>/dev/null; pkill -P $$ 2>/dev/null; exit 0' INT TERM
wait
