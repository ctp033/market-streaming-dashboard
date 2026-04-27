#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"
STREAMLIT_BIN="${STREAMLIT_BIN:-.venv/bin/streamlit}"
STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"
STREAMLIT_HOST="${STREAMLIT_HOST:-localhost}"

PIDS=()

port_in_use() {
  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"$STREAMLIT_PORT" -sTCP:LISTEN >/dev/null 2>&1
    return
  fi

  nc -z "$STREAMLIT_HOST" "$STREAMLIT_PORT" >/dev/null 2>&1
}

cleanup() {
  echo
  echo "Stopping real market-data pipeline..."

  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done

  wait >/dev/null 2>&1 || true
}

trap cleanup EXIT INT TERM

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Python runtime not found at $PYTHON_BIN"
  echo "Create the venv and install requirements first:"
  echo "  python3 -m venv .venv"
  echo "  .venv/bin/pip install -r requirements.txt"
  exit 1
fi

if [[ ! -x "$STREAMLIT_BIN" ]]; then
  echo "Streamlit not found at $STREAMLIT_BIN"
  echo "Install requirements first:"
  echo "  .venv/bin/pip install -r requirements.txt"
  exit 1
fi

if [[ -z "${FINNHUB_API_KEY:-}" ]]; then
  if [[ ! -f .env ]] || ! grep -q '^FINNHUB_API_KEY=.' .env; then
    echo "Missing FINNHUB_API_KEY."
    echo "Add FINNHUB_API_KEY to .env or export it before running this script."
    exit 1
  fi
fi

echo "Starting Kafka..."
docker compose up -d kafka

echo "Starting feature engine..."
"$PYTHON_BIN" -m src.features.feature_engine &
PIDS+=("$!")

echo "Starting Finnhub real-time trade producer..."
"$PYTHON_BIN" src/producers/finnhub_market_producer.py &
PIDS+=("$!")

if port_in_use; then
  echo "Streamlit port $STREAMLIT_PORT is already in use."
  echo "Using the existing dashboard at http://$STREAMLIT_HOST:$STREAMLIT_PORT"
else
  echo "Starting Streamlit dashboard..."
  "$STREAMLIT_BIN" run dashboards/streamlit_app.py \
    --server.port "$STREAMLIT_PORT" \
    --server.address "$STREAMLIT_HOST" &
  PIDS+=("$!")
fi

echo
echo "Real pipeline is running."
echo "Dashboard: http://$STREAMLIT_HOST:$STREAMLIT_PORT"
echo "Select 'Real pipeline' in the dashboard sidebar."
echo "Press Ctrl+C here to stop the Finnhub producer, feature engine, and dashboard."

wait
