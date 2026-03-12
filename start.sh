#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
HOST="${TRAIN_WATCH_HOST:-127.0.0.1}"
PORT="${TRAIN_WATCH_PORT:-8420}"
CONFIG_PATH="${TRAIN_WATCH_CONFIG:-config.empty.yaml}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "error: python3 not found. Please install Python 3 first." >&2
  exit 1
fi

if [ ! -d ".venv" ]; then
  echo "> creating virtualenv..."
  "$PYTHON_BIN" -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

if ! python -c 'import fastapi, uvicorn, paramiko, yaml, websockets' >/dev/null 2>&1; then
  echo "> installing dependencies..."
  python -m pip install -r requirements.txt
fi

echo "> starting Train Watch on http://${HOST}:${PORT}"
echo "> config: ${CONFIG_PATH}"
exec python run.py --config "$CONFIG_PATH" --host "$HOST" --port "$PORT"
