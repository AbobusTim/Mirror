#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/.runtime"
VENV_ACTIVATE="$ROOT_DIR/venv/bin/activate"

mkdir -p "$RUNTIME_DIR"

if [[ ! -f "$VENV_ACTIVATE" ]]; then
  echo "Virtualenv not found at $VENV_ACTIVATE"
  exit 1
fi

start_process() {
  local name="$1"
  local cmd="$2"
  local pid_file="$RUNTIME_DIR/$name.pid"
  local log_file="$RUNTIME_DIR/$name.log"

  if [[ -f "$pid_file" ]]; then
    local existing_pid
    existing_pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "${existing_pid:-}" ]] && kill -0 "$existing_pid" 2>/dev/null; then
      echo "$name already running (pid $existing_pid)"
      return
    fi
  fi

  nohup bash -lc "cd \"$ROOT_DIR\" && source \"$VENV_ACTIVATE\" && exec $cmd" \
    >"$log_file" 2>&1 < /dev/null &
  local new_pid=$!
  echo "$new_pid" > "$pid_file"

  sleep 0.3
  if kill -0 "$new_pid" 2>/dev/null; then
    echo "Started $name (pid $new_pid)"
  else
    echo "Failed to start $name. Check $log_file"
    exit 1
  fi
}

start_process "worker" "python main.py"
start_process "bot" "python -m src.bot"

echo "Detached services are running. Logs: $RUNTIME_DIR"
