#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/.runtime"

show_status() {
  local name="$1"
  local pid_file="$RUNTIME_DIR/$name.pid"

  if [[ ! -f "$pid_file" ]]; then
    echo "$name: stopped (no pid file)"
    return
  fi

  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -z "${pid:-}" ]]; then
    echo "$name: stopped (empty pid file)"
    return
  fi

  if kill -0 "$pid" 2>/dev/null; then
    echo "$name: running (pid $pid)"
  else
    echo "$name: stopped (stale pid $pid)"
  fi
}

show_status "worker"
show_status "bot"
