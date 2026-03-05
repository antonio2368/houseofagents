#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is required but was not found in PATH" >&2
  exit 1
fi

CONFIG_PATH="${1:-}"
FORCE_FLAG="${2:-}"

echo "Installing houseofagents..."
cargo install --path .

INIT_ARGS=("--init-config")
if [[ -n "$CONFIG_PATH" ]]; then
  INIT_ARGS+=("--config" "$CONFIG_PATH")
fi
if [[ "$FORCE_FLAG" == "--force" ]]; then
  INIT_ARGS+=("--force")
fi

echo "Initializing config..."
cargo run -- "${INIT_ARGS[@]}"

echo
echo "Install complete."
echo "Run: houseofagents"
if [[ -n "$CONFIG_PATH" ]]; then
  echo "Or:  houseofagents --config $CONFIG_PATH"
fi
echo
echo "Optional CLI mode tools:"
for bin in claude codex gemini; do
  if command -v "$bin" >/dev/null 2>&1; then
    echo "  - $bin: found"
  else
    echo "  - $bin: not found"
  fi
done
