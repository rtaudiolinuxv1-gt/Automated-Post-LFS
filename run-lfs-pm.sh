#!/bin/bash
set -e

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
ROOT_PATH="${1:-$SCRIPT_DIR/filesystem_mountpoint}"

shift $(( $# > 0 ? 1 : 0 ))

cd "$SCRIPT_DIR"
export PYTHONPATH="$SCRIPT_DIR/src${PYTHONPATH:+:$PYTHONPATH}"
exec python3 -m lfs_unified_pm.cli --root "$ROOT_PATH" tui "$@"
