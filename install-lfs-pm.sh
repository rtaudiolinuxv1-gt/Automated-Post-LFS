#!/bin/bash
set -e

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

cd "$SCRIPT_DIR"
python3 -m pip install --user -e .

echo
echo "Installed lfs-pm into the user environment."
echo "Run it with:"
echo "  lfs-pm --root \"$SCRIPT_DIR/filesystem_mountpoint\" tui"
