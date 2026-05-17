#!/usr/bin/env bash
# One-click launcher for UTA + heatmap-sdk (Linux / macOS).
#
# Run with: bash launcher/start.sh   (or chmod +x and ./launcher/start.sh)
#
# On first run this creates a virtualenv, extracts heatmap-sdk,
# applies the integration patch, installs Python dependencies, and
# prompts for Binance API credentials. Subsequent runs skip the
# already-done steps and just launch the server.

set -euo pipefail

# cd into repo root regardless of where the script was invoked from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/.."

# Pick a Python interpreter. We require >=3.11; the Python script
# itself re-checks this and aborts with a clear message if not met.
# Test each candidate by actually running ``--version`` so we skip
# pyenv shims that point at uninstalled versions and silently fail.
PY=""
for candidate in python3.13 python3.12 python3.11 python3 python; do
    if "$candidate" --version >/dev/null 2>&1; then
        PY="$candidate"
        break
    fi
done

if [[ -z "$PY" ]]; then
    echo
    echo "[launcher] ERROR: Python 3.11+ not found on PATH."
    echo "Install it from https://www.python.org/downloads/ or via your"
    echo "package manager (e.g. 'sudo apt install python3.11'), then"
    echo "run this script again."
    echo
    exit 1
fi

exec "$PY" launcher/_launcher.py "$@"
