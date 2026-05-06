#!/usr/bin/env sh
# ============================================================================
#  Terminal launcher (Mac / Linux)
#  Created by Oscar Zarraga Perez. Copyright (c) 2026 Oscar Zarraga Perez.
#  Released under the MIT License.
# ============================================================================

set -e

# Move to the directory this script lives in, so server.py is found.
cd "$(dirname "$0")"

# Pick a python interpreter.
if command -v python3 >/dev/null 2>&1; then
  PY=python3
elif command -v python >/dev/null 2>&1; then
  PY=python
else
  echo "Python 3.9+ is required but was not found on PATH." >&2
  echo "Install Python from https://www.python.org/downloads/ and try again." >&2
  exit 1
fi

echo "Starting Terminal..."
echo "Created by Oscar Zarraga Perez. Copyright (c) 2026 Oscar Zarraga Perez."
echo "Once the server is up, the UI opens at http://127.0.0.1:8788"
echo

exec "$PY" server.py
