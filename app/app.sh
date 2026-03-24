#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

service ssh restart >/dev/null 2>&1 || true

bash "$SCRIPT_DIR/start-services.sh"

python3 -m venv .venv
source .venv/bin/activate
pip install --no-cache-dir -r requirements.txt
venv-pack -o .venv.tar.gz

bash "$SCRIPT_DIR/prepare_data.sh"
bash "$SCRIPT_DIR/index.sh"
bash "$SCRIPT_DIR/search.sh" "this is a query"
