#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PARQUET_PATH="${PARQUET_PATH:-$PROJECT_ROOT/a.parquet}"

source "$SCRIPT_DIR/.venv/bin/activate"
export PYSPARK_DRIVER_PYTHON="$(which python)"
unset PYSPARK_PYTHON

hdfs dfs -rm -r -f /data /input/data >/dev/null 2>&1 || true
rm -rf "$SCRIPT_DIR/data"

export PARQUET_PATH
python "$SCRIPT_DIR/prepare_data.py"

hdfs dfs -put -f "$SCRIPT_DIR/data" /
hdfs dfs -ls /data
hdfs dfs -ls /input/data

echo "done data preparation!"
