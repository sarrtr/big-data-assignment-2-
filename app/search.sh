#!/bin/bash
set -euo pipefail

if [ $# -eq 0 ]; then
    echo "Usage: $0 'your query'"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "$SCRIPT_DIR/.venv/bin/activate"
export PYSPARK_DRIVER_PYTHON="$(which python)"
unset PYSPARK_PYTHON

spark-submit     --master yarn     --deploy-mode client     --archives "$SCRIPT_DIR/.venv.tar.gz#.venv"     --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python     --conf spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python     "$SCRIPT_DIR/query.py" "$*"
