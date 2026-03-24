#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_PATH="${1:-/input/data}"
OUTPUT_PATH="${2:-/indexer}"
HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop-3.3.6}
STREAMING_JAR="$(find "$HADOOP_HOME/share/hadoop/tools/lib" -name 'hadoop-streaming-*.jar' | head -n 1)"

if [[ -z "${STREAMING_JAR:-}" ]]; then
  echo "Cannot find hadoop-streaming jar" >&2
  exit 1
fi

hdfs dfs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true
hdfs dfs -rm -r -f /tmp/indexer >/dev/null 2>&1 || true
hdfs dfs -mkdir -p /tmp/indexer

hadoop jar "$STREAMING_JAR"   -D mapreduce.job.reduces=1   -files "$SCRIPT_DIR/mapreduce/mapper1.py,$SCRIPT_DIR/mapreduce/reducer1.py"   -mapper "python3 mapper1.py"   -reducer "python3 reducer1.py"   -input "$INPUT_PATH"   -output "$OUTPUT_PATH"
