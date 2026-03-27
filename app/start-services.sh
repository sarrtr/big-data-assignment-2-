#!/bin/bash
set -euo pipefail

HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop-3.3.6}
SPARK_JARS_DIR=/apps/spark/jars

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
mapred --daemon start historyserver || true

sleep 5

hdfs dfsadmin -safemode leave || true

hdfs dfs -mkdir -p "$SPARK_JARS_DIR"
hdfs dfs -mkdir -p /user/root
hdfs dfs -mkdir -p /tmp/indexer
hdfs dfs -mkdir -p /indexer
hdfs dfs -mkdir -p /input

for jar in /usr/local/spark/jars/*.jar; do
  hdfs dfs -put -f "$jar" "$SPARK_JARS_DIR/" >/dev/null 2>&1 || true
done

jps -lm || true
hdfs dfs -ls / || true
