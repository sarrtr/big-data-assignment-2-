#!/bin/bash
if [ $# -eq 0 ]; then
    echo "Usage: $0 'your query'"
    exit 1
fi

spark-submit --master yarn --deploy-mode client \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
    query.py "$*"