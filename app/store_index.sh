#!/bin/bash

spark-submit --master yarn --deploy-mode client \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
    store_index.py