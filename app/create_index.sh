#!/bin/bash

INPUT_PATH=${1:-/input/data}
OUTPUT_PATH=/indexer

hdfs dfs -rm -r -f $OUTPUT_PATH

hadoop jar /opt/hadoop-3.3.6/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -D mapreduce.job.reduces=1 \
    -input "$INPUT_PATH" \
    -output "$OUTPUT_PATH" \
    -mapper mapper1.py \
    -reducer reducer1.py \
    -file mapper1.py \
    -file reducer1.py
