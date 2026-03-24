#!/usr/bin/env python
import sys
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, log

def bm25_score(tf, doc_len, df, N, avgdl, k1=1.2, b=0.75):
    idf = math.log((N - df + 0.5) / (df + 0.5) + 1)
    numerator = (k1 + 1) * tf
    denominator = k1 * (1 - b + b * (doc_len / avgdl)) + tf
    return idf * (numerator / denominator)

def main(query):
    spark = SparkSession.builder \
        .appName("BM25Ranker") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .getOrCreate()
    sc = spark.sparkContext

    doc_lengths = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="doc_lengths", keyspace="search_engine") \
        .load()
    term_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="term_df", keyspace="search_engine") \
        .load()
    term_index = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="term_index", keyspace="search_engine") \
        .load()

    N = doc_lengths.count()
    avgdl = doc_lengths.selectExpr("avg(length)").collect()[0][0]

    query_terms = [term.lower() for term in query.split() if term.isalpha()]
    if not query_terms:
        print("No valid terms in query.")
        return

    df_filtered = term_df.filter(col("term").isin(query_terms))
    tf_filtered = term_index.filter(col("term").isin(query_terms))

    joined = tf_filtered.join(df_filtered, "term") \
                       .join(doc_lengths, "doc_id")

    from pyspark.sql.types import FloatType
    from pyspark.sql.functions import udf
    bm25_udf = udf(lambda tf, doc_len, df: bm25_score(tf, doc_len, df, N, avgdl), FloatType())
    scores = joined.withColumn("score", bm25_udf(col("tf"), col("length"), col("df")))

    doc_scores = scores.groupBy("doc_id", "title") \
                       .agg(spark_sum("score").alias("total_score"))

    # Топ-10
    top10 = doc_scores.orderBy(col("total_score").desc()).limit(10)

    print("Top 10 documents:")
    top10.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
    else:
        query = sys.stdin.read().strip()
    main(query)