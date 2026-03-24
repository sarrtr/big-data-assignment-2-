#!/usr/bin/env python3
import math
import re
import sys

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession


KEYSPACE = "search_engine"
HOSTS = ["cassandra-server"]


def bm25_score(tf, doc_len, df, N, avgdl, k1=1.2, b=0.75):
    if N <= 0 or avgdl <= 0 or df <= 0 or doc_len <= 0 or tf <= 0:
        return 0.0
    idf = math.log((N - df + 0.5) / (df + 0.5) + 1.0)
    numerator = (k1 + 1.0) * tf
    denominator = k1 * (1.0 - b + b * (doc_len / avgdl)) + tf
    return idf * (numerator / denominator)


def normalize_query(query):
    return [term for term in re.findall(r"[A-Za-z0-9]+", query.lower()) if term]


def fetch_cassandra_data():
    cluster = Cluster(HOSTS)
    session = cluster.connect()
    try:
        session.set_keyspace(KEYSPACE)
        doc_lengths = {row.doc_id: int(row.length) for row in session.execute("SELECT doc_id, length FROM doc_lengths")}
        doc_meta = {row.doc_id: row.title for row in session.execute("SELECT doc_id, title FROM doc_meta")}
        term_df = {row.term: int(row.df) for row in session.execute("SELECT term, df FROM term_df")}
        term_index = [(row.term, row.doc_id, int(row.tf)) for row in session.execute("SELECT term, doc_id, tf FROM term_index")]
        return doc_lengths, doc_meta, term_df, term_index
    finally:
        session.shutdown()
        cluster.shutdown()


def main(query):
    spark = SparkSession.builder.appName("BM25Ranker").getOrCreate()
    try:
        query_terms = normalize_query(query)
        if not query_terms:
            print("No valid terms in query.")
            return

        doc_lengths, doc_meta, term_df, term_index = fetch_cassandra_data()

        if not doc_lengths or not term_index:
            print("No indexed data found.")
            return

        query_terms_set = set(query_terms)
        postings = [row for row in term_index if row[0] in query_terms_set]
        if not postings:
            print("No matching documents found.")
            return

        N = len(doc_lengths)
        avgdl = sum(doc_lengths.values()) / N if N else 0.0

        sc = spark.sparkContext
        postings_rdd = sc.parallelize(postings)
        bc_doc_lengths = sc.broadcast(doc_lengths)
        bc_term_df = sc.broadcast(term_df)
        bc_query_terms = sc.broadcast(query_terms_set)
        bc_doc_meta = sc.broadcast(doc_meta)
        bc_N = sc.broadcast(N)
        bc_avgdl = sc.broadcast(avgdl)

        scores = (
            postings_rdd
            .filter(lambda row: row[0] in bc_query_terms.value)
            .map(lambda row: (
                row[1],
                bm25_score(
                    row[2],
                    bc_doc_lengths.value.get(row[1], 0),
                    bc_term_df.value.get(row[0], 0),
                    bc_N.value,
                    bc_avgdl.value,
                ),
            ))
            .reduceByKey(lambda a, b: a + b)
        )

        top10 = (
            scores
            .map(lambda item: (item[1], item[0], bc_doc_meta.value.get(item[0], "")))
            .sortBy(lambda item: item[0], ascending=False)
            .take(10)
        )

        print("Top 10 documents:")
        for score, doc_id, title in top10:
            print(f"{doc_id}	{title}	{score:.6f}")
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
    else:
        query = sys.stdin.read().strip()
    main(query)
