from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LoadIndexToCassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .getOrCreate()

sc = spark.sparkContext

lines = sc.textFile("/indexer/part-*")

def parse_line(line):
    parts = line.split('\t')
    if parts[0] == "DOC_LEN":
        return ("DOC_LEN", parts[1], int(parts[2]))
    elif parts[0] == "TERM_DF":
        return ("TERM_DF", parts[1], int(parts[2]))
    elif parts[0] == "TERM_TF":
        return ("TERM_TF", parts[1], parts[2], int(parts[3]))
    else:
        return None

parsed = lines.map(parse_line).filter(lambda x: x is not None)

doc_len_rdd = parsed.filter(lambda x: x[0] == "DOC_LEN").map(lambda x: (x[1], x[2]))
term_df_rdd = parsed.filter(lambda x: x[0] == "TERM_DF").map(lambda x: (x[1], x[2]))
term_tf_rdd = parsed.filter(lambda x: x[0] == "TERM_TF").map(lambda x: (x[1], x[2], x[3]))

df_doc_len = doc_len_rdd.toDF(["doc_id", "length"])
df_term_df = term_df_rdd.toDF(["term", "df"])
df_term_tf = term_tf_rdd.toDF(["term", "doc_id", "tf"])

spark.sql("""
CREATE KEYSPACE IF NOT EXISTS search_engine
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS search_engine.doc_lengths (
    doc_id text PRIMARY KEY,
    length int
)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS search_engine.term_df (
    term text PRIMARY KEY,
    df int
)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS search_engine.term_index (
    term text,
    doc_id text,
    tf int,
    PRIMARY KEY (term, doc_id)
)
""")

df_doc_len.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="doc_lengths", keyspace="search_engine") \
    .save()

df_term_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="term_df", keyspace="search_engine") \
    .save()

df_term_tf.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="term_index", keyspace="search_engine") \
    .save()

spark.stop()