from itertools import islice

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from pyspark.sql import SparkSession


KEYSPACE = "search_engine"
HOSTS = ["cassandra-server"]
BATCH_SIZE = 200


def ensure_schema(session):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(KEYSPACE)

    session.execute("""
        CREATE TABLE IF NOT EXISTS doc_lengths (
            doc_id text PRIMARY KEY,
            length int
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS doc_meta (
            doc_id text PRIMARY KEY,
            title text
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS term_df (
            term text PRIMARY KEY,
            df int
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS term_index (
            term text,
            doc_id text,
            tf int,
            PRIMARY KEY (term, doc_id)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            metric text PRIMARY KEY,
            value text
        )
    """)


def insert_many(session, prepared_stmt, rows):
    batch = BatchStatement()
    count = 0
    has_items = False
    for row in rows:
        batch.add(prepared_stmt, row)
        has_items = True
        count += 1
        if count % BATCH_SIZE == 0:
            session.execute(batch)
            batch = BatchStatement()
            has_items = False
    if has_items:
        session.execute(batch)


def parse_line(line):
    parts = line.split("	")
    if not parts:
        return None
    tag = parts[0]
    if tag == "DOC_LEN" and len(parts) >= 3:
        return ("DOC_LEN", parts[1], int(parts[2]))
    if tag == "DOC_META" and len(parts) >= 3:
        return ("DOC_META", parts[1], "	".join(parts[2:]))
    if tag == "TERM_DF" and len(parts) >= 3:
        return ("TERM_DF", parts[1], int(parts[2]))
    if tag == "TERM_TF" and len(parts) >= 4:
        return ("TERM_TF", parts[1], parts[2], int(parts[3]))
    if tag == "STATS" and len(parts) >= 3:
        return ("STATS", parts[1], "	".join(parts[2:]))
    return None


def main():
    spark = SparkSession.builder.appName("LoadIndexToCassandra").getOrCreate()
    try:
        sc = spark.sparkContext
        lines = sc.textFile("/indexer/part-*")
        parsed = lines.map(parse_line).filter(lambda x: x is not None).collect()

        doc_lengths = []
        doc_meta = []
        term_df = []
        term_index = []
        stats = []

        for item in parsed:
            kind = item[0]
            if kind == "DOC_LEN":
                _, doc_id, length = item
                doc_lengths.append((doc_id, int(length)))
            elif kind == "DOC_META":
                _, doc_id, title = item
                doc_meta.append((doc_id, title))
            elif kind == "TERM_DF":
                _, term, df = item
                term_df.append((term, int(df)))
            elif kind == "TERM_TF":
                _, term, doc_id, tf = item
                term_index.append((term, doc_id, int(tf)))
            elif kind == "STATS":
                _, metric, value = item
                stats.append((metric, value))

        cluster = Cluster(HOSTS)
        session = cluster.connect()
        try:
            ensure_schema(session)

            ps_doc_lengths = session.prepare("INSERT INTO doc_lengths (doc_id, length) VALUES (?, ?)")
            ps_doc_meta = session.prepare("INSERT INTO doc_meta (doc_id, title) VALUES (?, ?)")
            ps_term_df = session.prepare("INSERT INTO term_df (term, df) VALUES (?, ?)")
            ps_term_index = session.prepare("INSERT INTO term_index (term, doc_id, tf) VALUES (?, ?, ?)")
            ps_stats = session.prepare("INSERT INTO stats (metric, value) VALUES (?, ?)")

            insert_many(session, ps_doc_lengths, doc_lengths)
            insert_many(session, ps_doc_meta, doc_meta)
            insert_many(session, ps_term_df, term_df)
            insert_many(session, ps_term_index, term_index)
            insert_many(session, ps_stats, stats)
        finally:
            session.shutdown()
            cluster.shutdown()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
