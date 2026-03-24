import re
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('data preparation').getOrCreate()
sc = spark.sparkContext

df = spark.read.parquet("/a.parquet")
n = 1000
df = df.select(['id', 'title', 'text']).limit(n)

def write_to_hdfs(row):
    safe_title = re.sub(r'[^\w\s\-()]', '', row.title).replace(' ', '_')
    filename = f"/data/{row.id}_{safe_title}.txt"
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    path = sc._jvm.org.apache.hadoop.fs.Path(filename)
    out = fs.create(path, True)
    out.write(row.text.encode('utf-8'))
    out.close()

df.foreach(write_to_hdfs)

data_rdd = sc.wholeTextFiles("/data")

def parse_filename(path):
    fname = path.split('/')[-1]
    doc_id = fname.split('_')[0]
    doc_title = '_'.join(fname.split('_')[1:]).replace('.txt', '')
    return (doc_id, doc_title)

lines_rdd = data_rdd.map(lambda kv: f"{parse_filename(kv[0])[0]}\t{parse_filename(kv[0])[1]}\t{kv[1]}")
lines_rdd.saveAsTextFile("/input/data", compressionCodecClass=None)

spark.stop()