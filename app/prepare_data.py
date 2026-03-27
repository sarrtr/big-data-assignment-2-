import os
import re
import shutil
from pathlib import Path

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession

# def find_parquet_path() -> Path:
#     env_path = os.environ.get("PARQUET_PATH")
#     candidates = []
#     if env_path:
#         candidates.append(Path(env_path))

#     here = Path(__file__).resolve().parent
#     candidates.extend(
#         [
#             here.parent / "a.parquet",
#             here / "a.parquet",
#             Path.cwd() / "a.parquet",
#             Path("/workspace/a.parquet"),
#             Path("/app/a.parquet"),
#         ]
#     )

#     for candidate in candidates:
#         if candidate.exists():
#             return candidate.resolve()

#     raise FileNotFoundError("a.parquet was not found in any expected location")


def clean_text(value):
    if value is None:
        return ""
    return str(value).replace("\r", " ").replace("\n", " ").replace("\t", " ").strip()

def clean_title(title: str) -> str:
    title = clean_text(title)
    title = sanitize_filename(title)
    title = re.sub(r"\s+", "_", title.strip())
    title = re.sub(r"_+", "_", title).strip("_")
    return title

def main():
    spark = SparkSession.builder.appName("data preparation").getOrCreate()
    try:
        parquet_path = Path(__file__).resolve().parent.parent / "a.parquet"
        data_dir = Path(__file__).resolve().parent / "data"
        shutil.rmtree(data_dir, ignore_errors=True)
        data_dir.mkdir(parents=True, exist_ok=True)

        df = (
            spark.read.parquet(f"file://{parquet_path}")
            .select("id", "title", "text")
            .limit(1000)
            .fillna({"title": "", "text": ""})
        )

        rows = df.collect()
        if not rows:
            raise RuntimeError("No rows were read from parquet file")

        input_lines = []
        for row in rows:
            doc_id = clean_text(row["id"])
            title = clean_text(row["title"])
            title = clean_title(title)
            text = clean_text(row["text"])
            if not doc_id:
                continue

            local_file = data_dir / f"{doc_id}_{title}.txt"
            local_file.write_text(text, encoding="utf-8")
            input_lines.append(f"{doc_id}\t{title}\t{text}")

        if not input_lines:
            raise RuntimeError("All parquet rows were filtered out")

        input_rdd = spark.sparkContext.parallelize(input_lines, 1)
        input_rdd.saveAsTextFile("/input/data")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()