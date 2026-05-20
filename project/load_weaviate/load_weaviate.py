import argparse
import os
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import weaviate

CLASS_NAME = "Movie"

def create_spark_session():
    return SparkSession.builder \
        .appName("WeaviateLoader") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()

def read_gold_parquet(spark, gold_path):
    if not os.path.exists(gold_path):
        raise FileNotFoundError(f"Gold parquet path not found: {gold_path}")
    df = spark.read.parquet(gold_path)
    return df

def create_schema(client, class_name):
    schema = {
        "class": class_name,
        "vectorizer": "text2vec-transformers",
        "moduleConfig": {
            "text2vec-transformers": {
                "model": "sentence-transformers/all-MiniLM-L6-v2"
            }
        },
        "properties": [
            {"name": "tmdb_id", "dataType": ["string"]},
            {"name": "title", "dataType": ["text"]},
            {"name": "release_date", "dataType": ["date"]},
            {"name": "overview", "dataType": ["text"]},
            {"name": "tagline", "dataType": ["text"]},
            {"name": "genre_list", "dataType": ["text[]"]},
            {"name": "keyword_list", "dataType": ["text[]"]},
            {"name": "director", "dataType": ["text"]},
            {"name": "top_cast", "dataType": ["text[]"]},
            {"name": "avg_rating", "dataType": ["number"]},
            {"name": "rating_count", "dataType": ["int"]},
            {"name": "combined_text", "dataType": ["text"]},
        ],
    }

    if client.schema.exists(class_name):
        client.schema.delete_class(class_name)
    client.schema.create_class(schema)

def _serialize_date(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).isoformat().replace('+00:00', 'Z')
        return value.isoformat()
    if isinstance(value, date):
        return f"{value.isoformat()}T00:00:00Z"
    string_value = str(value).strip()
    if not string_value:
        return None
    try:
        parsed = datetime.fromisoformat(string_value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc).isoformat().replace('+00:00', 'Z')
        return parsed.isoformat()
    except ValueError:
        return string_value


def build_object(row):
    return {
        "tmdb_id": str(row.id) if row.id is not None else "",
        "title": row.title or "",
        "release_date": _serialize_date(row.release_date),
        "overview": row.overview or "",
        "tagline": row.tagline or "",
        "genre_list": row.genre_list if row.genre_list is not None else [],
        "keyword_list": row.keyword_list if row.keyword_list is not None else [],
        "director": row.director or "",
        "top_cast": row.top_cast if row.top_cast is not None else [],
        "avg_rating": float(row.avg_rating) if row.avg_rating is not None else None,
        "rating_count": int(row.rating_count) if row.rating_count is not None else 0,
        "combined_text": row.combined_text or "",
    }

def load_objects(client, class_name, objects):
    with client.batch as batch:
        batch.batch_size = 64
        batch.dynamic = True
        for obj in objects:
            batch.add_data_object(obj, class_name)

def main(gold_dir, endpoint):
    gold_path = os.path.join(gold_dir, "movies.parquet")
    spark = create_spark_session()
    print(f"Reading gold data from {gold_path}")
    gold_df = read_gold_parquet(spark, gold_path)

    client = weaviate.Client(url=endpoint)
    print(f"Connecting to Weaviate at {endpoint}")
    create_schema(client, CLASS_NAME)

    objects = []
    for row in gold_df.collect():
        objects.append(build_object(row))

    print(f"Loaded {len(objects)} gold records from Parquet")
    if objects:
        load_objects(client, CLASS_NAME, objects)
        print(f"Successfully loaded {len(objects)} objects into Weaviate class {CLASS_NAME}")
    else:
        print("No objects found to load.")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load gold movie data into Weaviate.")
    parser.add_argument("--gold", default="enrich/gold", help="Path to gold directory")
    parser.add_argument("--endpoint", default="http://localhost:8080", help="Weaviate endpoint")
    args = parser.parse_args()
    main(args.gold, args.endpoint)
