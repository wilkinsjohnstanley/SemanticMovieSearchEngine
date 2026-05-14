import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (avg, array_join, col, concat_ws, coalesce,
                                   count, expr, from_json, lit)
from pyspark.sql.types import (ArrayType, FloatType, IntegerType, StringType,
                               StructField, StructType)

MOVIE_SCHEMA = ArrayType(StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
]))

KEYWORD_SCHEMA = ArrayType(StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
]))

CAST_SCHEMA = ArrayType(StructType([
    StructField("cast_id", StringType(), True),
    StructField("character", StringType(), True),
    StructField("credit_id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("order", IntegerType(), True),
    StructField("profile_path", StringType(), True),
]))

CREW_SCHEMA = ArrayType(StructType([
    StructField("credit_id", StringType(), True),
    StructField("department", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("id", StringType(), True),
    StructField("job", StringType(), True),
    StructField("name", StringType(), True),
    StructField("profile_path", StringType(), True),
]))

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def create_spark_session():
    return SparkSession.builder \
        .appName("MovieDataEnrich") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()

def resolve_path(path):
    if os.path.isabs(path):
        return path
    return os.path.abspath(os.path.join(BASE_DIR, path))


def read_optional_parquet(spark, path):
    if os.path.exists(path):
        return spark.read.parquet(path)
    return None

def enrich_movies(movies_df, credits_df, keywords_df, ratings_df):
    if credits_df is not None:
        movies_df = movies_df.join(credits_df.select("id", "cast", "crew"), on="id", how="left")

    if keywords_df is not None:
        movies_df = movies_df.join(keywords_df.select("id", "keywords"), on="id", how="left")

    if ratings_df is not None:
        ratings_agg = ratings_df.groupBy("movieId").agg(
    avg("rating").alias("avg_rating"),
    count("rating").alias("rating_count"),
)

        movies_df = movies_df.join(
            ratings_agg,
            movies_df["id"] == ratings_agg["movieId"],
            "left"
        ).drop("movieId")
    else:
        movies_df = movies_df.withColumn("avg_rating", lit(None).cast(FloatType())) \
            .withColumn("rating_count", lit(0))

    movies_df = movies_df.withColumn("genres_json", from_json(col("genres"), MOVIE_SCHEMA))
    movies_df = movies_df.withColumn("keyword_names", from_json(col("keywords"), KEYWORD_SCHEMA))
    movies_df = movies_df.withColumn("cast_json", from_json(col("cast"), CAST_SCHEMA))
    movies_df = movies_df.withColumn("crew_json", from_json(col("crew"), CREW_SCHEMA))

    movies_df = movies_df.withColumn("genre_list", expr("transform(genres_json, x -> x.name)"))
    movies_df = movies_df.withColumn("keyword_list", expr("transform(keyword_names, x -> x.name)"))
    movies_df = movies_df.withColumn("top_cast", expr("slice(transform(cast_json, x -> x.name), 1, 5)"))
    movies_df = movies_df.withColumn("director", expr(
        "element_at(transform(filter(crew_json, x -> x.job = 'Director'), x -> x.name), 1)"
    ))

    movies_df = movies_df.withColumn(
        "combined_text",
        concat_ws(
            " ",
            coalesce(col("title"), lit("")),
            coalesce(col("tagline"), lit("")),
            coalesce(col("overview"), lit("")),
            coalesce(array_join(col("top_cast"), " "), lit("")),
            coalesce(array_join(col("keyword_list"), " "), lit("")),
            coalesce(array_join(col("genre_list"), " "), lit("")),
        )
    )

    return movies_df.select(
        "id",
        "title",
        "release_date",
        "overview",
        "tagline",
        "genre_list",
        "keyword_list",
        "director",
        "top_cast",
        "avg_rating",
        "rating_count",
        "combined_text",
    )

def main(silver_dir, gold_dir):
    silver_dir = resolve_path(silver_dir)
    gold_dir = resolve_path(gold_dir)

    spark = create_spark_session()
    print(f"Processing silver_dir: {silver_dir}, gold_dir: {gold_dir}")

    movies_df = read_optional_parquet(spark, os.path.join(silver_dir, "movies.parquet"))
    if movies_df is None:
        raise FileNotFoundError(f"Missing silver movies.parquet at {os.path.join(silver_dir, 'movies.parquet')}")

    credits_df = read_optional_parquet(spark, os.path.join(silver_dir, "credits.parquet"))
    keywords_df = read_optional_parquet(spark, os.path.join(silver_dir, "keywords.parquet"))
    ratings_df = read_optional_parquet(spark, os.path.join(silver_dir, "ratings.parquet"))

    enriched_df = enrich_movies(movies_df, credits_df, keywords_df, ratings_df)

    os.makedirs(gold_dir, exist_ok=True)
    enriched_df.write.mode("overwrite").parquet(os.path.join(gold_dir, "movies.parquet"))
    print("Gold layer written to:", os.path.join(gold_dir, "movies.parquet"))

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich silver movie data into a gold layer.")
    parser.add_argument("--silver", default="ingest/silver", help="Path to silver directory")
    parser.add_argument("--gold", default="enrich/gold", help="Path to gold directory")
    args = parser.parse_args()
    main(args.silver, args.gold)
