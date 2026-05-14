import argparse
import os

# Set HADOOP_HOME before importing Spark
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = os.environ.get('PATH', '') + r';C:\hadoop\bin'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, lower, when, lit
from pyspark.sql.types import StringType, FloatType

def create_spark_session():
    return SparkSession.builder \
        .appName("MovieDataIngest") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()

def clean_movies(df):
    from pyspark.sql.functions import to_date, year

    # Drop rows missing critical fields
    df = df.dropna(subset=["id", "title"])

    # Normalize and parse release_date
    df = df.withColumn("release_date", regexp_replace(col("release_date"), r"\.", "-"))
    df = df.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))

    # Drop impossible dates
    df = df.filter(
        (year(col("release_date")).isNull()) |
        ((year(col("release_date")) >= 1900) & (year(col("release_date")) <= 2030))
    )

    # Clean currency-formatted numeric fields
    for c in ["budget", "revenue"]:
        df = df.withColumn(c, regexp_replace(col(c).cast(StringType()), r"[\$,\s]", ""))
        df = df.withColumn(c, when(col(c).cast(FloatType()).isNotNull(), col(c).cast(FloatType())).otherwise(lit(0.0)))

    # Clean runtime
    df = df.withColumn("runtime", when(
        (col("runtime").cast(FloatType()).isNotNull()) & (col("runtime").cast(FloatType()) > 0),
        col("runtime").cast(FloatType())
    ).otherwise(lit(None)))

    # Normalize text fields: whitespace and strip non-ASCII (mojibake)
    text_cols = ["title", "overview", "tagline"]
    for c in text_cols:
        if c in df.columns:
            df = df.withColumn(c, trim(regexp_replace(col(c), r"\s+", " ")))
            df = df.withColumn(c, regexp_replace(col(c), r"[^\x00-\x7F]", ""))

    # Validate JSON array fields
    if "genres" in df.columns:
        df = df.withColumn("genres", when(col("genres").rlike(r"^\[.*\]$"), col("genres")).otherwise(lit("[]")))

    # Deduplicate: exact, then case-insensitive near-duplicates
    df = df.dropDuplicates()
    df = df.withColumn("_title_norm", lower(trim(col("title"))))
    df = df.dropDuplicates(["_title_norm", "release_date"])
    df = df.drop("_title_norm")

    return df

def clean_credits(df):
    df = df.dropna(subset=["id"])
    df = df.withColumn("id", col("id").cast(StringType()))
    df = df.withColumn("cast", when(col("cast").rlike(r"^\[.*\]$"), col("cast")).otherwise(lit("[]")))
    df = df.withColumn("crew", when(col("crew").rlike(r"^\[.*\]$"), col("crew")).otherwise(lit("[]")))
    return df.dropDuplicates(["id"])

def clean_keywords(df):
    df = df.dropna(subset=["id"])
    df = df.withColumn("id", col("id").cast(StringType()))
    df = df.withColumn("keywords", when(col("keywords").rlike(r"^\[.*\]$"), col("keywords")).otherwise(lit("[]")))
    return df.dropDuplicates(["id"])

def clean_ratings(df):
    df = df.dropna(subset=["userId", "movieId", "rating"])
    df = df.withColumn("userId", col("userId").cast(StringType()))
    df = df.withColumn("movieId", col("movieId").cast(StringType()))
    df = df.withColumn("rating", col("rating").cast(FloatType()))
    df = df.filter((col("rating") >= 0.5) & (col("rating") <= 5.0))
    return df.dropDuplicates()

def main(bronze_dir, silver_dir):
    spark = create_spark_session()
    print(f"Processing bronze_dir: {bronze_dir}, silver_dir: {silver_dir}")

    movies_df = None

    # Read and clean movies
    movies_path = os.path.join(bronze_dir, "movies_metadata.csv")
    print(f"Checking movies_path: {movies_path}, exists: {os.path.exists(movies_path)}")
    if os.path.exists(movies_path):
        movies_df = spark.read.csv(movies_path, header=True, inferSchema=True, multiLine=True, escape='"')
        print(f"Movies DF count before clean: {movies_df.count()}")
        movies_df = clean_movies(movies_df)
        movies_df = movies_df.withColumn("id", col("id").cast(StringType()))
        print(f"Movies DF count after clean: {movies_df.count()}")

    # Read and clean credits
    credits_df = None
    credits_path = os.path.join(bronze_dir, "credits.csv")
    if os.path.exists(credits_path):
        credits_df = spark.read.csv(credits_path, header=True, inferSchema=True, multiLine=True, escape='"')
        print(f"Credits DF count before clean: {credits_df.count()}")
        credits_df = clean_credits(credits_df)
        print(f"Credits DF count after clean: {credits_df.count()}")

    # Read and clean keywords
    keywords_df = None
    keywords_path = os.path.join(bronze_dir, "keywords.csv")
    if os.path.exists(keywords_path):
        keywords_df = spark.read.csv(keywords_path, header=True, inferSchema=True, multiLine=True, escape='"')
        print(f"Keywords DF count before clean: {keywords_df.count()}")
        keywords_df = clean_keywords(keywords_df)
        print(f"Keywords DF count after clean: {keywords_df.count()}")

    # Read and clean ratings
    ratings_df = None
    ratings_path = os.path.join(bronze_dir, "ratings_small.csv")
    links_df = None
    links_path = os.path.join(bronze_dir, "links_small.csv")

    if os.path.exists(ratings_path):
        ratings_df = spark.read.csv(ratings_path, header=True, inferSchema=True)
        print(f"Ratings DF count before clean: {ratings_df.count()}")
        ratings_df = clean_ratings(ratings_df)
        print(f"Ratings DF count after clean: {ratings_df.count()}")

    if os.path.exists(links_path):
        links_df = spark.read.csv(links_path, header=True, inferSchema=True)
        links_df = links_df.dropna(subset=["movieId", "tmdbId"]).withColumn("movieId", col("movieId").cast(StringType())).withColumn("tmdbId", col("tmdbId").cast(StringType()))

    if ratings_df is not None and links_df is not None:
        ratings_df = ratings_df.join(links_df, on="movieId", how="inner")
        ratings_df = ratings_df.withColumn("id", col("tmdbId"))
        print(f"Ratings DF count after linking to TMDB ids: {ratings_df.count()}")

    # Remove orphan IDs from credits and keywords
    if movies_df is not None:
        valid_ids = movies_df.select(col("id").cast(StringType()).alias("id"))
        if credits_df is not None:
            credits_df = credits_df.join(valid_ids, on="id", how="inner")
        if keywords_df is not None:
            keywords_df = keywords_df.join(valid_ids, on="id", how="inner")

    # Write silver layer
    os.makedirs(silver_dir, exist_ok=True)

    if movies_df is not None:
        try:
            movies_df.write.mode("overwrite").parquet(os.path.join(silver_dir, "movies.parquet"))
            print("Movies silver layer written.")
        except Exception as e:
            print(f"Error writing movies: {e}")

    if credits_df is not None:
        try:
            credits_df.write.mode("overwrite").parquet(os.path.join(silver_dir, "credits.parquet"))
            print("Credits silver layer written.")
        except Exception as e:
            print(f"Error writing credits: {e}")

    if keywords_df is not None:
        try:
            keywords_df.write.mode("overwrite").parquet(os.path.join(silver_dir, "keywords.parquet"))
            print("Keywords silver layer written.")
        except Exception as e:
            print(f"Error writing keywords: {e}")

    if ratings_df is not None:
        try:
            ratings_df.write.mode("overwrite").parquet(os.path.join(silver_dir, "ratings.parquet"))
            print("Ratings silver layer written.")
        except Exception as e:
            print(f"Error writing ratings: {e}")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest and clean movie data from bronze to silver layer.")
    parser.add_argument("--bronze", default="bronze", help="Path to bronze directory")
    parser.add_argument("--silver", default="ingest/silver", help="Path to silver directory")
    args = parser.parse_args()

    main(args.bronze, args.silver)