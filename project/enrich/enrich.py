import argparse
import ast
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array_join,
    avg,
    coalesce,
    col,
    concat_ws,
    count,
    lit,
    udf,
)
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    StringType,
)

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("MovieDataEnrich")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def resolve_path(path):
    if os.path.isabs(path):
        return path

    return os.path.abspath(
        os.path.join(BASE_DIR, path)
    )


def read_optional_parquet(spark, path):
    if os.path.exists(path):
        return spark.read.parquet(path)

    return None


# --------------------------------------------------
# Python parsing helpers
# --------------------------------------------------

def extract_director(crew_text):

    if not crew_text:
        return None

    try:
        crew = ast.literal_eval(crew_text)

        for member in crew:

            if not isinstance(member, dict):
                continue

            job = str(
                member.get("job", "")
            ).lower()

            if "director" in job:
                return member.get("name")

    except Exception:
        return None

    return None


def extract_top_cast(cast_text):

    if not cast_text:
        return []

    try:
        cast = ast.literal_eval(cast_text)

        names = []

        for actor in cast[:5]:

            if not isinstance(actor, dict):
                continue

            name = actor.get("name")

            if name:
                names.append(name)

        return names

    except Exception:
        return []


def extract_names(list_text):

    if not list_text:
        return []

    try:
        values = ast.literal_eval(list_text)

        names = []

        for item in values:

            if not isinstance(item, dict):
                continue

            name = item.get("name")

            if name:
                names.append(name)

        return names

    except Exception:
        return []


# --------------------------------------------------
# Main enrichment logic
# --------------------------------------------------

def enrich_movies(
    movies_df,
    credits_df,
    keywords_df,
    ratings_df
):

    # ----------------------------------------------
    # Create UDFs INSIDE function
    # Fixes PySpark pickling recursion bug
    # ----------------------------------------------

    director_udf = udf(
        extract_director,
        StringType()
    )

    top_cast_udf = udf(
        extract_top_cast,
        ArrayType(StringType())
    )

    name_list_udf = udf(
        extract_names,
        ArrayType(StringType())
    )

    # ----------------------------------------------
    # Join credits
    # ----------------------------------------------

    if credits_df is not None:

        movies_df = movies_df.join(
            credits_df.select(
                "id",
                "cast",
                "crew"
            ),
            on="id",
            how="left"
        )

    # ----------------------------------------------
    # Join keywords
    # ----------------------------------------------

    if keywords_df is not None:

        movies_df = movies_df.join(
            keywords_df.select(
                "id",
                "keywords"
            ),
            on="id",
            how="left"
        )

    # ----------------------------------------------
    # Ratings aggregation
    # ----------------------------------------------

    if ratings_df is not None:

        ratings_agg = ratings_df.groupBy("id").agg(
            avg("rating").alias("avg_rating"),
            count("rating").alias("rating_count"),
        )

        movies_df = movies_df.join(
            ratings_agg,
            on="id",
            how="left"
        )

    else:

        movies_df = (
            movies_df
            .withColumn(
                "avg_rating",
                lit(None).cast(FloatType())
            )
            .withColumn(
                "rating_count",
                lit(0)
            )
        )

    # ----------------------------------------------
    # Extract structured fields
    # ----------------------------------------------

    movies_df = movies_df.withColumn(
        "genre_list",
        name_list_udf(col("genres"))
    )

    movies_df = movies_df.withColumn(
        "keyword_list",
        name_list_udf(col("keywords"))
    )

    movies_df = movies_df.withColumn(
        "top_cast",
        top_cast_udf(col("cast"))
    )

    movies_df = movies_df.withColumn(
        "director",
        director_udf(col("crew"))
    )

    # ----------------------------------------------
    # Build semantic search text
    # ----------------------------------------------

    movies_df = movies_df.withColumn(
        "combined_text",
        concat_ws(
            " ",

            coalesce(col("title"), lit("")),

            coalesce(
                col("tagline"),
                lit("")
            ),

            coalesce(
                col("overview"),
                lit("")
            ),

            coalesce(
                col("director"),
                lit("")
            ),

            coalesce(
                array_join(
                    col("top_cast"),
                    " "
                ),
                lit("")
            ),

            coalesce(
                array_join(
                    col("keyword_list"),
                    " "
                ),
                lit("")
            ),

            coalesce(
                array_join(
                    col("genre_list"),
                    " "
                ),
                lit("")
            ),
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

    print(
        f"Processing silver_dir: {silver_dir}, "
        f"gold_dir: {gold_dir}"
    )

    movies_df = read_optional_parquet(
        spark,
        os.path.join(
            silver_dir,
            "movies.parquet"
        )
    )

    if movies_df is None:

        raise FileNotFoundError(
            f"Missing movies.parquet at "
            f"{os.path.join(silver_dir, 'movies.parquet')}"
        )

    credits_df = read_optional_parquet(
        spark,
        os.path.join(
            silver_dir,
            "credits.parquet"
        )
    )

    keywords_df = read_optional_parquet(
        spark,
        os.path.join(
            silver_dir,
            "keywords.parquet"
        )
    )

    ratings_df = read_optional_parquet(
        spark,
        os.path.join(
            silver_dir,
            "ratings.parquet"
        )
    )

    enriched_df = enrich_movies(
        movies_df,
        credits_df,
        keywords_df,
        ratings_df
    )

    os.makedirs(
        gold_dir,
        exist_ok=True
    )

    output_path = os.path.join(
        gold_dir,
        "movies.parquet"
    )

    (
        enriched_df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(
        f"Gold layer written to: {output_path}"
    )

    spark.stop()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Enrich silver movie data "
            "into a gold layer."
        )
    )

    parser.add_argument(
        "--silver",
        default="ingest/silver",
        help="Path to silver directory"
    )

    parser.add_argument(
        "--gold",
        default="enrich/gold",
        help="Path to gold directory"
    )

    args = parser.parse_args()

    main(
        args.silver,
        args.gold
    )