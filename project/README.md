# Ingest Layer

## Overview

`ingest.py` reads raw CSV files from a configurable bronze directory, cleans them, and writes Silver-layer Parquet files.

## Usage

```bash
py ingest.py --bronze bronze --silver ingest/silver
```

Both arguments are optional and default to `bronze` and `ingest/silver` respectively.

## Prerequisites (Windows)

Spark on Windows requires `winutils.exe` and `hadoop.dll`. Place both in `C:\hadoop\bin\` and set `HADOOP_HOME=C:\hadoop`. The script also sets these programmatically at startup, so the environment variable is optional if running directly.

## Input Files (Bronze)

All CSVs are expected in the bronze directory:

- `movies_metadata.csv`
- `credits.csv`
- `keywords.csv`
- `ratings_small.csv`
- `links_small.csv` (used to map MovieLens `movieId` to TMDB `id` for ratings enrichment)

Missing files are skipped without error.

## Output Files (Silver)

Written to the silver directory as Parquet:

- `movies.parquet`
- `credits.parquet`
- `keywords.parquet`
- `ratings.parquet`

All outputs are written with `mode("overwrite")`, making the script idempotent — re-runs produce identical output.

## Corruption Handling

### Movies (`movies_metadata.csv`)
- Rows missing `id` or `title` are dropped
- `release_date` is normalized (`.` separators replaced with `-`) and parsed to `DateType`
- Rows with `release_date` outside 1900–2030 are dropped (nulls are retained)
- `budget` and `revenue` are stripped of currency formatting (`$`, `,`, whitespace) before casting to `FloatType`; unparseable values become `0.0`
- `runtime` is cast to `FloatType`; nulls or non-positive values become `null`
- `title`, `overview`, and `tagline` are trimmed, have internal whitespace normalized, and non-ASCII characters (mojibake artifacts) are removed
- `genres` is validated as a JSON array string; invalid values are replaced with `"[]"`
- Exact duplicates are dropped, then near-duplicates on `(title, release_date)` are dropped using case-insensitive title comparison

### Credits (`credits.csv`)
- Rows missing `id` are dropped
- `id` is cast to `StringType` for consistent joining
- `cast` and `crew` are validated as JSON array strings; invalid values are replaced with `"[]"`
- Duplicates on `id` are dropped
- Rows whose `id` does not exist in `movies_metadata` are dropped (orphan removal)

### Keywords (`keywords.csv`)
- Rows missing `id` are dropped
- `id` is cast to `StringType` for consistent joining
- `keywords` is validated as a JSON array string; invalid values are replaced with `"[]"`
- Duplicates on `id` are dropped
- Rows whose `id` does not exist in `movies_metadata` are dropped (orphan removal)

### Ratings (`ratings_small.csv`)
- Rows missing `userId`, `movieId`, or `rating` are dropped
- `rating` is cast to `FloatType`
- Ratings outside `[0.5, 5.0]` are dropped
- Exact duplicates are dropped

## Inspecting Output

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Inspect").getOrCreate()
df = spark.read.parquet("ingest/silver/movies.parquet")
df.printSchema()
df.show()
```## Gold Layer

`enrich/enrich.py` reads the silver Parquet data and produces enriched gold-layer Parquet files in `enrich/gold`.

### Usage

```bash
py enrich.py --silver ingest/silver --gold enrich/gold
```

Both arguments are optional and default to `ingest/silver` and `enrich/gold` respectively.

### Gold Output

The gold layer includes joined and enriched movie records with:

- `id`, `title`, `release_date`, `overview`, `tagline`
- `genres` as a cleaned genre name list
- `keywords` as a cleaned keyword name list
- `director` and `top_cast` extracted from credits
- aggregated rating features: `avg_rating` and `rating_count`
- `combined_text` for semantic embedding

### Enrichment Notes

- Ratings are aggregated from `ratings.parquet`; the ingest stage uses `links_small.csv` to map MovieLens IDs to TMDB IDs before silver writing.
- Movies are left-joined with credits, keywords, and ratings so missing ancillary data does not drop the base movie record.
- The `combined_text` field concatenates title, tagline, overview, top cast names, keywords, and genres to support Weaviate semantic search.

## Load to Weaviate

`load_weaviate/load_weaviate.py` loads `enrich/gold/movies.parquet` into a local Weaviate instance.

### Usage

```bash
py load_weaviate/load_weaviate.py --gold enrich/gold --endpoint http://localhost:8080
```

### Docker Compose

A local Weaviate stack is available at `load_weaviate/docker-compose.yml`. Start it with:

```bash
cd load_weaviate
docker compose up -d
```

### Search

`search/search.py` queries the loaded Weaviate index using `nearText`.

```bash
py ../search/search.py "science fiction robots"
```

The script returns top matches with title, release date, director, cast, rating, and an overview snippet.
