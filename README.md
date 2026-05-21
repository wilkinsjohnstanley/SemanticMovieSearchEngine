# Ingest Layer

## Overview

`ingest.py` reads raw CSV files from a configurable bronze directory, cleans them, and writes Silver-layer Parquet files.

## Usage

```Download the requirements
pip install -r requirements.txt
```
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



cd enrich

Set the PySpark Python executable manually before running.

In your terminal:

set PYSPARK_PYTHON=python
set PYSPARK_DRIVER_PYTHON=python


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

The script returns top matches with title, release date, director, cast, rating, and an overview snippet:
```
    OMG, I'm a Robot! (2015-08-06T00:00:00Z)    Director: Tal Goldberg    Top cast: Yotam Ishay, Hili Yalon, Tzahi Grad, Dror Keren, Rob Schneider    Overview: A sensitive guy finds out he's... a robot.

 

    Robot Monster (1953-06-25T00:00:00Z)    Director: Jack Greenhalgh    Top cast: George Nader, Claudia Barrett, Selena Royle, John Mylong, Gregory Moffett    Overview: Ro-Man, an alien robot who greatly resembles a gorilla in a diving helmet, is sent to earth to destroy all human life. Ro-Man falls in love with one of the last six remaining humans, and struggles to understand how his programming can...

 

    Robotropolis (2011-09-02T00:00:00Z)    Director: Christopher Hatton    Top cast: Zoe Naylor, Graham Sibley, Edward Foy, Jourdan Lee    Overview: A group of reporters are covering the unveiling of a new facility that is completely maintained by robot prototypes. When one of the robots goes haywire, the reporters find themselves not just reporting on the malfunction, but fighting...

 

    Transmorphers (2007-06-26T00:00:00Z)    Director: Leigh Scott    Top cast: Matthew Wolf, Amy Weber, Shaley Scott, Eliza Swenson, Griff Furst    Overview: About a race of alien robots that have conquered Earth and forced humanity underground. After 400 years, a small group of humans develop a plan to defeat the mechanical invaders in the ultimate battle between man and machine.

 

    Hands of Steel (1986-03-26T00:00:00Z)    Director: Sergio Martino    Top cast: Daniel Greene, Janet Ågren, Claudio Cassinelli, George Eastman, John Saxon    Overview: A story about a cyborg who is programmed to kill a scientist who holds the fate of mankind in his hands.

 

    I, Robot (2004-07-15T00:00:00Z)    Director: Simon Duggan    Top cast: Will Smith, Bridget Moynahan, Alan Tudyk, James Cromwell, Bruce Greenwood    Avg rating: 3.34 (57 ratings)    Overview: In 2035, where robots are common-place and abide by the three laws of robotics, a techno-phobic cop investigates an apparent suicide. Suspecting that a robot may be responsible for the death, his investigation leads him to believe that...

 

    Robot Stories (2003-01-01T00:00:00Z)    Director: Greg Pak    Top cast: Karen Tsen Lee, Glenn Kubota, Tamlyn Tomita, James Saito, Vin Knight    Overview: Four stories including: "My Robot Baby," in which a couple must care for a robot baby before adopting a human child; "The Robot Fixer," in which a mother tries to connect with her dying son; "Machine Love," in which an office worker...

 

    The War of the Robots (1978-01-01T00:00:00Z)    Director: Alfonso Brescia    Top cast: Antonio Sabàto, Yanti Somer, Malisa Longo, Patrizia Gori, Giacomo Rossi-Stuart    Overview: An alien civilization, which facing eminent extinction, kidnaps two famous genetic scientists from Earth. A troop of soldiers is dispatched to combat the humanoid robots and rescue the victims.

 

    Making Mr. Right (1987-01-01T00:00:00Z)    Director: Susan Seidelman    Top cast: John Malkovich, Ann Magnuson, Glenne Headly, Ben Masters, Polly Bergen    Avg rating: 4.00 (1 ratings)    Overview: A reclusive scientist builds a robot that looks exactly like him to go on a long term space mission. Since the scientist seems to lack all human emotion he is unable to program them into his android and an eccentric woman is hired to...

 

    Omega Doom (1996-01-01T00:00:00Z)    Director: Albert Pyun    Top cast: Rutger Hauer, Shannon Whirry, Norbert Weisser, Tina Cote, Anna Katarina    Overview: After earth is taken over by an army of robots, the small number of humans left are forced into hiding. In the nuclear winter, only droids walk the face of the earth, in fear of the rumored human resurgence, and in search of a hidden...
```