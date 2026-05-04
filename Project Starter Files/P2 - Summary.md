# Capstone Project: Semantic Movie Search Engine

## What you're building

A data pipeline that takes messy movie data, cleans and enriches it, loads it into a vector database, and lets users search for movies using natural language descriptions.

Example: searching `"movies about lonely robots finding friendship"` should return things like *WALL-E* and *The Iron Giant* — even though those exact words don't appear in the movie data.

## The big picture

```
Messy CSVs (bronze)
    ↓ Spark: clean
Silver Parquet
    ↓ Spark: enrich + join
Gold Parquet
    ↓ Python: bulk load
Weaviate (local, in Docker)
    ↓ Python CLI
Search results
```

Everything runs locally on your machine. No cloud.

## The data

You'll get four messy CSV files derived from the TMDB dataset:

- **movies_metadata** — main movie info (split across multiple files)
- **credits** — cast and crew (stringified JSON)
- **keywords** — plot keywords (stringified JSON)
- **ratings_small** — user ratings; aggregate to per-movie avg rating and rating count

**Stretch goal files** (optional):
- **links** and **links_small** — map TMDB IDs to IMDB IDs, useful for fetching additional movie data from external APIs (e.g., OMDB) to enrich your text for better embeddings

You'll get a small **practice dataset** to develop against. The full dataset arrives a day or two before the demo. Same kinds of issues, just at scale — your pipeline should handle it without code changes.

## What your pipeline does

### 1. Ingest + Clean (Bronze → Silver)
- Read the messy CSVs
- Handle nulls, duplicates, broken JSON, encoding issues, bad dates, invalid numerics
- Make your own decisions about drop-vs-impute and document them
- Output cleaned Parquet

### 2. Enrich (Silver → Gold)
- Join movies + credits + keywords + ratings (aggregated)
- Extract useful fields from JSON (top cast, director, keyword list, genre list)
- Build a combined text field for embedding (title + tagline + overview + cast + keywords)
- Output gold-layer Parquet

### 3. Load to Weaviate
- Spin up Weaviate locally via Docker Compose (with the `text2vec-transformers` module)
- Define your schema
- Bulk-load gold Parquet into Weaviate
- Weaviate generates embeddings automatically — you don't need to call any embedding API yourself

### 4. Search
- A CLI script: `python search.py "your query here"`
- Returns top matches with title, year, brief description

## The presentation

You'll get the real dataset a day or two before. Run your pipeline against it ahead of time. The presentation covers:

- Your architecture
- Cleaning decisions and tradeoffs
- What happened when you ran on the full dataset (what worked, what surprised you)
- **Live search demo** with queries you've prepared
- Q&A

You won't run the full pipeline live during the presentation — just the search portion.