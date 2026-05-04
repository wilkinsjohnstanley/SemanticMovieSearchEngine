# Capstone Project Specification: Semantic Movie Search Engine

## Overview

Build a local data pipeline that ingests messy movie data, cleans and enriches it through a medallion architecture (bronze → silver → gold), loads the result into a vector database, and exposes a CLI for natural-language semantic search.

This document is the authoritative spec. Refer here for requirements, milestones, and definition of done. Refer to the project summary doc for the high-level overview.

---

## Project Requirements

### Functional Requirements

1. The pipeline must ingest the provided messy CSVs (movies_metadata, credits, keywords) without manual preprocessing.
2. The pipeline must produce a silver layer of cleaned, validated Parquet data for each entity.
3. The pipeline must produce a gold layer of enriched, joined Parquet data with a combined text field suitable for semantic embedding.
4. The pipeline must load the gold layer into a local Weaviate instance configured with the `text2vec-transformers` module.
5. The pipeline must expose a CLI script that accepts a natural-language query and returns semantically relevant movie matches.
6. The pipeline must be re-runnable end-to-end on a new dataset of the same shape without code changes.

### Technical Requirements

- **Spark** must be used for all data transformation work (ingest, clean, enrich, join, feature engineer).
- **Weaviate** runs locally via Docker Compose. The `text2vec-transformers` module handles all embedding generation.
- **Parquet** is the storage format for silver and gold layers.
- **Each pipeline stage is its own script** that can be run independently.
- **File paths must be configurable** (no hardcoded paths to specific machines or directories).

### Documentation Requirements

A `README.md` must be included that covers:
- How to set up the environment (dependencies, Docker, etc.)
- How to run each pipeline stage
- The schema of the silver and gold layers
- Cleaning policies and the reasoning behind them (e.g., "we dropped rows with null overviews because they cannot be embedded")
- Known limitations or edge cases not handled
- Example search queries and results

---

## Data

You will receive:

- A **practice dataset** at the start of the project. Small, curated to include at least one example of every corruption type your pipeline must handle.
- A **full dataset** delivered a day or two before the demo. Same corruption types, larger scale, more variety.

Both datasets contain four required files:
- `movies_metadata` (split across multiple files)
- `credits`
- `keywords`
- `ratings_small` — user ratings, to be aggregated per movie

Two additional files are provided for the optional stretch goal:
- `links` and `links_small` — map TMDB IDs to IMDB IDs for fetching additional data from external APIs (e.g., OMDB)

### Corruption types your pipeline must handle

- Null values in critical and non-critical fields
- Exact duplicate rows
- Near-duplicate rows (whitespace, casing variations)
- Mixed date formats
- Impossible dates (far future, year 0)
- Negative or impossible numeric values (runtimes, etc.)
- Numeric values stored as currency-formatted strings
- Stringified JSON with various corruption (truncated, swapped quotes, missing brackets)
- Empty strings where empty arrays are expected
- Mojibake / encoding artifacts in text fields
- Orphan IDs in credits/keywords that don't exist in movies_metadata

---

## Milestones

Each milestone corresponds to a pipeline stage. Work through them in order. The last few days are intentionally unscheduled — use them for polish, debugging, running on the full dataset, and preparing the presentation.

### Day 1 — Setup

- Repository created, environment set up
- PySpark installed and verified working
- Practice dataset reviewed and corruption types catalogued
- Every team member can run a "hello world" Spark script

### Day 2-6 — Milestone 1: Ingest + Clean (Bronze → Silver)

- `ingest/ingest.py` reads all messy CSVs from a configurable bronze directory
- All corruption types listed above are handled
- Silver-layer Parquet is written for movies, credits, and keywords
- Cleaning policies are documented in the README
- The script is idempotent (re-runs produce identical output)
- Silver Parquet can be read back and inspected with `df.show()` and `df.printSchema()`

### Day 7-10 — Milestone 2: Enrich (Silver → Gold)

- `enrich/enrich.py` reads silver Parquet and produces gold Parquet
- Movies, credits, keywords, and aggregated ratings are joined (join strategy is a deliberate choice — document it)
- Ratings are aggregated per movie (e.g., avg rating, rating count) before joining
- Useful fields are extracted from the parsed JSON (top cast, director, keyword list, genre list)
- A combined text field is built for embedding (title + tagline + overview + cast + keywords, or your variation)
- Orphan record handling is intentional and documented
- Gold schema is documented in the README

### Day 11-12 — Milestone 3: Weaviate + Search

- Weaviate runs locally via Docker Compose with the `text2vec-transformers` module
- Weaviate schema/collection is defined in code
- `load_weaviate/load_weaviate.py` reads gold Parquet and bulk-loads into Weaviate
- Embeddings are generated by the `text2vec-transformers` module on ingest
- Loading is verified (object count matches expected, sample objects can be retrieved with vector embeddings attached)
- The loader is re-runnable (handles existing data sensibly — either truncates and reloads, or upserts)
- `search/search.py` accepts a natural language query as a CLI argument
- Query is sent to Weaviate using `nearText`
- Top matches are returned with title, year, and a brief overview snippet
- Searches return semantically relevant results (verify against your own curated test queries)
- Edge cases (empty query, no matches, very long query) are handled gracefully

### Day 13-14 — Buffer

Reserved for unexpected delays, running and validating on the full dataset, additional polish, presentation preparation, and final testing. Do not plan new features for these days.

### Day 15 — Presentation

See "Presentation" section below.

---

## Stretch Goals (Optional)

For groups that want to extend the project beyond the core requirements.

### API enrichment via OMDB (or similar)

Use the provided `links` / `links_small` files to look up IMDB IDs for movies, then call an external API (such as OMDB) to fetch additional data — full plot text, awards, critic scores, box office numbers — and merge it into the gold layer.

This exercises:
- Parallel external API calls in Spark via `mapPartitions`
- Rate limit and error handling
- Enrichment patterns common in real-world data engineering

The fetched data should improve search quality by adding more meaningful text to the embedding input.

---

## Project Structure

A suggested layout:

```
project/
├── .venv/
├── bronze/
├── ingest/
│   ├── ingest.py
│   └── silver/
├── enrich/
│   ├── enrich.py
│   └── gold/
├── load_weaviate/
│   ├── load_weaviate.py
│   └── weaviate_data/
├── search/
│   └── search.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

- `bronze/` holds the input messy CSVs
- `ingest/silver/` holds the cleaned Parquet output
- `enrich/gold/` holds the joined/enriched Parquet output
- `load_weaviate/weaviate_data/` is Weaviate's persistent storage, mounted into the Docker container — you never read or write it directly
- `search/` interacts with Weaviate over the network, so it has no data subdirectory

You may organize differently if you prefer (e.g., shared utility modules, config files), but each pipeline stage must remain independently runnable.

---

## Presentation

You will not run the full pipeline live. Run it ahead of time against the full dataset. The presentation covers:

1. **Architecture overview** — your bronze/silver/gold layers, Weaviate setup, search flow
2. **Key cleaning decisions** — what corruption you saw, how you chose to handle it, what tradeoffs you made
3. **What happened on the full dataset** — what worked, what broke, what surprised you, what you fixed
4. **Live search demo** — run several of your prepared queries and discuss the results
5. **Q&A**

Prepare 5-8 curated search queries that demonstrate the strengths and/or weaknesses of your pipeline.

---

## Definition of Done (Project-Level)

The project is complete when all of the following are true:

- All four pipeline stages run successfully end-to-end on the full dataset
- The search CLI returns semantically relevant results for arbitrary natural-language queries
- The README documents setup, execution, schema, and cleaning decisions
- The pipeline is re-runnable on the same data without producing different output
- The pipeline handles all corruption types catalogued in this spec
- The presentation covers all required sections