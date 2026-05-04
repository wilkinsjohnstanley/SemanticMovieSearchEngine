# Data Files Reference

This document describes each CSV file in the dataset and its columns.

---

## movies_metadata.csv

The main movie information file. Each row represents one movie.

**Columns:**

| Column | Description |
|--------|-------------|
| `id` | TMDB movie ID (primary key) |
| `imdb_id` | IMDB ID (e.g., `tt0114709`) |
| `title` | Display title |
| `original_title` | Title in the original language |
| `original_language` | ISO 639-1 language code (e.g., `en`, `fr`) |
| `overview` | Plot summary text |
| `tagline` | Marketing tagline |
| `release_date` | Release date |
| `runtime` | Length in minutes |
| `budget` | Production budget in USD |
| `revenue` | Box office revenue in USD |
| `popularity` | TMDB popularity score |
| `vote_average` | Average user rating (TMDB) |
| `vote_count` | Number of votes (TMDB) |
| `genres` | Stringified JSON array of genre objects |
| `production_companies` | Stringified JSON array of production company objects |
| `production_countries` | Stringified JSON array of country objects |
| `spoken_languages` | Stringified JSON array of language objects |
| `belongs_to_collection` | Stringified JSON object describing collection membership (e.g., "Toy Story Collection") |
| `homepage` | Official movie URL |
| `poster_path` | Path to poster image on TMDB CDN |
| `backdrop_path` | Path to backdrop image on TMDB CDN |
| `status` | Release status (Released, Post Production, etc.) |
| `adult` | Boolean flag for adult content |
| `video` | Boolean flag |

---

## credits.csv

Cast and crew information per movie.

**Columns:**

| Column | Description |
|--------|-------------|
| `id` | TMDB movie ID (joins to `movies_metadata.id`) |
| `cast` | Stringified JSON array of cast member objects (name, character, order, etc.) |
| `crew` | Stringified JSON array of crew member objects (name, job, department, etc.) |

The `cast` array is ordered by importance (lower `order` = more prominent). The `crew` array contains everyone from director to caterer — filter by `job` field to find specific roles.

---

## keywords.csv

Plot keywords per movie.

**Columns:**

| Column | Description |
|--------|-------------|
| `id` | TMDB movie ID (joins to `movies_metadata.id`) |
| `keywords` | Stringified JSON array of keyword objects (e.g., `{'id': 931, 'name': 'jealousy'}`) |

---

## ratings_small.csv

User ratings from MovieLens. ~100K ratings.

**Columns:**

| Column | Description |
|--------|-------------|
| `userId` | Anonymous user ID |
| `movieId` | MovieLens movie ID — **does not directly join to TMDB IDs**, use `links_small` to map |
| `rating` | Numeric rating (typically 0.5 to 5.0 in 0.5 increments) |
| `timestamp` | Unix epoch seconds |

---

## links_small.csv

Maps MovieLens IDs to TMDB and IMDB IDs. Required to join `ratings_small` with `movies_metadata`.

**Columns:**

| Column | Description |
|--------|-------------|
| `movieId` | MovieLens movie ID (joins to `ratings_small.movieId`) |
| `imdbId` | IMDB ID (numeric portion only, prepend `tt` and pad to get the full IMDB ID) |
| `tmdbId` | TMDB movie ID (joins to `movies_metadata.id`) |

---

## links.csv

Larger version of `links_small`. Same structure, more rows. Used for stretch goal API enrichment.

**Columns:** Same as `links_small.csv`.

---

## ratings.csv

Full MovieLens ratings file (~26M rows). **Not used in this project** — too large for the scope. Listed here only so you know it exists in the source dataset.