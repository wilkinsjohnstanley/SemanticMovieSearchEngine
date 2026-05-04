# Data Corruption Reference

This document describes every type of data corruption your pipeline must handle. Each section explains what the corruption looks like, why it happens in real-world data, and gives concrete examples drawn from the practice dataset.

Your cleaning logic must handle all of these. The full dataset will contain the same corruption types — possibly in slightly different forms.

---

## 1. Null Values

Missing values where data should exist. Some fields are critical (the row is unusable without them), others are not.

**What it looks like:**
- A row where `overview` is missing — you can't generate an embedding without text
- A row where `tagline` is missing — annoying but not fatal
- A row where `runtime` is missing — you can still display the movie, just without runtime
- A row where `release_date` is missing — affects sorting and date-based features

**Why it happens:**
Source systems frequently have incomplete data. Movies in early production might not have a runtime yet. Some movies never had taglines. Data entry is inconsistent.

**Example:**
```
id   | title       | overview                  | tagline       | runtime
862  | Toy Story   | Led by Woody, Andy's...   | (null)        | 81
8844 | Jumanji     | (null)                    | (null)        | (null)
```

---

## 2. Exact Duplicate Rows

The same row appearing multiple times.

**What it looks like:**
Two rows with identical values across every field.

**Why it happens:**
Data ingestion errors, repeated imports, joins gone wrong upstream.

**Example:**
```
id   | title      | overview                | release_date
862  | Toy Story  | Led by Woody, Andy's... | 1995-10-30
862  | Toy Story  | Led by Woody, Andy's... | 1995-10-30
```

---

## 3. Near-Duplicate Rows

Rows that represent the same entity but differ in superficial ways — whitespace, casing, punctuation.

**What it looks like:**
- `"Toy Story"` vs `"  Toy Story  "` (leading/trailing whitespace)
- `"Toy Story"` vs `"TOY STORY"` (casing)
- `"Toy Story"` vs `"Toy Story."` (trailing punctuation)

**Why it happens:**
Data entered by different people, merged from different sources, or processed by different upstream systems with different normalization rules.

**Example:**
```
id   | title         | overview
862  | Toy Story     | Led by Woody, Andy's...
862  |   Toy Story   | Led by Woody, Andy's...
862  | TOY STORY     | Led by Woody, Andy's...
```

---

## 4. Mixed Date Formats

Dates stored in inconsistent formats within the same column.

**What it looks like:**
- `1995-10-30` (ISO format)
- `10/30/1995` (US format)
- `30-10-1995` (European format)
- `1995.10.30`
- `October 30, 1995`

**Why it happens:**
Data merged from international sources, manual entry by people in different locales, exports from different software.

**Example:**
```
title       | release_date
Toy Story   | 1995-10-30
Jumanji     | 12/15/1995
GoldenEye   | 17-11-1995
Casino      | November 22, 1995
```

---

## 5. Impossible Dates

Dates that are technically parseable but don't make sense.

**What it looks like:**
- `0000-00-00` (year zero, often indicates "unknown")
- `2150-01-01` (far future — no movie was released that year yet)
- `1492-10-12` (predates cinema)

**Why it happens:**
Placeholder values for unknown dates, data entry errors, off-by-many bugs in upstream systems.

**Example:**
```
title             | release_date
Toy Story         | 1995-10-30
Mystery Movie     | 0000-00-00
Future Project    | 2087-06-15
```

---

## 6. Negative or Impossible Numeric Values

Numbers that are technically valid integers but don't make sense for the field.

**What it looks like:**
- `runtime = -90` (a movie can't have negative length)
- `runtime = 0` (a movie with zero runtime isn't a movie)
- `runtime = 99999` (a 70-day movie doesn't exist)
- `budget = -1`

**Why it happens:**
Placeholder values for "unknown" (people sometimes use -1 or 0), data entry errors, integer overflow.

**Example:**
```
title       | runtime
Toy Story   | 81
Jumanji     | -1
GoldenEye   | 99999
```

---

## 7. Numeric Values as Currency-Formatted Strings

Numbers stored as formatted strings instead of actual numbers.

**What it looks like:**
- `"$30,000,000"` instead of `30000000`
- `"€1.500.000"` instead of `1500000`
- `"30,000,000.00 USD"`

**Why it happens:**
Data exported from spreadsheets where formatting was applied for display, scraped from web pages where numbers appear formatted.

**Example:**
```
title       | budget
Toy Story   | 30000000
Jumanji     | $65,000,000
GoldenEye   | 60000000
```

---

## 8. Stringified JSON with Various Corruption

The TMDB dataset stores arrays of objects (genres, cast, crew, keywords) as Python-style stringified JSON inside CSV cells. The corruption introduces several types of breakage.

**What clean values look like:**
```
"[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}]"
```

**Note:** TMDB uses single quotes, which is not valid JSON. This is the normal state of the data, not a corruption — but it's a parsing concern you'll need to address.

**Types of corruption you'll see:**

### 8a. Truncated
The JSON string is cut off partway through.
```
"[{'id': 16, 'name': 'Anim"
```

### 8b. Missing brackets
The opening or closing brackets are stripped entirely.
```
"{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}"
```

---

## 9. Empty Strings Where Empty Arrays Are Expected

A field that should contain `"[]"` (empty array) instead contains `""` (empty string) or null.

**What it looks like:**
```
id   | cast       | crew
862  | "[{...}]"  | "[{...}]"
8844 |            | "[]"
```

**Why it happens:**
Inconsistent handling of "no data" cases — some systems write empty arrays, others write nulls or empty strings.

---

## 10. Mojibake / Encoding Artifacts

Text that was encoded in one character set and decoded as another, producing garbled characters.

**What it looks like:**
- `"Amélie"` becomes `"AmÃ©lie"`
- `"naïve"` becomes `"naÃ¯ve"`
- `"don't"` (curly apostrophe) becomes `"donâ€™t"`

**Why it happens:**
Text saved as UTF-8 but read as Latin-1, or vice versa. Common when data passes through multiple systems with different default encodings.

**Example:**
```
title                          | overview
Amélie                         | A shy waitress decides to...
AmÃ©lie                        | A shy waitress decides to...
The KingÃ¢â‚¬â„¢s Speech       | The story of King George VI...
```

---

## 11. Orphan IDs

Rows in `credits`, `keywords`, or `ratings` that reference a movie ID that doesn't exist in `movies_metadata`. Or movies in `movies_metadata` with no matching enrichment data.

**What it looks like:**

`movies_metadata`:
```
id   | title
862  | Toy Story
8844 | Jumanji
```

`credits`:
```
id        | cast
862       | "[{...}]"
8844      | "[{...}]"
9999999   | "[{...}]"   ← this movie doesn't exist in movies_metadata
```

**Why it happens:**
Data integrity issues upstream — referential integrity wasn't enforced, deletes happened in one table but not another, IDs got mistyped.