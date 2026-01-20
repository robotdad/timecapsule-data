# Internet Archive Index Database

This document describes the SQLite database schema used to track Internet Archive items for the TimeCapsule corpus.

## Overview

The IA index database (`ia_index_*.db`) stores metadata for items discovered from Internet Archive, tracks download progress, and enables efficient querying for the download pipeline.

## Schema

### Items Table

```sql
CREATE TABLE items (
    identifier TEXT PRIMARY KEY,    -- IA identifier (e.g., "thegraborneadven00adam")
    title TEXT,                      -- JSON array of titles
    date TEXT,                       -- Publication date string
    year INTEGER,                    -- Extracted year (for filtering/sorting)
    creator TEXT,                    -- JSON array of authors/creators
    publisher TEXT,                  -- JSON array of publishers
    subject TEXT,                    -- JSON array of subjects
    description TEXT,                -- JSON array of descriptions
    format TEXT,                     -- JSON array of file formats available
    imagecount INTEGER,              -- Page count (used as quality proxy)
    downloads INTEGER,               -- IA download count
    contributor TEXT,                -- Digitization contributor
    scanner TEXT,                    -- Scanner used for digitization
    rights TEXT,                     -- Rights statement
    licenseurl TEXT,                 -- License URL if applicable
    call_number TEXT,                -- Library call number
    isbn TEXT,                       -- ISBN if available
    issn TEXT,                       -- ISSN if available
    lccn TEXT,                       -- Library of Congress Control Number
    publicdate TEXT,                 -- When item was made public on IA
    addeddate TEXT,                  -- When item was added to IA
    collection TEXT,                 -- JSON array of IA collections
    quality_score REAL,              -- Computed quality score (0.0-1.0)
    text_filename TEXT,              -- Discovered text filename (e.g., "item_djvu.txt")
    enriched_at TEXT,                -- ISO timestamp when metadata was enriched
    downloaded_at TEXT,              -- ISO timestamp when text was downloaded
    download_failed_at TEXT          -- ISO timestamp if download failed
);
```

### JSON Fields

Several fields store JSON arrays because IA metadata can have multiple values:
- `title`, `creator`, `publisher`, `subject`, `description`, `format`, `collection`

Example:
```python
import json
titles = json.loads(row['title'])  # Returns list like ["The Great Adventure", "A Novel"]
```

### Quality Score

The `quality_score` field (0.0-1.0) is computed during indexing based on:
- Presence of required metadata (title, date, creator)
- Page count (`imagecount`) - more pages generally means real books vs pamphlets
- OCR quality indicators from IA metadata

Recommended minimum for corpus building: `0.65`

## Indexes

### Required Indexes

These indexes are essential for reasonable performance:

```sql
-- Primary lookup
-- (automatic via PRIMARY KEY on identifier)

-- Quality filtering
CREATE INDEX idx_quality ON items(quality_score);

-- Year-based sorting/filtering
CREATE INDEX idx_year ON items(year);

-- Download status tracking
CREATE INDEX idx_downloaded ON items(downloaded_at);
```

### Performance Indexes

These indexes significantly speed up the download pipeline:

```sql
-- Optimized for download queue query
-- Covers: WHERE downloaded_at IS NULL AND quality_score >= X ORDER BY year
CREATE INDEX idx_download_queue ON items(quality_score, year) 
WHERE downloaded_at IS NULL;

-- Further optimized for specific quality/imagecount thresholds
-- Adjust the threshold values to match your --min-quality and --min-imagecount settings
CREATE INDEX idx_download_queue_v2 ON items(year, identifier) 
WHERE downloaded_at IS NULL 
  AND quality_score >= 0.65 
  AND imagecount >= 10;
```

### Optional Indexes

```sql
-- If doing enrichment passes
CREATE INDEX idx_enriched ON items(enriched_at);

-- If querying by known filename
CREATE INDEX idx_text_filename ON items(text_filename) 
WHERE text_filename IS NOT NULL;
```

## Setup for New Database

### Creating from Scratch

If building a new index database:

```sql
-- Create table
CREATE TABLE items (
    identifier TEXT PRIMARY KEY,
    title TEXT,
    date TEXT,
    year INTEGER,
    creator TEXT,
    publisher TEXT,
    subject TEXT,
    description TEXT,
    format TEXT,
    imagecount INTEGER,
    downloads INTEGER,
    contributor TEXT,
    scanner TEXT,
    rights TEXT,
    licenseurl TEXT,
    call_number TEXT,
    isbn TEXT,
    issn TEXT,
    lccn TEXT,
    publicdate TEXT,
    addeddate TEXT,
    collection TEXT,
    quality_score REAL,
    text_filename TEXT,
    enriched_at TEXT,
    downloaded_at TEXT,
    download_failed_at TEXT
);

-- Required indexes
CREATE INDEX idx_quality ON items(quality_score);
CREATE INDEX idx_year ON items(year);
CREATE INDEX idx_downloaded ON items(downloaded_at);

-- Performance indexes (adjust thresholds as needed)
CREATE INDEX idx_download_queue ON items(quality_score, year) 
WHERE downloaded_at IS NULL;

CREATE INDEX idx_download_queue_v2 ON items(year, identifier) 
WHERE downloaded_at IS NULL 
  AND quality_score >= 0.65 
  AND imagecount >= 10;
```

### Adding Indexes to Existing Database

If you have an existing database without indexes:

```bash
sqlite3 your_database.db "CREATE INDEX idx_quality ON items(quality_score);"
sqlite3 your_database.db "CREATE INDEX idx_year ON items(year);"
sqlite3 your_database.db "CREATE INDEX idx_downloaded ON items(downloaded_at);"
sqlite3 your_database.db "CREATE INDEX idx_download_queue ON items(quality_score, year) WHERE downloaded_at IS NULL;"
sqlite3 your_database.db "CREATE INDEX idx_download_queue_v2 ON items(year, identifier) WHERE downloaded_at IS NULL AND quality_score >= 0.65 AND imagecount >= 10;"
```

Note: Index creation on large databases (millions of rows) may take several minutes.

## Performance Considerations

### WSL2 + Windows Filesystem

If running on WSL2 with the database on a Windows drive (`/mnt/c/`, `/mnt/d/`, etc.), expect significantly slower performance due to filesystem translation overhead.

**Recommendations:**
- Copy database to Linux filesystem (`/home/user/`) for long operations
- Use WAL mode for better concurrent access
- Increase busy timeout for write operations

```sql
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=60000;  -- 60 seconds
PRAGMA synchronous=NORMAL;
```

### Large Query Optimization

The download tool's main query:
```sql
SELECT identifier, text_filename FROM items
WHERE quality_score >= ?
  AND imagecount >= ?
  AND downloaded_at IS NULL
ORDER BY year ASC, identifier ASC
```

This benefits from `idx_download_queue_v2` when thresholds match. If using different thresholds, create a matching partial index.

## Common Operations

### Check Download Progress

```sql
-- Total items
SELECT COUNT(*) FROM items;

-- Downloaded
SELECT COUNT(*) FROM items WHERE downloaded_at IS NOT NULL;

-- Failed
SELECT COUNT(*) FROM items WHERE download_failed_at IS NOT NULL;

-- Pending (eligible for download)
SELECT COUNT(*) FROM items 
WHERE downloaded_at IS NULL 
  AND download_failed_at IS NULL
  AND quality_score >= 0.65 
  AND imagecount >= 10;
```

### Reset Failed Downloads

```sql
-- Allow retry of failed items
UPDATE items SET download_failed_at = NULL WHERE download_failed_at IS NOT NULL;
```

### Export Statistics by Year

```sql
SELECT year, 
       COUNT(*) as total,
       SUM(CASE WHEN downloaded_at IS NOT NULL THEN 1 ELSE 0 END) as downloaded
FROM items 
WHERE year IS NOT NULL
GROUP BY year 
ORDER BY year;
```

## Tools

| Tool | Description |
|------|-------------|
| `tc-ia-download` | Download texts from items in the index |
| `tc-ia-index` | Build/update index from IA search results |

See `--help` on each tool for usage details.
