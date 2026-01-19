#!/usr/bin/env python3
"""
Internet Archive Index Builder (SQLite)

Builds a complete catalog of all IA items in a date range using the Scraping API.
Stores directly to SQLite database for efficient querying and updates.

The Scraping API uses cursor-based pagination with NO result limits, allowing us
to retrieve all 2.3M+ items without hitting the 10k pagination wall.

Usage:
    tc-ia-index --year-start 1800 --year-end 1914 -o /path/to/corpus/

Creates: corpus/metadata/ia_index_1800_1914.db

Features:
- Direct SQLite storage (no intermediate JSON)
- Resume support (continues from last cursor)
- Instant commits after each batch
- ~75% smaller than JSON
"""

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def create_schema(conn: sqlite3.Connection):
    """Create the SQLite schema for IA items."""
    cursor = conn.cursor()

    # Main items table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS items (
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
            enriched_at TEXT
        )
    """)

    # Indexes for common queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_quality ON items(quality_score)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_year ON items(year)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_enriched ON items(enriched_at)")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_text_filename ON items(text_filename) WHERE text_filename IS NOT NULL"
    )

    # Metadata table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS index_metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # Resume state table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS resume_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            cursor TEXT,
            last_batch_at TEXT
        )
    """)

    conn.commit()


def serialize_field(value):
    """Serialize field for storage (lists/dicts as JSON)."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    if isinstance(value, str):
        return value
    return str(value)


def scrape_batch(query, fields, cursor=None, count=10000, timeout=300):
    """
    Fetch a batch using IA's Scraping API (cursor-based, no 10k limit).

    Returns:
        dict with 'items', 'total', 'count', 'cursor' (or None on error)
    """
    params = {
        "q": query,
        "fields": fields,
        "count": count,
    }
    if cursor:
        params["cursor"] = cursor

    url = f"https://archive.org/services/search/v1/scrape?{urlencode(params)}"

    time.sleep(2)  # Rate limit

    try:
        req = Request(url, headers={"User-Agent": "TimeCapsuleLLM-Research/1.0"})
        with urlopen(req, timeout=timeout) as response:
            data = json.loads(response.read().decode("utf-8"))
            return data
    except (HTTPError, URLError, TimeoutError) as e:
        print(f"  Error: {e}")
        return None


# Quality collections (same as enrichment)
QUALITY_COLLECTIONS = {
    "americana": 0.9,
    "library_of_congress": 0.9,
    "toronto": 0.85,
    "europeanlibraries": 0.85,
    "jstor": 0.85,
    "blc": 0.8,
    "bplscas": 0.8,
    "biodiversity": 0.8,
    "medicalheritage": 0.8,
    "digitallibraryindia": 0.75,
    "newspaper": 0.7,
    "internetarchivebooks": 0.65,
    "jaigyan": 0.65,
    "journal": 0.65,
    "opensource": 0.5,
    "folkscanomy": 0.5,
}


def calculate_quality_score(collections):
    """Calculate quality score based on collections."""
    if not collections:
        return 0.5

    if isinstance(collections, str):
        collections = [collections]

    best_score = 0.5
    for coll in collections:
        coll_lower = coll.lower()
        for known, score in QUALITY_COLLECTIONS.items():
            if known in coll_lower:
                best_score = max(best_score, score)

    return best_score


def build_index(year_start, year_end, output_dir, batch_size=10000):
    """
    Build complete IA index for date range using cursor-based scraping.
    Stores directly to SQLite with resume support.
    """
    # Build query
    query = (
        f"date:[{year_start} TO {year_end}] "
        f"AND mediatype:texts "
        f"AND language:eng "
        f'AND (format:DjVu OR format:Text OR format:"Abbyy GZ")'
    )

    # Request fields
    fields = ",".join(
        [
            "identifier",
            "title",
            "date",
            "year",
            "creator",
            "publisher",
            "subject",
            "collection",
            "description",
            "format",
            "imagecount",
            "downloads",
            "contributor",
            "scanner",
            "rights",
            "licenseurl",
            "call_number",
            "isbn",
            "issn",
            "lccn",
            "publicdate",
            "addeddate",
        ]
    )

    start_time = datetime.now()

    print("=" * 80)
    print("INTERNET ARCHIVE INDEX BUILDER (SQLite)")
    print("=" * 80)
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Date range: {year_start}-{year_end}")
    print(f"Query: {query}")
    print()

    # Prepare database path
    index_dir = Path(output_dir) / "metadata"
    index_dir.mkdir(parents=True, exist_ok=True)
    db_path = index_dir / f"ia_index_{year_start}_{year_end}.db"

    print(f"Database: {db_path}")
    print()

    # Connect to database
    conn = sqlite3.Connection(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")

    create_schema(conn)
    cursor = conn.cursor()

    # Check for existing data (resume support)
    cursor.execute("SELECT COUNT(*) FROM items")
    existing_count = cursor.fetchone()[0]

    cursor.execute("SELECT cursor, last_batch_at FROM resume_state WHERE id = 1")
    resume_row = cursor.fetchone()
    resume_cursor = resume_row[0] if resume_row else None

    if existing_count > 0:
        print(f"RESUMING: Found {existing_count:,} existing items")
        if resume_cursor:
            print(f"  Last cursor: {resume_cursor[:50]}...")
            print(f"  Last batch: {resume_row[1]}")
        print()
    else:
        # Store metadata for new index
        cursor.execute(
            "INSERT OR REPLACE INTO index_metadata (key, value) VALUES (?, ?)",
            ("query", query),
        )
        cursor.execute(
            "INSERT OR REPLACE INTO index_metadata (key, value) VALUES (?, ?)",
            ("date_range_start", str(year_start)),
        )
        cursor.execute(
            "INSERT OR REPLACE INTO index_metadata (key, value) VALUES (?, ?)",
            ("date_range_end", str(year_end)),
        )
        cursor.execute(
            "INSERT OR REPLACE INTO index_metadata (key, value) VALUES (?, ?)",
            ("created_at", datetime.now().isoformat()),
        )
        conn.commit()

    # First request (or resume from cursor)
    print("Fetching batch to determine total size...")
    first_batch = scrape_batch(query, fields, cursor=resume_cursor, count=batch_size)

    if not first_batch:
        print("Failed to fetch batch")
        return None

    total_found = first_batch.get("total", 0)
    items = first_batch.get("items", [])
    next_cursor = first_batch.get("cursor")

    print(f"  Total items in IA: {total_found:,}")
    print(f"  Already have: {existing_count:,}")
    print(f"  Remaining: {total_found - existing_count:,}")
    print(f"  Batch size: {batch_size:,}")
    print()

    if total_found == 0:
        print("No items found matching query")
        return None

    # Insert first batch
    batch_data = []
    for item in items:
        collections = item.get("collection", [])
        quality_score = calculate_quality_score(collections)

        row = (
            item.get("identifier"),
            serialize_field(item.get("title")),
            serialize_field(item.get("date")),
            item.get("year"),
            serialize_field(item.get("creator")),
            serialize_field(item.get("publisher")),
            serialize_field(item.get("subject")),
            serialize_field(item.get("description")),
            serialize_field(item.get("format")),
            item.get("imagecount"),
            item.get("downloads"),
            serialize_field(item.get("contributor")),
            serialize_field(item.get("scanner")),
            serialize_field(item.get("rights")),
            serialize_field(item.get("licenseurl")),
            serialize_field(item.get("call_number")),
            serialize_field(item.get("isbn")),
            serialize_field(item.get("issn")),
            serialize_field(item.get("lccn")),
            serialize_field(item.get("publicdate")),
            serialize_field(item.get("addeddate")),
            serialize_field(collections),
            quality_score,
            None,  # text_filename
            None,  # enriched_at
        )
        batch_data.append(row)

    cursor.executemany(
        """
        INSERT OR IGNORE INTO items (
            identifier, title, date, year, creator, publisher, subject,
            description, format, imagecount, downloads, contributor, scanner,
            rights, licenseurl, call_number, isbn, issn, lccn, publicdate,
            addeddate, collection, quality_score, text_filename, enriched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        batch_data,
    )

    # Update resume state
    cursor.execute(
        "INSERT OR REPLACE INTO resume_state (id, cursor, last_batch_at) VALUES (1, ?, ?)",
        (next_cursor, datetime.now().isoformat()),
    )

    conn.commit()

    # Update count
    cursor.execute("SELECT COUNT(*) FROM items")
    current_count = cursor.fetchone()[0]

    batch_num = 1
    batch_start_time = time.time()

    print("Scraping with cursor-based pagination...")
    print()

    # Continue fetching
    while next_cursor:
        batch_num += 1
        batch = scrape_batch(query, fields, cursor=next_cursor, count=batch_size)

        if not batch:
            print(f"  Failed to fetch batch {batch_num}, stopping")
            break

        items = batch.get("items", [])
        next_cursor = batch.get("cursor")

        if not items:
            print(f"  Batch {batch_num} returned 0 items - stopping")
            break

        # Insert batch
        batch_data = []
        for item in items:
            collections = item.get("collection", [])
            quality_score = calculate_quality_score(collections)

            row = (
                item.get("identifier"),
                serialize_field(item.get("title")),
                serialize_field(item.get("date")),
                item.get("year"),
                serialize_field(item.get("creator")),
                serialize_field(item.get("publisher")),
                serialize_field(item.get("subject")),
                serialize_field(item.get("description")),
                serialize_field(item.get("format")),
                item.get("imagecount"),
                item.get("downloads"),
                serialize_field(item.get("contributor")),
                serialize_field(item.get("scanner")),
                serialize_field(item.get("rights")),
                serialize_field(item.get("licenseurl")),
                serialize_field(item.get("call_number")),
                serialize_field(item.get("isbn")),
                serialize_field(item.get("issn")),
                serialize_field(item.get("lccn")),
                serialize_field(item.get("publicdate")),
                serialize_field(item.get("addeddate")),
                serialize_field(collections),
                quality_score,
                None,
                None,
            )
            batch_data.append(row)

        cursor.executemany(
            """
            INSERT OR IGNORE INTO items (
                identifier, title, date, year, creator, publisher, subject,
                description, format, imagecount, downloads, contributor, scanner,
                rights, licenseurl, call_number, isbn, issn, lccn, publicdate,
                addeddate, collection, quality_score, text_filename, enriched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            batch_data,
        )

        # Update resume state
        cursor.execute(
            "INSERT OR REPLACE INTO resume_state (id, cursor, last_batch_at) VALUES (1, ?, ?)",
            (next_cursor, datetime.now().isoformat()),
        )

        conn.commit()

        # Update count
        cursor.execute("SELECT COUNT(*) FROM items")
        current_count = cursor.fetchone()[0]

        # Progress reporting
        if batch_num % 10 == 0 or batch_num <= 5:
            pct = current_count / total_found * 100 if total_found > 0 else 0
            db_size_mb = db_path.stat().st_size / 1024 / 1024
            remaining = total_found - current_count
            elapsed = time.time() - batch_start_time
            rate = (current_count - existing_count) / elapsed if elapsed > 0 else 0
            eta_sec = remaining / rate if rate > 0 else 0
            eta_min = eta_sec / 60

            print(
                f"  Batch {batch_num} - got {len(items)} items - "
                f"TOTAL: {current_count:,}/{total_found:,} ({pct:.1f}%) - "
                f"{db_size_mb:.1f} MB - ETA: {eta_min:.0f}m"
            )

        # Safety: stop if we've gotten everything
        if current_count >= total_found:
            break

    print()
    print(f"✓ Index complete: {current_count:,} items")

    # Final metadata update
    cursor.execute(
        "INSERT OR REPLACE INTO index_metadata (key, value) VALUES (?, ?)",
        ("total_found", str(total_found)),
    )
    cursor.execute(
        "INSERT OR REPLACE INTO index_metadata (key, value) VALUES (?, ?)",
        ("total_exported", str(current_count)),
    )
    cursor.execute(
        "INSERT OR REPLACE INTO index_metadata (key, value) VALUES (?, ?)",
        ("completed_at", datetime.now().isoformat()),
    )

    conn.commit()

    # Summary statistics
    print()
    print("=" * 80)
    print("INDEX ANALYSIS")
    print("=" * 80)

    # Year distribution
    cursor.execute("SELECT MIN(year), MAX(year) FROM items WHERE year IS NOT NULL")
    year_range = cursor.fetchone()
    cursor.execute("SELECT COUNT(*) FROM items WHERE year IS NOT NULL")
    with_year = cursor.fetchone()[0]

    if year_range[0]:
        print(f"Year range: {year_range[0]} - {year_range[1]}")
        print(f"Items with year: {with_year:,} / {current_count:,}")
    print()

    # Quality distribution
    thresholds = [0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9]
    print("Quality score distribution (collection-based heuristic):")
    for threshold in thresholds:
        cursor.execute("SELECT COUNT(*) FROM items WHERE quality_score >= ?", (threshold,))
        count_above = cursor.fetchone()[0]
        pct = count_above / current_count * 100 if current_count > 0 else 0
        print(f"  >= {threshold:.2f}: {count_above:7,} items ({pct:5.1f}%)")

    print()
    db_size_mb = db_path.stat().st_size / 1024 / 1024
    end_time = datetime.now()
    duration = end_time - start_time
    duration_minutes = duration.total_seconds() / 60

    print(f"✓ Database: {db_path}")
    print(f"✓ Size: {db_size_mb:.1f} MB")
    print()
    print(f"Started:  {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Ended:    {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration_minutes:.1f} minutes ({duration.total_seconds():.0f} seconds)")

    conn.close()
    return db_path


def main():
    parser = argparse.ArgumentParser(
        description="Build IA item index using cursor-based Scraping API (SQLite)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build index for 1800-1914
  tc-ia-index --year-start 1800 --year-end 1914 -o /path/to/corpus

  # Resume interrupted index build (automatic)
  tc-ia-index --year-start 1800 --year-end 1914 -o /path/to/corpus

Notes:
  - Builds SQLite database directly (no JSON)
  - Automatic resume support if interrupted
  - ~75% smaller than JSON equivalent
  - Can retrieve ALL 2.3M+ items (no 10k limit)
        """,
    )

    parser.add_argument("-o", "--output", required=True, help="Output directory (corpus base)")
    parser.add_argument("--year-start", type=int, default=1800, help="Start year (default: 1800)")
    parser.add_argument("--year-end", type=int, default=1914, help="End year (default: 1914)")
    parser.add_argument(
        "--batch-size", type=int, default=10000, help="Items per request (default: 10000, min: 100)"
    )

    args = parser.parse_args()

    # Validate batch size
    if args.batch_size < 100:
        print("Error: batch-size must be at least 100")
        sys.exit(1)

    # Build index
    result = build_index(args.year_start, args.year_end, args.output, args.batch_size)

    if not result:
        sys.exit(1)


if __name__ == "__main__":
    main()
