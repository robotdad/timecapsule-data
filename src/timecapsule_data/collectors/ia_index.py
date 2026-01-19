#!/usr/bin/env python3
"""
Internet Archive Index Builder (SQLite with Time Chunking)

Builds a complete catalog of all IA items in a date range using adaptive time chunking.
Splits large date ranges into manageable chunks (~100k items each) for robust resume support.

The Scraping API uses cursor-based pagination within each chunk. If interrupted, resume
starts from the next incomplete chunk without re-scanning completed time periods.

Usage:
    tc-ia-index --year-start 1800 --year-end 1914 -o /path/to/corpus/

Creates: corpus/metadata/ia_index_1800_1914.db

Features:
- Adaptive time chunking (adjusts granularity based on data density)
- Robust resume (continues from incomplete chunks only)
- Safe to interrupt (atomic commits per chunk)
- No duplicate scanning on resume
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
    """Create the SQLite schema for IA items with chunking support."""
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
            enriched_at TEXT,
            downloaded_at TEXT,
            download_failed_at TEXT
        )
    """)

    # Indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_quality ON items(quality_score)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_year ON items(year)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_enriched ON items(enriched_at)")

    # Metadata table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS index_metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # Time chunks tracking table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS time_chunks (
            chunk_id TEXT PRIMARY KEY,
            year_start INTEGER,
            year_end INTEGER,
            month_start INTEGER,
            month_end INTEGER,
            expected_items INTEGER,
            actual_items INTEGER,
            completed_at TEXT,
            last_attempted_at TEXT
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
    Fetch a batch using IA's Scraping API.

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

    time.sleep(2)

    try:
        req = Request(url, headers={"User-Agent": "TimeCapsuleLLM-Research/1.0"})
        with urlopen(req, timeout=timeout) as response:
            data = json.loads(response.read().decode("utf-8"))
            return data
    except (HTTPError, URLError, TimeoutError) as e:
        print(f"  Error: {e}")
        return None


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


def build_base_query():
    """Build base query without date range."""
    return 'mediatype:texts AND language:eng AND (format:DjVu OR format:Text OR format:"Abbyy GZ")'


def query_count(year_start, year_end, month_start=None, month_end=None):
    """
    Query IA for item count in a time range.

    Returns:
        (count, error_message) - count is None on error
    """
    base = build_base_query()

    if month_start and month_end:
        # Month-level query
        date_query = f"date:[{year_start:04d}-{month_start:02d} TO {year_end:04d}-{month_end:02d}]"
    else:
        # Year-level query
        date_query = f"date:[{year_start} TO {year_end}]"

    query = f"{date_query} AND {base}"

    # Use scrape with count=0 to just get total
    result = scrape_batch(query, "identifier", cursor=None, count=1)

    if not result:
        return None, "API request failed"

    return result.get("total", 0), None


def plan_chunks(year_start, year_end, target_size=100000):
    """
    Plan time chunks adaptively based on data density.

    Returns:
        List of chunk specifications: [{"chunk_id": "...", "year_start": ..., ...}, ...]
    """
    print()
    print("=" * 80)
    print("PLANNING TIME CHUNKS (targeting ~100k items per chunk)")
    print("=" * 80)
    print()

    chunks = []
    current_year = year_start

    while current_year <= year_end:
        # Try increasingly larger ranges until we hit target or year_end
        for span in [50, 25, 10, 5, 1]:
            test_end = min(current_year + span - 1, year_end)

            print(f"  Testing range {current_year}-{test_end}...", end="", flush=True)
            count, error = query_count(current_year, test_end)

            if error:
                print(f" ERROR: {error}")
                continue

            print(f" {count:,} items")

            # If we found a good chunk or hit year_end
            if count is not None and (count <= target_size or test_end == year_end):
                chunk = {
                    "chunk_id": f"{current_year}-{test_end}",
                    "year_start": current_year,
                    "year_end": test_end,
                    "month_start": None,
                    "month_end": None,
                    "expected_items": count,
                }
                chunks.append(chunk)
                current_year = test_end + 1
                break

            # If still too big and span is 1 year, split by month
            if span == 1 and count is not None and count > target_size:
                print(f"    Year {current_year} has {count:,} items - splitting by month")
                # Add monthly chunks for this year
                for month in range(1, 13):
                    month_end = month
                    chunk = {
                        "chunk_id": f"{current_year}-{month:02d}",
                        "year_start": current_year,
                        "year_end": current_year,
                        "month_start": month,
                        "month_end": month_end,
                        "expected_items": None,  # Don't pre-query monthly (too many API calls)
                    }
                    chunks.append(chunk)
                current_year += 1
                break

    print()
    print(f"Plan complete: {len(chunks)} chunks")
    total_expected = sum(c["expected_items"] or 0 for c in chunks)
    print(f"Expected total: {total_expected:,} items")
    print()

    return chunks


def infer_completed_chunks(conn: sqlite3.Connection, chunks: list):
    """
    Infer which chunks are complete based on existing items in database.

    For chunks with expected_items, mark complete if actual >= 90% of expected.
    For monthly chunks (no expected), mark complete if count > 0.
    """
    cursor = conn.cursor()

    print("Checking existing items against chunk plan...")
    marked_complete = 0

    for chunk in chunks:
        year_start = chunk["year_start"]
        year_end = chunk["year_end"]
        month_start = chunk.get("month_start")
        month_end = chunk.get("month_end")

        # Query actual count
        if month_start:
            cursor.execute(
                """
                SELECT COUNT(*) FROM items
                WHERE year BETWEEN ? AND ?
                AND CAST(substr(date, 6, 2) AS INTEGER) BETWEEN ? AND ?
            """,
                (year_start, year_end, month_start, month_end),
            )
        else:
            cursor.execute(
                "SELECT COUNT(*) FROM items WHERE year BETWEEN ? AND ?", (year_start, year_end)
            )

        actual = cursor.fetchone()[0]
        expected = chunk.get("expected_items") or 0

        # Mark complete if we have substantial data
        if expected > 0:
            threshold = int(expected * 0.9)
            is_complete = actual >= threshold
        else:
            # Monthly chunk with no expected count - assume complete if has any data
            is_complete = actual > 0

        if is_complete:
            chunk["completed_at"] = datetime.now().isoformat()
            chunk["actual_items"] = actual
            marked_complete += 1

    print(f"  Marked {marked_complete}/{len(chunks)} chunks as complete based on existing data")
    print()
    return chunks


def scrape_chunk(chunk, conn, fields, batch_size=10000):
    """
    Scrape all items for a single time chunk.

    Returns:
        (items_added, error) - error is None on success
    """
    year_start = chunk["year_start"]
    year_end = chunk["year_end"]
    month_start = chunk.get("month_start")
    month_end = chunk.get("month_end")

    # Build query for this chunk
    base = build_base_query()

    if month_start and month_end:
        date_query = f"date:[{year_start:04d}-{month_start:02d} TO {year_end:04d}-{month_end:02d}]"
        chunk_label = f"{year_start}-{month_start:02d}"
    else:
        date_query = f"date:[{year_start} TO {year_end}]"
        chunk_label = f"{year_start}-{year_end}"

    query = f"{date_query} AND {base}"

    print(f"  Chunk {chunk_label}: ", end="", flush=True)

    # Scrape with pagination
    cursor = None
    items_added = 0
    batch_num = 0

    while True:
        batch = scrape_batch(query, fields, cursor=cursor, count=batch_size)

        if not batch:
            return items_added, "API request failed"

        items = batch.get("items", [])
        cursor = batch.get("cursor")
        batch_num += 1

        if not items:
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
                None,
                None,
            )
            batch_data.append(row)

        db_cursor = conn.cursor()
        db_cursor.executemany(
            """
            INSERT OR IGNORE INTO items (
                identifier, title, date, year, creator, publisher, subject,
                description, format, imagecount, downloads, contributor, scanner,
                rights, licenseurl, call_number, isbn, issn, lccn, publicdate,
                addeddate, collection, quality_score, text_filename, enriched_at,
                downloaded_at, download_failed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            batch_data,
        )

        items_added += db_cursor.rowcount
        conn.commit()

        if not cursor:
            break

    print(f"{items_added:,} new items")
    return items_added, None


def build_index(year_start, year_end, output_dir, batch_size=10000):
    """
    Build complete IA index using adaptive time chunking.
    """
    start_time = datetime.now()

    print("=" * 80)
    print("INTERNET ARCHIVE INDEX BUILDER (Time-Chunked SQLite)")
    print("=" * 80)
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Date range: {year_start}-{year_end}")
    print()

    # Prepare database
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

    # Check for existing data
    cursor.execute("SELECT COUNT(*) FROM items")
    existing_count = cursor.fetchone()[0]

    if existing_count > 0:
        print(f"Found {existing_count:,} existing items")
        print()

    # Store metadata
    base_query = build_base_query()
    full_query = f"date:[{year_start} TO {year_end}] AND {base_query}"

    cursor.execute(
        "INSERT OR REPLACE INTO index_metadata (key, value) VALUES (?, ?)", ("query", full_query)
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

    # Fields to request
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

    # Check if we have a chunk plan
    cursor.execute("SELECT COUNT(*) FROM time_chunks")
    has_chunks = cursor.fetchone()[0] > 0

    if has_chunks:
        print("Existing chunk plan found - resuming")
        cursor.execute(
            "SELECT chunk_id, year_start, year_end, month_start, month_end, expected_items, completed_at FROM time_chunks ORDER BY year_start, month_start"
        )
        chunks = []
        for row in cursor.fetchall():
            chunks.append(
                {
                    "chunk_id": row[0],
                    "year_start": row[1],
                    "year_end": row[2],
                    "month_start": row[3],
                    "month_end": row[4],
                    "expected_items": row[5],
                    "completed_at": row[6],
                }
            )
    else:
        # Plan chunks
        chunks = plan_chunks(year_start, year_end, target_size=100000)

        # If we have existing items, infer completed chunks
        if existing_count > 0:
            chunks = infer_completed_chunks(conn, chunks)

        # Store chunk plan
        for chunk in chunks:
            cursor.execute(
                """
                INSERT OR REPLACE INTO time_chunks
                (chunk_id, year_start, year_end, month_start, month_end, expected_items, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    chunk["chunk_id"],
                    chunk["year_start"],
                    chunk["year_end"],
                    chunk.get("month_start"),
                    chunk.get("month_end"),
                    chunk.get("expected_items"),
                    chunk.get("completed_at"),
                ),
            )
        conn.commit()

    # Count incomplete chunks
    incomplete_chunks = [c for c in chunks if not c.get("completed_at")]
    completed_chunks = len(chunks) - len(incomplete_chunks)

    print(f"Chunk plan: {len(chunks)} total chunks")
    print(f"  Completed: {completed_chunks}")
    print(f"  Remaining: {len(incomplete_chunks)}")
    print()

    if not incomplete_chunks:
        print("All chunks complete!")
        cursor.execute("SELECT COUNT(*) FROM items")
        total_count = cursor.fetchone()[0]

        print()
        print(f"✓ Index complete: {total_count:,} items")
        print(f"✓ Database: {db_path}")
        print(f"✓ Size: {db_path.stat().st_size / 1024 / 1024:.1f} MB")
        conn.close()
        return db_path

    print("Scraping incomplete chunks...")
    print()

    # Process incomplete chunks
    total_new_items = 0
    for i, chunk in enumerate(incomplete_chunks):
        chunk_num = i + 1
        print(f"[{chunk_num}/{len(incomplete_chunks)}] ", end="")

        items_added, error = scrape_chunk(chunk, conn, fields, batch_size)

        # Update chunk status
        cursor.execute(
            """
            UPDATE time_chunks
            SET completed_at = ?, actual_items = ?, last_attempted_at = ?
            WHERE chunk_id = ?
        """,
            (
                datetime.now().isoformat() if not error else None,
                items_added if not error else None,
                datetime.now().isoformat(),
                chunk["chunk_id"],
            ),
        )
        conn.commit()

        if error:
            print(f"    ERROR: {error}")
            print()
            print(f"Chunk {chunk['chunk_id']} failed - state saved for resume")
            break

        total_new_items += items_added

        # Progress update
        if chunk_num % 5 == 0 or chunk_num == len(incomplete_chunks):
            cursor.execute("SELECT COUNT(*) FROM items")
            current_total = cursor.fetchone()[0]
            db_size = db_path.stat().st_size / 1024 / 1024
            print(
                f"    Progress: {chunk_num}/{len(incomplete_chunks)} chunks - {current_total:,} total items - {db_size:.1f} MB"
            )

    # Final stats
    cursor.execute("SELECT COUNT(*) FROM items")
    final_count = cursor.fetchone()[0]

    print()
    print(f"✓ Scraping session complete: {total_new_items:,} new items added")
    print(f"✓ Total items in database: {final_count:,}")

    # Update metadata
    cursor.execute(
        "INSERT OR REPLACE INTO index_metadata (key, value) VALUES (?, ?)",
        ("total_items", str(final_count)),
    )
    cursor.execute(
        "INSERT OR REPLACE INTO index_metadata (key, value) VALUES (?, ?)",
        ("last_updated", datetime.now().isoformat()),
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
        print(f"Items with year: {with_year:,} / {final_count:,}")
    print()

    # Quality distribution
    thresholds = [0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9]
    print("Quality score distribution (collection-based heuristic):")
    for threshold in thresholds:
        cursor.execute("SELECT COUNT(*) FROM items WHERE quality_score >= ?", (threshold,))
        count_above = cursor.fetchone()[0]
        pct = count_above / final_count * 100 if final_count > 0 else 0
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
        description="Build IA item index using adaptive time chunking (SQLite)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build index for 1800-1914
  tc-ia-index --year-start 1800 --year-end 1914 -o /path/to/corpus

  # Resume interrupted index build (automatic)
  tc-ia-index --year-start 1800 --year-end 1914 -o /path/to/corpus

Notes:
  - Adaptive chunking (year or month-level based on density)
  - Robust resume (continues from incomplete chunks only)
  - No duplicate scanning on resume
  - Safe to interrupt at any time
        """,
    )

    parser.add_argument("-o", "--output", required=True, help="Output directory (corpus base)")
    parser.add_argument("--year-start", type=int, default=1800, help="Start year (default: 1800)")
    parser.add_argument("--year-end", type=int, default=1914, help="End year (default: 1914)")
    parser.add_argument(
        "--batch-size", type=int, default=10000, help="Items per request (default: 10000, min: 100)"
    )

    args = parser.parse_args()

    if args.batch_size < 100:
        print("Error: batch-size must be at least 100")
        sys.exit(1)

    result = build_index(args.year_start, args.year_end, args.output, args.batch_size)

    if not result:
        sys.exit(1)


if __name__ == "__main__":
    main()
