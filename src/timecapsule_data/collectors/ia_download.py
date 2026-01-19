#!/usr/bin/env python3
"""
Internet Archive Download Tool (SQLite)

Downloads text files from items in a SQLite index with filtering and resume support.

This is Phase 3 of the pipeline:
- Phase 1: tc-ia-index builds complete catalog
- Phase 2: tc-ia-enrich adds text filenames
- Phase 3: tc-ia-download downloads texts (THIS TOOL)

Usage:
    tc-ia-download --index /path/to/ia_index_1800_1914.db \
        --max-items 50000 --workers 4 \
        -o /path/to/corpus/ia/

Features:
- Resume support (stores download state in database)
- Parallel downloads with rate limiting
- Progress tracking
- Quality filtering
- Gutenberg deduplication
"""

import argparse
import csv
import json
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Set
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

IA_DOWNLOAD_BASE = "https://archive.org/download"


@dataclass
class RateLimiter:
    """Adaptive rate limiter - starts slow, backs off on errors."""

    base_delay: float = 2.0
    current_delay: float = 2.0
    max_delay: float = 60.0
    min_delay: float = 0.5
    backoff_factor: float = 2.0
    success_speedup: float = 0.9
    consecutive_successes: int = 0
    consecutive_errors: int = 0

    def wait(self):
        time.sleep(self.current_delay)

    def record_success(self):
        self.consecutive_successes += 1
        self.consecutive_errors = 0
        if self.consecutive_successes >= 10:
            self.current_delay = max(self.min_delay, self.current_delay * self.success_speedup)
            self.consecutive_successes = 0

    def record_error(self, is_rate_limit: bool = False):
        self.consecutive_errors += 1
        self.consecutive_successes = 0
        if is_rate_limit:
            self.current_delay = min(self.max_delay, self.current_delay * self.backoff_factor * 2)
        else:
            self.current_delay = min(self.max_delay, self.current_delay * self.backoff_factor)


@dataclass
class ExistingCorpus:
    """Track what we already have to avoid re-downloading."""

    titles: Set[str]
    title_author_pairs: Set[tuple]

    def __init__(self):
        self.titles = set()
        self.title_author_pairs = set()

    def normalize_title(self, title: str) -> str:
        title = title.lower()
        title = re.sub(r"[^\w\s]", "", title)
        title = re.sub(r"\s+", " ", title).strip()
        title = re.sub(r"^(the|a|an)\s+", "", title)
        return title

    def normalize_author(self, author: str) -> str:
        author = author.lower()
        author = re.sub(r"[^\w\s]", "", author)
        if "," in author:
            parts = author.split(",", 1)
            author = f"{parts[1].strip()} {parts[0].strip()}"
        author = re.sub(r"\s+", " ", author).strip()
        return author

    def add_from_gutenberg_metadata(self, metadata_path: Path):
        if not metadata_path.exists():
            print(f"  Gutenberg metadata not found at {metadata_path}")
            return

        with open(metadata_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                title = self.normalize_title(row.get("title", ""))
                if title:
                    self.titles.add(title)

                authors = row.get("authors", "")
                for author in authors.split(";"):
                    author = self.normalize_author(author)
                    if author and title:
                        self.title_author_pairs.add((title, author))

        print(f"  Loaded {len(self.titles)} titles from Gutenberg metadata")

    def is_duplicate(self, title: str, author: str = "") -> bool:
        norm_title = self.normalize_title(title)
        if norm_title in self.titles:
            return True

        if author:
            norm_author = self.normalize_author(author)
            for existing_title, existing_author in self.title_author_pairs:
                if existing_author == norm_author:
                    if norm_title in existing_title or existing_title in norm_title:
                        return True
        return False


def fetch_with_retry(url: str, rate_limiter: RateLimiter, retries: int = 3) -> Optional[str]:
    """Fetch URL with rate limiting and retry logic."""
    for attempt in range(retries):
        rate_limiter.wait()

        try:
            req = Request(url, headers={"User-Agent": "TimeCapsuleLLM-Research/1.0"})
            with urlopen(req, timeout=60) as response:
                rate_limiter.record_success()
                return response.read().decode("utf-8", errors="replace")

        except HTTPError as e:
            if e.code == 429:
                rate_limiter.record_error(is_rate_limit=True)
            elif e.code == 503:
                rate_limiter.record_error(is_rate_limit=True)
            else:
                rate_limiter.record_error()

            if attempt < retries - 1:
                continue
            else:
                return None

        except (URLError, TimeoutError):
            rate_limiter.record_error()
            if attempt < retries - 1:
                continue
            else:
                return None

    return None


def download_worker(
    items: list,
    output_dir: Path,
    db_path: Path,
    worker_id: int,
) -> dict:
    """Worker function for parallel downloads."""
    rate_limiter = RateLimiter(base_delay=2.0)
    stats = {
        "downloaded": 0,
        "failed": 0,
    }

    # Each worker gets its own connection
    conn = sqlite3.Connection(db_path)
    cursor = conn.cursor()

    for identifier, text_filename in items:
        if not text_filename:
            stats["failed"] += 1
            continue

        # Download the text file
        url = f"{IA_DOWNLOAD_BASE}/{identifier}/{text_filename}"
        content = fetch_with_retry(url, rate_limiter)

        if not content:
            stats["failed"] += 1
            cursor.execute(
                "UPDATE items SET download_failed_at = ? WHERE identifier = ?",
                (datetime.now().isoformat(), identifier),
            )
            conn.commit()
            continue

        # Save to disk
        safe_id = re.sub(r"[^\w\-]", "_", identifier)[:100]
        filepath = output_dir / f"{safe_id}.txt"

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)

            # Mark as downloaded in database
            cursor.execute(
                "UPDATE items SET downloaded_at = ? WHERE identifier = ?",
                (datetime.now().isoformat(), identifier),
            )
            conn.commit()
            stats["downloaded"] += 1

        except Exception:
            stats["failed"] += 1
            cursor.execute(
                "UPDATE items SET download_failed_at = ? WHERE identifier = ?",
                (datetime.now().isoformat(), identifier),
            )
            conn.commit()

    conn.close()
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Download texts from IA SQLite index with filtering and resume",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download up to 1000 items
  tc-ia-download --index corpus/metadata/ia_index_1800_1914.db \\
    --max-items 1000 --workers 4 -o corpus/ia

  # Resume interrupted download (automatic)
  tc-ia-download --index corpus/metadata/ia_index_1800_1914.db \\
    --max-items 50000 --workers 4 -o corpus/ia

  # With Gutenberg deduplication
  tc-ia-download --index corpus/metadata/ia_index_1800_1914.db \\
    --gutenberg-metadata corpus/gutenberg/metadata.csv \\
    --max-items 50000 -o corpus/ia

Notes:
  - Automatically resumes (skips items already downloaded)
  - Download state stored in database
  - Rate limits per worker (respectful to IA)
        """,
    )

    parser.add_argument("--index", required=True, help="Path to SQLite index (.db)")
    parser.add_argument("-o", "--output", required=True, help="Output directory")
    parser.add_argument("--gutenberg-metadata", help="Gutenberg metadata CSV for dedup")
    parser.add_argument("--max-items", type=int, default=50000, help="Max items to download")
    parser.add_argument("--workers", type=int, default=4, help="Parallel download workers")
    parser.add_argument("--min-quality", type=float, default=0.65, help="Min quality threshold")
    parser.add_argument(
        "--min-imagecount", type=int, default=10, help="Min page count (excludes pamphlets)"
    )

    args = parser.parse_args()

    # Validate index
    index_path = Path(args.index)
    if not index_path.exists():
        print(f"Error: Index not found at {args.index}")
        sys.exit(1)

    if index_path.suffix != ".db":
        print(f"Error: Expected SQLite database (.db), got: {index_path.suffix}")
        sys.exit(1)

    start_time = datetime.now()

    print("=" * 80)
    print("INTERNET ARCHIVE DOWNLOADER")
    print("=" * 80)
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Database: {index_path}")
    print(f"Output: {args.output}")
    print()

    # Ensure downloaded_at column exists
    conn = sqlite3.Connection(index_path)
    cursor = conn.cursor()

    # Add download tracking columns if they don't exist
    cursor.execute("PRAGMA table_info(items)")
    columns = [row[1] for row in cursor.fetchall()]

    if "downloaded_at" not in columns:
        print("Adding download tracking to database...")
        cursor.execute("ALTER TABLE items ADD COLUMN downloaded_at TEXT")
        cursor.execute("ALTER TABLE items ADD COLUMN download_failed_at TEXT")
        conn.commit()

    # Get total count
    cursor.execute("SELECT COUNT(*) FROM items")
    total_items = cursor.fetchone()[0]
    print(f"  Total items in index: {total_items:,}")

    # Count already downloaded
    cursor.execute("SELECT COUNT(*) FROM items WHERE downloaded_at IS NOT NULL")
    already_downloaded = cursor.fetchone()[0]

    if already_downloaded > 0:
        print(f"  Already downloaded: {already_downloaded:,}")

    # Load existing corpus for dedup
    existing = ExistingCorpus()
    if args.gutenberg_metadata:
        gb_path = Path(args.gutenberg_metadata)
        if gb_path.exists():
            print("Loading Gutenberg metadata for deduplication...")
            existing.add_from_gutenberg_metadata(gb_path)

    # Build exclusion list for Gutenberg duplicates
    gutenberg_exclusions = set()
    if existing.titles:
        print("Checking for Gutenberg duplicates...")
        cursor.execute("SELECT identifier, title, creator FROM items WHERE downloaded_at IS NULL")
        for identifier, title_json, creator_json in cursor.fetchall():
            if title_json:
                titles = json.loads(title_json) if isinstance(title_json, str) else title_json
                title = titles[0] if isinstance(titles, list) and titles else str(titles)
            else:
                title = ""

            if creator_json:
                creators = (
                    json.loads(creator_json) if isinstance(creator_json, str) else creator_json
                )
                creator = "; ".join(creators) if isinstance(creators, list) else str(creators)
            else:
                creator = ""

            if existing.is_duplicate(title, creator):
                gutenberg_exclusions.add(identifier)

        if gutenberg_exclusions:
            print(f"  Excluding {len(gutenberg_exclusions):,} Gutenberg duplicates")

    # Query items to download
    print()
    print("Querying items to download...")

    # Build exclusion clause
    exclusion_clause = ""
    if gutenberg_exclusions:
        excluded_ids = ",".join(f"'{id}'" for id in gutenberg_exclusions)
        exclusion_clause = f"AND identifier NOT IN ({excluded_ids})"

    query = f"""
        SELECT identifier, text_filename FROM items
        WHERE quality_score >= ?
        AND imagecount >= ?
        AND text_filename IS NOT NULL
        AND downloaded_at IS NULL
        {exclusion_clause}
        LIMIT ?
    """

    cursor.execute(query, (args.min_quality, args.min_imagecount, args.max_items))
    items_to_download = cursor.fetchall()

    print()
    print("Download plan:")
    print(f"  Items to download: {len(items_to_download):,}")
    print(f"  Workers: {args.workers}")
    print(f"  Min quality: {args.min_quality}")
    print(f"  Min pages: {args.min_imagecount}")
    print()

    if not items_to_download:
        print("No items to download (all filtered or already downloaded)")
        conn.close()
        return

    conn.close()

    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Split items across workers
    chunk_size = max(1, len(items_to_download) // args.workers)
    chunks = [
        items_to_download[i : i + chunk_size] for i in range(0, len(items_to_download), chunk_size)
    ]

    # Download with workers
    print("Starting downloads...")
    download_start_time = time.time()

    total_downloaded = 0
    total_failed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for worker_id, chunk in enumerate(chunks):
            future = executor.submit(download_worker, chunk, output_dir, index_path, worker_id)
            futures.append(future)

        # Monitor progress
        completed_workers = 0
        for future in as_completed(futures):
            stats = future.result()
            total_downloaded += stats["downloaded"]
            total_failed += stats["failed"]
            completed_workers += 1

            # Progress update
            elapsed = time.time() - download_start_time
            rate = total_downloaded / elapsed if elapsed > 0 else 0
            print(
                f"  Worker {completed_workers}/{len(chunks)} complete - "
                f"{total_downloaded:,} downloaded, {total_failed:,} failed - "
                f"{rate:.1f} items/sec"
            )

    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    duration_minutes = duration.total_seconds() / 60

    print()
    print("=" * 80)
    print("DOWNLOAD COMPLETE")
    print("=" * 80)
    print(f"  Downloaded: {total_downloaded:,}")
    print(f"  Failed: {total_failed:,}")
    print()
    print(f"Output: {output_dir}")
    print()
    print(f"Started:  {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Ended:    {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration_minutes:.1f} minutes ({duration.total_seconds():.0f} seconds)")


if __name__ == "__main__":
    main()
