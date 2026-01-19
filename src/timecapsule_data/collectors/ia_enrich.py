#!/usr/bin/env python3
"""
Internet Archive Index Enrichment Tool

Enriches an existing SQLite index with text filenames by querying the Metadata API.
This is Phase 2 of the two-phase approach:
- Phase 1: tc-ia-index builds basic index (fast, Scraping API)
- Phase 2: tc-ia-enrich adds filenames (selective, Metadata API)

Usage:
    tc-ia-enrich --index corpus/metadata/ia_index_1800_1914.db \
        --min-quality 0.65 --workers 4

Features:
- Resume support (skips already enriched items)
- Quality-based filtering (only enrich items you'll download)
- Parallel workers with rate limiting
- Fast SQLite updates (no file rewrites)
"""

import argparse
import json
import signal
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# Quality collections (must match ia_index.py)
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

# Global cancellation event for graceful shutdown
cancellation_event = threading.Event()


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully by setting cancellation event."""
    print("\n\nCancellation requested, finishing current enrichments...")
    print("(This may take a moment as workers complete their current items)")
    cancellation_event.set()


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


def calculate_quality_score(collections: list) -> float:
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


def get_item_metadata(
    identifier: str, rate_limiter: RateLimiter, retries: int = 3
) -> Optional[dict]:
    """Get full metadata for an item."""
    url = f"https://archive.org/metadata/{identifier}"

    for attempt in range(retries):
        rate_limiter.wait()

        try:
            req = Request(url, headers={"User-Agent": "TimeCapsuleLLM-Research/1.0"})
            with urlopen(req, timeout=60) as response:
                rate_limiter.record_success()
                data = json.loads(response.read().decode("utf-8"))
                return data

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

        except (URLError, TimeoutError, json.JSONDecodeError):
            rate_limiter.record_error()
            if attempt < retries - 1:
                continue
            else:
                return None

    return None


def find_text_file(files: list) -> Optional[str]:
    """Find the best text file from item files list. Returns filename."""
    # Priority order for text files
    for suffix in ["_djvu.txt", ".txt", "_ocr.txt", "_hocr_searchtext.txt.gz"]:
        for f in files:
            name = f.get("name", "")
            if name.endswith(suffix):
                return name
    return None


def print_interruption_summary(db_path: Path, starting_count: int, items_requested: int):
    """Print summary when enrichment is interrupted."""
    conn = sqlite3.Connection(db_path)
    cursor = conn.cursor()

    # Get current counts
    cursor.execute("SELECT COUNT(*) FROM items WHERE text_filename IS NOT NULL")
    total_with_filenames = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM items WHERE enriched_at IS NOT NULL")
    total_enriched = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM items WHERE enriched_at IS NULL")
    remaining = cursor.fetchone()[0]

    conn.close()

    enriched_this_run = total_enriched - starting_count

    print()
    print("=" * 80)
    print("INTERRUPTED - ENRICHMENT PAUSED")
    print("=" * 80)
    print()
    print("Progress summary:")
    print(f"  Items enriched this run: {enriched_this_run:,}")
    print(f"  Total enriched in database: {total_enriched:,}")
    print(f"  Items with text filenames: {total_with_filenames:,}")
    print(f"  Remaining items: {remaining:,}")
    print()
    print("Resume instructions:")
    print("  Your progress has been saved to the database.")
    print("  Simply run the same command again to continue enrichment.")
    print("  The tool will automatically skip already-enriched items.")
    print()


def enrich_worker(
    identifiers_to_enrich: list,
    db_path: Path,
    worker_id: int,
    lock: threading.Lock,
) -> dict:
    """Worker function for parallel enrichment."""
    rate_limiter = RateLimiter(base_delay=2.0)
    stats = {
        "enriched": 0,
        "failed": 0,
        "no_text_file": 0,
    }

    # Each worker gets its own connection
    conn = sqlite3.Connection(db_path)
    cursor = conn.cursor()

    for identifier in identifiers_to_enrich:
        # Check for cancellation before starting new item
        if cancellation_event.is_set():
            break

        if not identifier:
            continue

        # Get metadata
        metadata = get_item_metadata(identifier, rate_limiter)

        if not metadata:
            stats["failed"] += 1
            # Mark as attempted but failed
            with lock:
                cursor.execute(
                    "UPDATE items SET enriched_at = ? WHERE identifier = ?",
                    (datetime.now().isoformat(), identifier),
                )
                conn.commit()
            continue

        # Find text file
        files = metadata.get("files", [])
        text_filename = find_text_file(files)

        # Update item (thread-safe via lock)
        with lock:
            cursor.execute(
                "UPDATE items SET text_filename = ?, enriched_at = ? WHERE identifier = ?",
                (text_filename, datetime.now().isoformat(), identifier),
            )
            conn.commit()

        if text_filename:
            stats["enriched"] += 1
        else:
            stats["no_text_file"] += 1

    conn.close()
    return stats


class ProgressMonitor:
    """Progress monitor for enrichment."""

    def __init__(
        self,
        db_path: Path,
        total_to_enrich: int,
        lock: threading.Lock,
    ):
        self.db_path = db_path
        self.total_to_enrich = total_to_enrich
        self.lock = lock
        self.stop_event = threading.Event()
        self.start_time = time.time()
        self.last_count = 0

        # Get starting counts
        conn = sqlite3.Connection(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM items WHERE enriched_at IS NOT NULL")
        self.starting_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM items WHERE text_filename IS NOT NULL")
        self.starting_with_text = cursor.fetchone()[0]
        conn.close()

    def _monitor_loop(self):
        conn = sqlite3.Connection(self.db_path)
        cursor = conn.cursor()

        while not self.stop_event.is_set() and not cancellation_event.is_set():
            # Count enriched items
            cursor.execute("SELECT COUNT(*) FROM items WHERE enriched_at IS NOT NULL")
            enriched = cursor.fetchone()[0]

            # Print progress update (only if significant change or time passed)
            if enriched != self.last_count and enriched - self.last_count >= 100:
                elapsed = time.time() - self.start_time
                # Calculate rate based on NEW items only
                new_items = enriched - self.starting_count
                rate = new_items / elapsed if elapsed > 0.1 else 0
                pct = new_items / self.total_to_enrich * 100 if self.total_to_enrich > 0 else 0

                # Calculate ETA with human-friendly format
                remaining = self.total_to_enrich - new_items
                eta_sec = remaining / rate if rate > 0 else 0

                # Format ETA
                if eta_sec < 3600:
                    eta_str = f"{eta_sec / 60:.0f}m"
                else:
                    hours = int(eta_sec // 3600)
                    mins = int((eta_sec % 3600) // 60)
                    eta_str = f"{hours}h {mins}m"

                # Count successes vs failures (NEW items only)
                cursor.execute(
                    "SELECT COUNT(*) FROM items WHERE text_filename IS NOT NULL AND enriched_at IS NOT NULL"
                )
                total_with_text = cursor.fetchone()[0]
                new_with_text = total_with_text - self.starting_with_text
                success_rate = (new_with_text / new_items * 100) if new_items > 0 else 0

                # Use items/min or items/hour based on rate
                if rate < 1:
                    rate_str = f"{rate * 60:.1f} items/min"
                else:
                    rate_str = f"{rate:.1f} items/sec"

                print(
                    f"  Progress: {new_items:,}/{self.total_to_enrich:,} ({pct:.1f}%) - "
                    f"{new_with_text:,} with text ({success_rate:.1f}%) - "
                    f"{rate_str} - "
                    f"ETA: {eta_str}"
                )

                self.last_count = enriched

            self.stop_event.wait(5)  # Check every 5 seconds

        conn.close()

    def start(self):
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=1)


def main():
    parser = argparse.ArgumentParser(
        description="Enrich IA SQLite index with text filenames",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Enrich items with quality >= 0.65
  tc-ia-enrich --index corpus/metadata/ia_index_1800_1914.db \\
    --min-quality 0.65 --workers 4

  # Enrich specific quality range
  tc-ia-enrich --index corpus/metadata/ia_index_1800_1914.db \\
    --min-quality 0.8 --max-quality 0.89 --workers 4

  # Resume interrupted enrichment (automatic)
  tc-ia-enrich --index corpus/metadata/ia_index_1800_1914.db \\
    --min-quality 0.65 --workers 4

Notes:
  - Automatically resumes (skips items already enriched)
  - Fast SQLite updates (no file rewrites)
  - Can run multiple times with different quality ranges
        """,
    )

    parser.add_argument("--index", required=True, help="Path to SQLite index (.db)")
    parser.add_argument("--min-quality", type=float, default=0.65, help="Min quality threshold")
    parser.add_argument(
        "--max-quality",
        type=float,
        default=1.0,
        help="Max quality threshold (for range enrichment)",
    )
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers")

    args = parser.parse_args()

    # Validate index
    index_path = Path(args.index)
    if not index_path.exists():
        print(f"Error: Index not found at {args.index}")
        sys.exit(1)

    if index_path.suffix != ".db":
        print(f"Error: Expected SQLite database (.db), got: {index_path.suffix}")
        print("Use tc-ia-migrate-to-sqlite to convert JSON indexes")
        sys.exit(1)

    start_time = datetime.now()

    print("=" * 80)
    print("INTERNET ARCHIVE INDEX ENRICHMENT")
    print("=" * 80)
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Database: {index_path}")
    print()

    # Connect to database
    conn = sqlite3.Connection(index_path)
    cursor = conn.cursor()

    # Get total count
    cursor.execute("SELECT COUNT(*) FROM items")
    total_items = cursor.fetchone()[0]
    print(f"  Total items in index: {total_items:,}")

    # Calculate quality scores for items that don't have them yet
    print("Calculating quality scores...")
    cursor.execute("SELECT COUNT(*) FROM items WHERE quality_score IS NULL")
    items_without_score = cursor.fetchone()[0]

    if items_without_score > 0:
        print(f"  Items without quality score: {items_without_score:,}")
        print("  Calculating...")

        # Fetch items without scores
        cursor.execute("SELECT identifier, collection FROM items WHERE quality_score IS NULL")
        items_to_score = cursor.fetchall()

        # Calculate and update in batches
        batch_size = 1000
        for i in range(0, len(items_to_score), batch_size):
            batch = items_to_score[i : i + batch_size]
            updates = []
            for identifier, collection_json in batch:
                if collection_json:
                    collections = json.loads(collection_json) if collection_json else []
                else:
                    collections = []
                quality_score = calculate_quality_score(collections)
                updates.append((quality_score, identifier))

            cursor.executemany("UPDATE items SET quality_score = ? WHERE identifier = ?", updates)
            conn.commit()

            if (i + batch_size) % 10000 == 0:
                print(f"    Scored {i + batch_size:,}/{len(items_to_score):,} items")

        print(f"  ✓ Calculated quality scores for {items_without_score:,} items")

    # Get items to enrich
    print()
    print("Querying items to enrich...")
    cursor.execute(
        """
        SELECT identifier FROM items
        WHERE quality_score >= ?
        AND quality_score <= ?
        AND enriched_at IS NULL
    """,
        (args.min_quality, args.max_quality),
    )
    identifiers_to_enrich = [row[0] for row in cursor.fetchall()]

    # Format estimated time
    est_seconds = len(identifiers_to_enrich) / (args.workers * 0.3)
    if est_seconds < 3600:
        est_str = f"{est_seconds / 60:.0f} minutes"
    else:
        hours = int(est_seconds // 3600)
        mins = int((est_seconds % 3600) // 60)
        est_str = f"{hours}h {mins}m"

    print()
    print("Enrichment plan:")
    print(f"  Quality range: {args.min_quality} - {args.max_quality}")
    print(f"  Items to enrich: {len(identifiers_to_enrich):,}")
    print(f"  Workers: {args.workers}")
    print(f"  Estimated time: {est_str}")
    print()

    if not identifiers_to_enrich:
        print("No items need enrichment (all already enriched or outside quality range)")
        conn.close()
        return

    # Split identifiers across workers
    chunk_size = max(1, len(identifiers_to_enrich) // args.workers)
    chunks = [
        identifiers_to_enrich[i : i + chunk_size]
        for i in range(0, len(identifiers_to_enrich), chunk_size)
    ]

    # Get starting count for interruption summary
    cursor.execute("SELECT COUNT(*) FROM items WHERE enriched_at IS NOT NULL")
    starting_enriched_count = cursor.fetchone()[0]

    conn.close()

    # Register signal handler for graceful cancellation
    signal.signal(signal.SIGINT, signal_handler)

    # Start enrichment
    print("Starting enrichment...")
    lock = threading.Lock()

    # Start progress monitor
    monitor = ProgressMonitor(index_path, len(identifiers_to_enrich), lock)
    monitor.start()

    total_enriched = 0
    total_failed = 0
    total_no_text = 0

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = []
            for worker_id, chunk in enumerate(chunks):
                future = executor.submit(enrich_worker, chunk, index_path, worker_id, lock)
                futures.append(future)

            # Collect results
            for future in as_completed(futures):
                stats = future.result()
                total_enriched += stats["enriched"]
                total_failed += stats["failed"]
                total_no_text += stats["no_text_file"]

    except KeyboardInterrupt:
        monitor.stop()
        print_interruption_summary(index_path, starting_enriched_count, len(identifiers_to_enrich))
        sys.exit(0)

    # Stop monitor
    monitor.stop()

    # Update metadata
    print()
    print("Updating metadata...")
    cursor.execute(
        "INSERT OR REPLACE INTO index_metadata (key, value) VALUES (?, ?)",
        ("enrichment_last_enriched_at", datetime.now().isoformat()),
    )

    # Get total enriched count
    cursor.execute("SELECT COUNT(*) FROM items WHERE text_filename IS NOT NULL")
    total_with_filenames = cursor.fetchone()[0]

    cursor.execute(
        "INSERT OR REPLACE INTO index_metadata (key, value) VALUES (?, ?)",
        ("enrichment_total_enriched", str(total_with_filenames)),
    )

    conn.commit()
    conn.close()

    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    duration_minutes = duration.total_seconds() / 60

    print()
    print("=" * 80)
    print("ENRICHMENT COMPLETE")
    print("=" * 80)
    print(f"  Items enriched: {total_enriched:,}")
    print(f"  Items with no text file: {total_no_text:,}")
    print(f"  Items failed: {total_failed:,}")
    print()

    # Show overall enrichment status
    conn = sqlite3.Connection(index_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM items")
    total_in_index = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM items WHERE enriched_at IS NOT NULL")
    attempted = cursor.fetchone()[0]

    print("Index status:")
    print(f"  Total items: {total_in_index:,}")
    print(f"  Enrichment attempted: {attempted:,} ({attempted / total_in_index * 100:.1f}%)")
    print(
        f"  Text filenames found: {total_with_filenames:,} ({total_with_filenames / total_in_index * 100:.1f}%)"
    )
    print()
    print(f"Database: {index_path}")
    print()
    print(f"Started:  {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Ended:    {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration_minutes:.1f} minutes ({duration.total_seconds():.0f} seconds)")

    conn.close()


if __name__ == "__main__":
    main()
