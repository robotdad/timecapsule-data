#!/usr/bin/env python3
"""
Internet Archive Download Tool (SQLite with Smart Filename Discovery)

Downloads text files from items in a SQLite index with intelligent filename guessing
and fallback to metadata API.

Strategy:
1. Try common filename patterns (fast)
2. If 404, call metadata API to get real filename
3. Store discovered filename in database for future use

This eliminates the need for a separate enrichment phase, reducing 3-week enrichment
to a few days of concurrent download + discovery.

Usage:
    tc-ia-download --index /path/to/ia_index_1800_1914.db \
        --max-items 50000 --workers 4 \
        -o /path/to/corpus/ia/

Features:
- Resume support (download state in database)
- Smart filename guessing (85%+ success rate)
- Parallel downloads with rate limiting
- Quality filtering
- Gutenberg deduplication
"""

import argparse
import csv
import json
import re
import signal
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import Optional, Set
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


class ThreadSafeDBWriter:
    """
    Thread-safe database writer that serializes all writes through a queue.

    SQLite doesn't handle concurrent writes well, especially on slow filesystems
    (like WSL2 → Windows). This class provides a single writer thread that
    processes all database updates sequentially, avoiding lock contention.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._queue: Queue[tuple[str, tuple, threading.Event | None]] = Queue()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._conn: sqlite3.Connection | None = None

    def start(self):
        """Start the writer thread."""
        self._thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 30.0):
        """Stop the writer thread, waiting for pending writes to complete."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)

    def _writer_loop(self):
        """Main loop that processes database writes."""
        self._conn = sqlite3.connect(self.db_path, timeout=60.0)
        # WAL mode provides better concurrent read/write performance
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=60000")  # 60 second timeout
        self._conn.execute("PRAGMA synchronous=NORMAL")  # Faster, still safe with WAL

        while not self._stop_event.is_set() or not self._queue.empty():
            try:
                # Use timeout so we can check stop_event periodically
                sql, params, done_event = self._queue.get(timeout=0.5)
                try:
                    self._conn.execute(sql, params)
                    self._conn.commit()
                except sqlite3.Error as e:
                    # Log but don't crash - the download still succeeded
                    print(f"  DB write error (non-fatal): {e}")
                finally:
                    if done_event:
                        done_event.set()
                    self._queue.task_done()
            except Exception:
                # Queue.get timeout - just continue
                pass

        # Final cleanup
        if self._conn:
            try:
                self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except sqlite3.Error:
                pass
            self._conn.close()

    def execute(self, sql: str, params: tuple = (), wait: bool = False):
        """
        Queue a SQL statement for execution.

        Args:
            sql: SQL statement with ? placeholders
            params: Parameters tuple
            wait: If True, block until this specific write completes
        """
        done_event = threading.Event() if wait else None
        self._queue.put((sql, params, done_event))
        if done_event:
            done_event.wait()

    def update_downloaded(self, identifier: str, filename: str):
        """Mark an item as successfully downloaded."""
        self.execute(
            "UPDATE items SET text_filename = ?, downloaded_at = ? WHERE identifier = ?",
            (filename, datetime.now().isoformat(), identifier),
        )

    def update_failed(self, identifier: str):
        """Mark an item as failed to download."""
        self.execute(
            "UPDATE items SET download_failed_at = ? WHERE identifier = ?",
            (datetime.now().isoformat(), identifier),
        )


IA_DOWNLOAD_BASE = "https://archive.org/download"

# Common filename patterns (priority order)
FILENAME_PATTERNS = [
    "{identifier}_djvu.txt",
    "{identifier}.txt",
    "{identifier}_ocr.txt",
    "{identifier}_hocr_searchtext.txt.gz",
]

# Global cancellation event for graceful shutdown
cancellation_event = threading.Event()


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully by setting cancellation event."""
    print("\n\nCancellation requested, finishing current downloads...")
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
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def wait(self):
        with self._lock:
            delay = self.current_delay
        time.sleep(delay)

    def record_success(self):
        with self._lock:
            self.consecutive_successes += 1
            self.consecutive_errors = 0
            if self.consecutive_successes >= 10:
                self.current_delay = max(self.min_delay, self.current_delay * self.success_speedup)
                self.consecutive_successes = 0

    def record_error(self, is_rate_limit: bool = False):
        with self._lock:
            self.consecutive_errors += 1
            self.consecutive_successes = 0
            if is_rate_limit:
                self.current_delay = min(
                    self.max_delay, self.current_delay * self.backoff_factor * 2
                )
            else:
                self.current_delay = min(self.max_delay, self.current_delay * self.backoff_factor)


# Global shared rate limiter for all workers (prevents IP bans)
global_rate_limiter = RateLimiter(base_delay=2.0)


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
            if e.code == 404:
                return None  # File not found (don't retry)
            elif e.code == 429:
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


def get_item_metadata(identifier: str, rate_limiter: RateLimiter) -> Optional[dict]:
    """Get full metadata for an item to find available files."""
    url = f"https://archive.org/metadata/{identifier}"

    for attempt in range(3):
        rate_limiter.wait()

        try:
            req = Request(url, headers={"User-Agent": "TimeCapsuleLLM-Research/1.0"})
            with urlopen(req, timeout=60) as response:
                rate_limiter.record_success()
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
            rate_limiter.record_error()
            if attempt < 2:
                continue
            else:
                return None

    return None


def find_text_file_from_metadata(metadata: dict) -> Optional[str]:
    """Find the best text file from metadata files list."""
    files = metadata.get("files", [])
    for suffix in ["_djvu.txt", ".txt", "_ocr.txt", "_hocr_searchtext.txt.gz"]:
        for f in files:
            name = f.get("name", "")
            if name.endswith(suffix):
                return name
    return None


def download_with_discovery(
    identifier: str,
    known_filename: Optional[str],
    output_dir: Path,
    db_writer: ThreadSafeDBWriter,
    rate_limiter: RateLimiter,
) -> tuple[bool, str, Optional[str]]:
    """
    Download text file with smart filename discovery.

    Returns:
        (success, reason, discovered_filename)
    """
    filenames_to_try = []

    # If we already know the filename, try it first
    if known_filename:
        filenames_to_try = [known_filename]
    else:
        # Try common patterns
        filenames_to_try = [pattern.format(identifier=identifier) for pattern in FILENAME_PATTERNS]

    # Try each filename
    for filename in filenames_to_try:
        url = f"{IA_DOWNLOAD_BASE}/{identifier}/{quote(filename, safe='')}"
        content = fetch_with_retry(url, rate_limiter, retries=1)

        if content:
            # Success! Save file and update database
            safe_id = re.sub(r"[^\w\-]", "_", identifier)[:100]
            filepath = output_dir / f"{safe_id}.txt"

            try:
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(content)

                # Update database with discovered filename (via thread-safe writer)
                db_writer.update_downloaded(identifier, filename)
                return True, "success", filename

            except Exception as e:
                return False, f"save_error: {e}", None

    # All guesses failed - call metadata API
    metadata = get_item_metadata(identifier, rate_limiter)

    if not metadata:
        db_writer.update_failed(identifier)
        return False, "metadata_api_failed", None

    # Find actual filename from metadata
    actual_filename = find_text_file_from_metadata(metadata)

    if not actual_filename:
        db_writer.update_failed(identifier)
        return False, "no_text_file_in_metadata", None

    # Download with discovered filename
    url = f"{IA_DOWNLOAD_BASE}/{identifier}/{quote(actual_filename, safe='')}"
    content = fetch_with_retry(url, rate_limiter)

    if not content:
        db_writer.update_failed(identifier)
        return False, "download_failed_after_metadata", None

    # Save file and update database
    safe_id = re.sub(r"[^\w\-]", "_", identifier)[:100]
    filepath = output_dir / f"{safe_id}.txt"

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

        db_writer.update_downloaded(identifier, actual_filename)
        return True, "success_via_metadata", actual_filename

    except Exception as e:
        return False, f"save_error: {e}", None


def print_interruption_summary(db_path: Path, starting_count: int, items_requested: int):
    """Print summary when download is interrupted."""
    conn = sqlite3.Connection(db_path)
    cursor = conn.cursor()

    # Get current counts
    cursor.execute("SELECT COUNT(*) FROM items WHERE downloaded_at IS NOT NULL")
    total_downloaded = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM items WHERE downloaded_at IS NULL")
    remaining = cursor.fetchone()[0]

    conn.close()

    downloaded_this_run = total_downloaded - starting_count

    print()
    print("=" * 80)
    print("INTERRUPTED - DOWNLOADS PAUSED")
    print("=" * 80)
    print()
    print("Progress summary:")
    print(f"  Downloaded this run: {downloaded_this_run:,}")
    print(f"  Total in database: {total_downloaded:,}")
    print(f"  Remaining items: {remaining:,}")
    print()
    print("Resume instructions:")
    print("  Your progress has been saved to the database.")
    print("  Simply run the same command again to continue downloading.")
    print("  The tool will automatically skip already-downloaded items.")
    print()


def download_worker(
    items: list,
    output_dir: Path,
    db_writer: ThreadSafeDBWriter,
    worker_id: int,
) -> dict:
    """Worker function for parallel downloads with discovery."""
    # Use global shared rate limiter (removed local instance)
    stats = {
        "downloaded": 0,
        "failed": 0,
        "guessed_correct": 0,
        "needed_metadata": 0,
    }

    for identifier, known_filename in items:
        # Check for cancellation before starting new download
        if cancellation_event.is_set():
            break

        success, reason, discovered_filename = download_with_discovery(
            identifier, known_filename, output_dir, db_writer, global_rate_limiter
        )

        if success:
            stats["downloaded"] += 1
            if reason == "success" and not known_filename:
                stats["guessed_correct"] += 1
            elif reason == "success_via_metadata":
                stats["needed_metadata"] += 1
        else:
            stats["failed"] += 1

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Download texts from IA SQLite index with smart filename discovery",
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
  - Discovers filenames on-the-fly (no separate enrichment needed)
  - Rate limits per worker (respectful to IA)
        """,
    )

    parser.add_argument("--index", required=True, help="Path to SQLite index (.db)")
    parser.add_argument("-o", "--output", required=True, help="Output directory")
    parser.add_argument("--gutenberg-metadata", help="Gutenberg metadata CSV for dedup")
    parser.add_argument(
        "--max-items", type=int, default=None, help="Max items to download (default: all)"
    )
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

    # Ensure download tracking columns exist
    conn = sqlite3.Connection(index_path)
    cursor = conn.cursor()

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
        placeholders = ",".join("?" * len(gutenberg_exclusions))
        exclusion_clause = f"AND identifier NOT IN ({placeholders})"
        exclusion_params = tuple(gutenberg_exclusions)
    else:
        exclusion_params = ()

    # Build LIMIT clause conditionally
    limit_clause = "LIMIT ?" if args.max_items else ""

    query = f"""
        SELECT identifier, text_filename FROM items
        WHERE quality_score >= ?
        AND imagecount >= ?
        AND downloaded_at IS NULL
        {exclusion_clause}
        ORDER BY year ASC, identifier ASC
        {limit_clause}
    """

    params = (args.min_quality, args.min_imagecount) + exclusion_params
    if args.max_items:
        params = params + (args.max_items,)
    cursor.execute(query, params)
    items_to_download = cursor.fetchall()

    # Count how many already have filenames (from previous enrichment or downloads)
    items_with_known_filenames = sum(1 for _, filename in items_to_download if filename)

    print()
    print("Download plan:")
    print(f"  Items to download: {len(items_to_download):,}")
    print(f"  Already have filenames: {items_with_known_filenames:,}")
    print(f"  Need filename discovery: {len(items_to_download) - items_with_known_filenames:,}")
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

    # Register signal handler for graceful cancellation
    signal.signal(signal.SIGINT, signal_handler)

    # Download with workers
    print("Starting downloads with smart filename discovery and global rate limiter...")
    print()

    # Progress monitor thread
    class DownloadProgressMonitor:
        def __init__(self, db_path: Path, starting_count: int, target_count: int):
            self.db_path = db_path
            self.starting_count = starting_count
            self.target_count = target_count
            self.stop_event = threading.Event()
            self.start_time = time.time()
            self.last_count = starting_count

        def _monitor_loop(self):
            conn = sqlite3.Connection(self.db_path)
            cursor = conn.cursor()

            while not self.stop_event.is_set() and not cancellation_event.is_set():
                cursor.execute("SELECT COUNT(*) FROM items WHERE downloaded_at IS NOT NULL")
                current_count = cursor.fetchone()[0]

                if current_count != self.last_count:
                    new_items = current_count - self.starting_count
                    elapsed = time.time() - self.start_time
                    rate = new_items / elapsed if elapsed > 0 else 0
                    pct = (new_items / self.target_count * 100) if self.target_count > 0 else 0

                    remaining = self.target_count - new_items
                    eta_sec = remaining / rate if rate > 0 else 0

                    if eta_sec < 3600:
                        eta_str = f"{eta_sec / 60:.0f}m"
                    else:
                        hours = int(eta_sec // 3600)
                        mins = int((eta_sec % 3600) // 60)
                        eta_str = f"{hours}h {mins}m"

                    if rate < 1:
                        rate_str = f"{rate * 60:.1f} items/min"
                    else:
                        rate_str = f"{rate:.1f} items/sec"

                    print(
                        f"  Progress: {new_items:,}/{self.target_count:,} ({pct:.1f}%) - "
                        f"{rate_str} - ETA: {eta_str}"
                    )
                    self.last_count = current_count

                self.stop_event.wait(10)

            conn.close()

        def start(self):
            self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.thread.start()

        def stop(self):
            self.stop_event.set()
            self.thread.join(timeout=1)

    monitor = DownloadProgressMonitor(index_path, already_downloaded, len(items_to_download))
    monitor.start()

    # Create thread-safe database writer (serializes all DB writes to avoid lock contention)
    db_writer = ThreadSafeDBWriter(index_path)
    db_writer.start()

    total_downloaded = 0
    total_failed = 0
    total_guessed = 0
    total_metadata = 0

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = []
            for worker_id, chunk in enumerate(chunks):
                future = executor.submit(download_worker, chunk, output_dir, db_writer, worker_id)
                futures.append(future)

            # Collect results
            for future in as_completed(futures):
                stats = future.result()
                total_downloaded += stats["downloaded"]
                total_failed += stats["failed"]
                total_guessed += stats["guessed_correct"]
                total_metadata += stats["needed_metadata"]

    except KeyboardInterrupt:
        monitor.stop()
        print("Waiting for pending database writes to complete...")
        db_writer.stop(timeout=30.0)
        print_interruption_summary(index_path, already_downloaded, len(items_to_download))
        sys.exit(0)

    monitor.stop()
    db_writer.stop(timeout=30.0)

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
    print("Filename discovery stats:")
    print(f"  Guessed correctly: {total_guessed:,} ({total_guessed / total_downloaded * 100:.1f}%)")
    print(
        f"  Needed metadata API: {total_metadata:,} ({total_metadata / total_downloaded * 100:.1f}%)"
    )
    print()
    print(f"Output: {output_dir}")
    print()
    print(f"Started:  {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Ended:    {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

    if duration_minutes < 60:
        duration_str = f"{duration_minutes:.1f} minutes"
    else:
        hours = int(duration_minutes // 60)
        mins = int(duration_minutes % 60)
        duration_str = f"{hours}h {mins}m"

    print(f"Duration: {duration_str} ({duration.total_seconds():.0f} seconds)")


if __name__ == "__main__":
    main()
