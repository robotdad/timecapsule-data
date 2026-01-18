#!/usr/bin/env python3
"""
Internet Archive Index Enrichment Tool

Enriches an existing index with text filenames by querying the Metadata API.
This is Phase 2 of the two-phase approach:
- Phase 1: tc-ia-index builds basic index (fast, Scraping API)
- Phase 2: tc-ia-enrich adds filenames (selective, Metadata API)

Usage:
    tc-ia-enrich --index corpus/metadata/ia_index_1800_1914.json \\
        --min-quality 0.65 --workers 4

Features:
- Resume support (skips already enriched items)
- Quality-based filtering (only enrich items you'll download)
- Incremental saving (every 100 items)
- Parallel workers with rate limiting
"""

import argparse
import json
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

# Known text formats (priority order)
TEXT_FORMATS = ["DjVuTXT", "Text PDF", "Abbyy GZ", "hOCR", "OCR Search Text"]

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


def enrich_worker(
    items_to_enrich: list,
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

    for item in items_to_enrich:
        identifier = item.get("identifier", "")
        if not identifier:
            continue

        # Get metadata
        metadata = get_item_metadata(identifier, rate_limiter)

        if not metadata:
            stats["failed"] += 1
            with lock:
                item["enriched_at"] = datetime.now().isoformat()
                # Leave text_filename as None to mark attempted but failed
            continue

        # Find text file
        files = metadata.get("files", [])
        text_filename = find_text_file(files)

        # Update item (thread-safe)
        with lock:
            item["text_filename"] = text_filename
            item["enriched_at"] = datetime.now().isoformat()

        if text_filename:
            stats["enriched"] += 1
        else:
            stats["no_text_file"] += 1

    return stats


def save_index(index_data: dict, index_file: Path, lock: threading.Lock):
    """Thread-safe index save."""
    with lock:
        # Ensure enrichment_status exists (backward compat with old indexes)
        if "enrichment_status" not in index_data:
            index_data["enrichment_status"] = {
                "total_enriched": 0,
                "last_enriched_at": None,
                "quality_thresholds_completed": [],
            }

        # Update enrichment status
        enriched_count = sum(
            1 for item in index_data["items"] if item.get("text_filename") is not None
        )
        index_data["enrichment_status"]["total_enriched"] = enriched_count
        index_data["enrichment_status"]["last_enriched_at"] = datetime.now().isoformat()

        # Write to disk
        with open(index_file, "w") as f:
            json.dump(index_data, f, indent=2)


def main():
    parser = argparse.ArgumentParser(
        description="Enrich IA index with text filenames",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Enrich items with quality >= 0.65
  tc-ia-enrich --index corpus/metadata/ia_index_1800_1914.json \\
    --min-quality 0.65 --workers 4

  # Enrich specific quality range
  tc-ia-enrich --index corpus/metadata/ia_index_1800_1914.json \\
    --min-quality 0.8 --max-quality 0.89 --workers 4

  # Resume interrupted enrichment
  tc-ia-enrich --index corpus/metadata/ia_index_1800_1914.json \\
    --min-quality 0.65 --workers 4

Notes:
  - Automatically resumes (skips items with text_filename set)
  - Saves every 100 items (max ~75s loss on interrupt)
  - Can run multiple times with different quality ranges
  - Items below min-quality are skipped (saves API calls)
        """,
    )

    parser.add_argument("--index", required=True, help="Path to IA index JSON file")
    parser.add_argument("--min-quality", type=float, default=0.65, help="Min quality threshold")
    parser.add_argument(
        "--max-quality",
        type=float,
        default=1.0,
        help="Max quality threshold (for range enrichment)",
    )
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers")

    args = parser.parse_args()

    # Load index
    print("=" * 80)
    print("INTERNET ARCHIVE INDEX ENRICHMENT")
    print("=" * 80)

    index_file = Path(args.index)
    if not index_file.exists():
        print(f"Error: Index not found at {args.index}")
        sys.exit(1)

    print(f"Loading index from {index_file}...")
    with open(index_file) as f:
        index_data = json.load(f)

    all_items = index_data.get("items", [])
    print(f"  Total items in index: {len(all_items):,}")

    # Calculate quality scores for items that don't have them yet
    print("Calculating quality scores...")
    items_scored = 0
    for item in all_items:
        if item.get("quality_score") is None:
            collections = item.get("collection", [])
            item["quality_score"] = calculate_quality_score(collections)
            items_scored += 1

    if items_scored > 0:
        print(f"  Calculated quality scores for {items_scored:,} items")

    # Filter to items that need enrichment
    items_to_enrich = []
    for item in all_items:
        quality = item.get("quality_score", 0.5)

        # Quality range filter
        if quality < args.min_quality or quality > args.max_quality:
            continue

        # Skip already enriched
        if item.get("enriched_at") is not None:
            continue

        items_to_enrich.append(item)

    print()
    print("Enrichment plan:")
    print(f"  Quality range: {args.min_quality} - {args.max_quality}")
    print(f"  Items to enrich: {len(items_to_enrich):,}")
    print(f"  Workers: {args.workers}")
    print(
        f"  Estimated time: {len(items_to_enrich) / (args.workers * 0.3):.0f} seconds ({len(items_to_enrich) / (args.workers * 0.3) / 60:.1f} minutes)"
    )
    print()

    if not items_to_enrich:
        print("No items need enrichment (all already enriched or outside quality range)")
        return

    # Split items across workers
    chunk_size = max(1, len(items_to_enrich) // args.workers)
    chunks = [
        items_to_enrich[i : i + chunk_size] for i in range(0, len(items_to_enrich), chunk_size)
    ]

    # Progress monitoring thread
    class ProgressMonitor:
        def __init__(
            self,
            items: list,
            total_to_enrich: int,
            index_data: dict,
            index_file: Path,
            lock: threading.Lock,
        ):
            self.items = items
            self.total_to_enrich = total_to_enrich
            self.index_data = index_data
            self.index_file = index_file
            self.lock = lock
            self.stop_event = threading.Event()
            self.start_time = time.time()
            self.last_count = 0
            self.last_save_count = 0
            # Track starting count for accurate rate calculation (resume support)
            self.starting_count = sum(1 for item in items if item.get("enriched_at") is not None)

        def _monitor_loop(self):
            while not self.stop_event.is_set():
                # Count enriched items (those with enriched_at set)
                enriched = sum(1 for item in self.items if item.get("enriched_at") is not None)

                # SAVE every 100 items (CRITICAL!)
                if enriched - self.last_save_count >= 100:
                    print("    [Saving progress to disk...]")
                    save_index(self.index_data, self.index_file, self.lock)
                    self.last_save_count = enriched

                # Print progress update
                if enriched != self.last_count:
                    elapsed = time.time() - self.start_time
                    # Calculate rate based on NEW items only (not pre-existing)
                    new_items = enriched - self.starting_count
                    rate = (
                        new_items / elapsed if elapsed > 0.1 else 0
                    )  # Avoid division by tiny numbers
                    # Progress: new items this run / total to enrich this run
                    pct = new_items / self.total_to_enrich * 100 if self.total_to_enrich > 0 else 0

                    # Calculate ETA
                    remaining = self.total_to_enrich - enriched
                    eta_sec = remaining / rate if rate > 0 else 0
                    eta_min = eta_sec / 60

                    # Count successes vs failures
                    with_text = sum(
                        1
                        for item in self.items
                        if item.get("text_filename") is not None
                        and item.get("enriched_at") is not None
                    )

                    print(
                        f"  Progress: {new_items:,}/{self.total_to_enrich:,} ({pct:.1f}%) - "
                        f"{with_text:,} with text - "
                        f"{rate:.1f} items/sec - "
                        f"ETA: {eta_min:.1f}m"
                    )

                    self.last_count = enriched

                self.stop_event.wait(5)  # Check every 5 seconds

        def start(self):
            self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.thread.start()

        def stop(self):
            self.stop_event.set()
            self.thread.join(timeout=1)

    # Enrichment with periodic saves
    print("Starting enrichment...")
    start_time = time.time()
    lock = threading.Lock()

    # Start progress monitor (also handles periodic saves!)
    monitor = ProgressMonitor(all_items, len(items_to_enrich), index_data, index_file, lock)
    monitor.start()

    total_enriched = 0
    total_failed = 0
    total_no_text = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for worker_id, chunk in enumerate(chunks):
            future = executor.submit(enrich_worker, chunk, worker_id, lock)
            futures.append(future)

        # Collect results (saves happen in monitor thread every 100 items)
        completed_workers = 0
        for future in as_completed(futures):
            stats = future.result()
            total_enriched += stats["enriched"]
            total_failed += stats["failed"]
            total_no_text += stats["no_text_file"]
            completed_workers += 1

    # Stop monitor
    monitor.stop()

    # Final save
    print()
    print("Saving final enrichment...")
    save_index(index_data, index_file, lock)

    # Update enrichment status
    if args.min_quality not in index_data["enrichment_status"]["quality_thresholds_completed"]:
        index_data["enrichment_status"]["quality_thresholds_completed"].append(args.min_quality)

    with open(index_file, "w") as f:
        json.dump(index_data, f, indent=2)

    # Summary
    print()
    print("=" * 80)
    print("ENRICHMENT COMPLETE")
    print("=" * 80)
    print(f"  Items enriched: {total_enriched:,}")
    print(f"  Items with no text file: {total_no_text:,}")
    print(f"  Items failed: {total_failed:,}")
    print(f"  Total time: {time.time() - start_time:.1f}s")
    print()

    # Show overall enrichment status
    total_in_index = len(all_items)
    total_with_filenames = sum(1 for item in all_items if item.get("text_filename") is not None)
    attempted = sum(1 for item in all_items if item.get("enriched_at") is not None)

    print("Index status:")
    print(f"  Total items: {total_in_index:,}")
    print(f"  Enrichment attempted: {attempted:,} ({attempted / total_in_index * 100:.1f}%)")
    print(
        f"  Text filenames found: {total_with_filenames:,} ({total_with_filenames / total_in_index * 100:.1f}%)"
    )
    print()
    print(f"  Index updated: {index_file}")


if __name__ == "__main__":
    main()
