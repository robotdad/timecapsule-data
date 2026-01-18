#!/usr/bin/env python3
"""
Internet Archive Download Tool

Downloads text files from items listed in an index, with filtering and resume support.

This is Phase 3 of the redesign:
- Phase 1: tc-ia-index builds complete catalog (DONE)
- Phase 2: Optional filtering (done inline here)
- Phase 3: Download with resume (THIS TOOL)

Usage:
    tc-ia-download --index /path/to/ia_index_1800_1914.json \\
        --max-items 50000 --workers 4 \\
        -o /path/to/corpus/raw/ia/

Features:
- Resume support via download_state.json
- Parallel downloads with rate limiting
- Progress tracking
- Metadata traceability (identifier -> index -> IA)
- Filtering: text format, quality, size, Gutenberg dedup
"""

import argparse
import csv
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Set
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# Known text formats
TEXT_FORMATS = ["DjVuTXT", "Text PDF", "Abbyy GZ", "hOCR", "OCR Search Text"]

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
        """Wait before next request."""
        time.sleep(self.current_delay)

    def record_success(self):
        """Record successful request - may speed up."""
        self.consecutive_successes += 1
        self.consecutive_errors = 0

        # Speed up after 10 consecutive successes
        if self.consecutive_successes >= 10:
            self.current_delay = max(self.min_delay, self.current_delay * self.success_speedup)
            self.consecutive_successes = 0

    def record_error(self, is_rate_limit: bool = False):
        """Record error - back off."""
        self.consecutive_errors += 1
        self.consecutive_successes = 0

        if is_rate_limit:
            self.current_delay = min(self.max_delay, self.current_delay * self.backoff_factor * 2)
        else:
            self.current_delay = min(self.max_delay, self.current_delay * self.backoff_factor)


@dataclass
class ExistingCorpus:
    """Track what we already have to avoid re-downloading."""

    titles: Set[str] = field(default_factory=set)
    title_author_pairs: Set[tuple] = field(default_factory=set)

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


@dataclass
class DownloadState:
    """Track download progress for resume support."""

    downloaded: Set[str] = field(default_factory=set)
    failed: dict = field(default_factory=dict)
    last_updated: str = ""

    @classmethod
    def load(cls, path: Path) -> "DownloadState":
        if not path.exists():
            return cls()

        with open(path) as f:
            data = json.load(f)

        return cls(
            downloaded=set(data.get("downloaded", [])),
            failed=data.get("failed", {}),
            last_updated=data.get("last_updated", ""),
        )

    def save(self, path: Path):
        self.last_updated = datetime.now().isoformat()
        with open(path, "w") as f:
            json.dump(
                {
                    "downloaded": list(self.downloaded),
                    "failed": self.failed,
                    "last_updated": self.last_updated,
                },
                f,
                indent=2,
            )


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


def get_item_metadata(identifier: str, rate_limiter: RateLimiter) -> Optional[dict]:
    """Get full metadata for an item to find available files."""
    url = f"https://archive.org/metadata/{identifier}"
    content = fetch_with_retry(url, rate_limiter)
    if content:
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return None
    return None


def find_text_file(files: list) -> Optional[str]:
    """
    Find the best text file from item files list.

    Returns the filename (not the full file object).
    """
    # Priority order for text files
    for suffix in ["_djvu.txt", ".txt", "_ocr.txt", "_hocr_searchtext.txt.gz"]:
        for f in files:
            name = f.get("name", "")
            if name.endswith(suffix):
                return name
    return None


def download_text(
    identifier: str, text_filename: Optional[str], rate_limiter: RateLimiter
) -> tuple[Optional[str], str]:
    """
    Download text content for an item.

    Args:
        identifier: IA item identifier
        text_filename: Filename from enriched index (or None if not enriched)
        rate_limiter: Rate limiter instance

    Returns:
        (text_content, error_reason) - content is None on failure
    """
    # If filename not provided (index not enriched), fetch metadata
    if not text_filename:
        metadata = get_item_metadata(identifier, rate_limiter)
        if not metadata:
            return None, "metadata_fetch_failed"

        files = metadata.get("files", [])
        if not files:
            return None, "no_files_in_metadata"

        text_filename = find_text_file(files)
        if not text_filename:
            return None, "no_text_file_found"

    # Download the text file
    url = f"{IA_DOWNLOAD_BASE}/{identifier}/{text_filename}"
    content = fetch_with_retry(url, rate_limiter)

    if not content:
        return None, "download_failed"

    return content, ""


def filter_item(
    item: dict,
    min_quality: float,
    min_imagecount: int,
    existing: ExistingCorpus,
    quality_collections: dict,
) -> tuple[bool, str]:
    """
    Filter an item based on criteria.

    Returns: (should_download, reason_if_not)
    """
    # Check for text format
    formats = item.get("format", [])
    if isinstance(formats, str):
        formats = [formats]

    has_text = any(fmt in TEXT_FORMATS for fmt in formats)
    if not has_text:
        return False, "no_text_format"

    # Check imagecount (page count)
    imagecount = item.get("imagecount", 0)
    if isinstance(imagecount, str):
        try:
            imagecount = int(imagecount)
        except ValueError:
            imagecount = 0

    if imagecount < min_imagecount:
        return False, f"too_small_{imagecount}pages"

    # Check quality (collection-based heuristic)
    collections = item.get("collection", [])
    if isinstance(collections, str):
        collections = [collections]

    best_score = 0.5  # Default for unknown
    for coll in collections:
        coll_lower = coll.lower()
        for known, score in quality_collections.items():
            if known in coll_lower:
                best_score = max(best_score, score)

    if best_score < min_quality:
        return False, f"low_quality_{best_score:.2f}"

    # Check Gutenberg duplicate
    title = item.get("title", "")
    if isinstance(title, list):
        title = title[0] if title else ""

    creator = item.get("creator", "")
    if isinstance(creator, list):
        creator = "; ".join(creator)

    if existing.is_duplicate(title, creator):
        return False, "gutenberg_duplicate"

    return True, ""


def download_worker(
    items: list,
    output_dir: Path,
    state: DownloadState,
    existing: ExistingCorpus,
    quality_collections: dict,
    min_quality: float,
    min_imagecount: int,
    worker_id: int,
) -> dict:
    """Worker function for parallel downloads."""
    rate_limiter = RateLimiter(base_delay=2.0)
    stats = {
        "downloaded": 0,
        "skipped_already_have": 0,
        "skipped_filter": 0,
        "failed": 0,
    }

    for item in items:
        identifier = item.get("identifier", "")
        if not identifier:
            continue

        # Skip if already downloaded
        if identifier in state.downloaded:
            stats["skipped_already_have"] += 1
            continue

        # Filter item
        should_download, reason = filter_item(
            item, min_quality, min_imagecount, existing, quality_collections
        )

        if not should_download:
            stats["skipped_filter"] += 1
            continue

        # Download (use filename from index if available)
        item_text_filename = item.get("text_filename")
        content, error_reason = download_text(identifier, item_text_filename, rate_limiter)

        if not content:
            stats["failed"] += 1
            state.failed[identifier] = error_reason
            continue

        # Save
        safe_id = re.sub(r"[^\w\-]", "_", identifier)[:100]
        filepath = output_dir / f"{safe_id}.txt"

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)

            state.downloaded.add(identifier)
            stats["downloaded"] += 1

        except Exception as e:
            stats["failed"] += 1
            state.failed[identifier] = f"save_error_{str(e)}"

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Download texts from IA index with filtering and resume",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download up to 1000 items
  tc-ia-download --index corpus/metadata/ia_index_1800_1914.json \\
    --max-items 1000 --workers 4 -o corpus/raw/ia

  # Resume interrupted download
  tc-ia-download --index corpus/metadata/ia_index_1800_1914.json \\
    --max-items 50000 --workers 4 -o corpus/raw/ia

  # With Gutenberg deduplication
  tc-ia-download --index corpus/metadata/ia_index_1800_1914.json \\
    --gutenberg-metadata corpus/raw/gutenberg/metadata.csv \\
    --max-items 50000 -o corpus/raw/ia

Notes:
  - Automatically resumes from download_state.json
  - Saves metadata.json with full traceability
  - Rate limits per worker (respectful to IA)
        """,
    )

    parser.add_argument("--index", required=True, help="Path to IA index JSON file")
    parser.add_argument("-o", "--output", required=True, help="Output directory")
    parser.add_argument("--gutenberg-metadata", help="Gutenberg metadata CSV for dedup")
    parser.add_argument("--max-items", type=int, default=50000, help="Max items to download")
    parser.add_argument("--workers", type=int, default=4, help="Parallel download workers")
    parser.add_argument("--min-quality", type=float, default=0.65, help="Min quality threshold")
    parser.add_argument(
        "--min-imagecount", type=int, default=10, help="Min page count (excludes pamphlets)"
    )

    args = parser.parse_args()

    # Load index
    print("=" * 80)
    print("INTERNET ARCHIVE DOWNLOADER")
    print("=" * 80)
    print(f"Index: {args.index}")
    print(f"Output: {args.output}")
    print()

    index_path = Path(args.index)
    if not index_path.exists():
        print(f"Error: Index not found at {args.index}")
        sys.exit(1)

    print("Loading index...")
    with open(index_path) as f:
        index_data = json.load(f)

    all_items = index_data.get("items", [])
    print(f"  Total items in index: {len(all_items):,}")

    # Load existing corpus for dedup
    existing = ExistingCorpus()
    if args.gutenberg_metadata:
        gb_path = Path(args.gutenberg_metadata)
        if gb_path.exists():
            print("Loading Gutenberg metadata for deduplication...")
            existing.add_from_gutenberg_metadata(gb_path)

    # Quality collections (same as index builder)
    quality_collections = {
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

    # Load download state
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    state_file = output_dir.parent / "metadata" / "download_state.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)

    state = DownloadState.load(state_file)

    if state.downloaded:
        print(f"  Resuming: {len(state.downloaded)} already downloaded")

    # Filter and prepare items for download
    print()
    print("Filtering items...")
    items_to_download = []

    for item in all_items:
        identifier = item.get("identifier", "")
        if not identifier:
            continue

        if identifier in state.downloaded:
            continue

        should_download, _ = filter_item(
            item, args.min_quality, args.min_imagecount, existing, quality_collections
        )

        if should_download:
            items_to_download.append(item)

        if len(items_to_download) >= args.max_items:
            break

    print(f"  Items to download: {len(items_to_download):,}")
    print(f"  Workers: {args.workers}")
    print(f"  Min quality: {args.min_quality}")
    print(f"  Min pages: {args.min_imagecount}")
    print()

    if not items_to_download:
        print("No items to download (all filtered or already downloaded)")
        return

    # Split items across workers
    chunk_size = len(items_to_download) // args.workers
    chunks = [
        items_to_download[i : i + chunk_size] for i in range(0, len(items_to_download), chunk_size)
    ]

    # Download with workers
    print("Starting downloads...")
    start_time = time.time()
    last_save = time.time()

    total_stats = {
        "downloaded": 0,
        "skipped_already_have": 0,
        "skipped_filter": 0,
        "failed": 0,
    }

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for worker_id, chunk in enumerate(chunks):
            future = executor.submit(
                download_worker,
                chunk,
                output_dir,
                state,
                existing,
                quality_collections,
                args.min_quality,
                args.min_imagecount,
                worker_id,
            )
            futures.append(future)

        # Monitor progress
        completed = 0
        for future in as_completed(futures):
            stats = future.result()
            for key in total_stats:
                total_stats[key] += stats[key]

            completed += 1

            # Save state periodically
            if time.time() - last_save > 30:  # Every 30 seconds
                state.save(state_file)
                last_save = time.time()

            # Progress update
            elapsed = time.time() - start_time
            rate = total_stats["downloaded"] / elapsed if elapsed > 0 else 0
            print(
                f"  Progress: {total_stats['downloaded']:,} downloaded, "
                f"{total_stats['skipped_filter']:,} filtered, "
                f"{total_stats['failed']:,} failed - "
                f"{rate:.1f} items/min"
            )

    # Final save
    state.save(state_file)

    # Save metadata
    metadata_file = output_dir / "metadata.json"
    downloaded_items = [item for item in all_items if item.get("identifier") in state.downloaded]

    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "source": "Internet Archive",
                "source_index": str(index_path),
                "query": index_data.get("query", ""),
                "date_range": index_data.get("date_range", []),
                "filters": {
                    "min_quality": args.min_quality,
                    "min_imagecount": args.min_imagecount,
                },
                "download_timestamp": datetime.now().isoformat(),
                "total_downloaded": len(state.downloaded),
                "items": downloaded_items,
            },
            f,
            indent=2,
        )

    # Summary
    print()
    print("=" * 80)
    print("DOWNLOAD COMPLETE")
    print("=" * 80)
    print(f"  Downloaded: {total_stats['downloaded']:,}")
    print(f"  Skipped (filtered): {total_stats['skipped_filter']:,}")
    print(f"  Failed: {total_stats['failed']:,}")
    print(f"  Total time: {time.time() - start_time:.1f}s")
    print()
    print(f"  Output: {output_dir}")
    print(f"  Metadata: {metadata_file}")
    print(f"  State: {state_file}")


if __name__ == "__main__":
    main()
