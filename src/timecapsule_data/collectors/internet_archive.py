#!/usr/bin/env python3
"""
Internet Archive Collector - Custom Implementation

Why not use the official internetarchive library?
- IA has no published rate limits and aggressively throttles bulk downloads
- The official library does not provide rate limiting assistance
- For 100k+ item collections over days, adaptive rate limiting is critical
- This implementation provides fine-grained control over request patterns

See: https://internetarchive.archiveteam.org/index.php/Uploading_With_Python
"The internetarchive library does not provide assistance with complying with [rate limiting]"

IA Guidance (from their blog):
- "Start slowly and ramp up"
- "Contact info@archive.org for large projects"
- No published rate limits, but be respectful

Metadata tracked for later scoring:
- collection: Which IA collection (americana, toronto, etc.)
- mediatype: books, texts, etc.
- content_type: newspaper, magazine, book, pamphlet, government, etc.
- scanner: Which scanning center/equipment
- contributor: Library/institution that provided the item
"""

import argparse
import csv
import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

IA_SEARCH_API = "https://archive.org/advancedsearch.php"
IA_METADATA_API = "https://archive.org/metadata"
IA_DOWNLOAD_BASE = "https://archive.org/download"


# Known good collections for historical texts
QUALITY_COLLECTIONS = {
    "americana": 0.9,  # American Libraries - generally good
    "toronto": 0.85,  # University of Toronto - good quality
    "europeanlibraries": 0.85,  # European Libraries
    "blc": 0.8,  # British Library
    "bplscas": 0.8,  # Boston Public Library
    "library_of_congress": 0.9,  # LOC - excellent
    "gutenberg": 0.95,  # Already have this, but good quality
    "biodiversity": 0.8,  # Biodiversity Heritage Library
    "medicalheritage": 0.8,  # Medical Heritage Library
}

# Content type inference from metadata
CONTENT_TYPE_PATTERNS = {
    "newspaper": ["newspaper", "daily", "gazette", "times", "herald", "tribune", "journal news"],
    "magazine": ["magazine", "periodical", "monthly", "weekly", "quarterly", "review"],
    "government": ["government", "congress", "parliamentary", "official", "statutes", "laws"],
    "directory": ["directory", "almanac", "register", "catalogue", "census"],
    "reference": ["dictionary", "encyclopedia", "encyclopaedia", "handbook", "manual"],
    "religious": ["bible", "sermon", "hymn", "prayer", "church", "religious"],
    "scientific": ["scientific", "science", "proceedings", "transactions", "journal of"],
    "fiction": ["novel", "stories", "tales", "fiction"],
    "poetry": ["poems", "poetry", "verse", "sonnets"],
}


class BannedException(Exception):
    """Raised when IA appears to have banned us."""

    pass


@dataclass
class RateLimiter:
    """Adaptive rate limiter - starts slow, backs off on errors."""

    base_delay: float = 3.0  # Starting delay between requests (increased from 2.0)
    current_delay: float = 3.0
    max_delay: float = 120.0  # Increased from 60.0 to allow longer backoffs
    min_delay: float = 0.5
    backoff_factor: float = 2.5  # Increased from 2.0 for more aggressive backoff
    success_speedup: float = 0.9  # Gradually speed up on success
    consecutive_successes: int = 0
    consecutive_errors: int = 0
    consecutive_rate_limits: int = 0  # Track 429s in a row for ban detection

    def wait(self):
        """Wait before next request."""
        time.sleep(self.current_delay)

    def record_success(self):
        """Record successful request - may speed up."""
        self.consecutive_successes += 1
        self.consecutive_errors = 0
        self.consecutive_rate_limits = 0  # Reset ban detection counter

        # Speed up after 10 consecutive successes
        if self.consecutive_successes >= 10:
            self.current_delay = max(self.min_delay, self.current_delay * self.success_speedup)
            self.consecutive_successes = 0

    def record_error(self, is_rate_limit: bool = False):
        """Record error - back off."""
        self.consecutive_errors += 1
        self.consecutive_successes = 0

        if is_rate_limit:
            self.consecutive_rate_limits += 1

            # Ban detection: 5 consecutive rate limits = likely banned
            if self.consecutive_rate_limits >= 5:
                raise BannedException(
                    "Hit 5 consecutive rate limits (HTTP 429) - "
                    "Internet Archive has likely banned this IP temporarily. "
                    "Wait a few hours and try resuming with --resume"
                )

            # Aggressive backoff for rate limits (increased from 2x to 4x)
            self.current_delay = min(self.max_delay, self.current_delay * self.backoff_factor * 4)
        else:
            self.consecutive_rate_limits = 0  # Reset if it's not a rate limit
            self.current_delay = min(self.max_delay, self.current_delay * self.backoff_factor)

    def reset(self):
        """Reset to base delay."""
        self.current_delay = self.base_delay
        self.consecutive_successes = 0
        self.consecutive_errors = 0


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
            print(f"Warning: Gutenberg metadata not found at {metadata_path}")
            return

        with open(metadata_path, "r", encoding="utf-8") as f:
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

        print(f"Loaded {len(self.titles)} titles from existing corpus")

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


def infer_content_type(title: str, subject: str = "", description: str = "") -> str:
    """Infer content type from metadata."""
    text = f"{title} {subject} {description}".lower()

    for content_type, patterns in CONTENT_TYPE_PATTERNS.items():
        for pattern in patterns:
            if pattern in text:
                return content_type

    return "book"  # Default


def fetch_with_retry(url: str, rate_limiter: RateLimiter, retries: int = 3) -> Optional[str]:
    """Fetch URL with rate limiting and retry logic."""

    for attempt in range(retries):
        rate_limiter.wait()

        try:
            req = Request(
                url,
                headers={
                    "User-Agent": "TimeCapsuleLLM-Research/1.0 (academic research; contact: info@example.edu)"
                },
            )
            with urlopen(req, timeout=60) as response:
                rate_limiter.record_success()
                return response.read().decode("utf-8", errors="replace")

        except HTTPError as e:
            if e.code == 429:  # Rate limited
                rate_limiter.record_error(is_rate_limit=True)
                print(f"  Rate limited, backing off to {rate_limiter.current_delay:.1f}s")
            elif e.code == 503:  # Service unavailable
                rate_limiter.record_error(is_rate_limit=True)
                print("  Service unavailable, backing off")
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


def search_ia(
    query: str, fields: List[str], rows: int, page: int, rate_limiter: RateLimiter
) -> dict:
    """Search Internet Archive."""
    field_params = "&".join(f"fl[]={f}" for f in fields)
    url = f"{IA_SEARCH_API}?q={quote(query)}&{field_params}&rows={rows}&page={page}&output=json"

    content = fetch_with_retry(url, rate_limiter)
    if content:
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return {}
    return {}


def get_item_metadata(identifier: str, rate_limiter: RateLimiter) -> dict:
    """Get full metadata for an item."""
    url = f"{IA_METADATA_API}/{identifier}"
    content = fetch_with_retry(url, rate_limiter)
    if content:
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return {}
    return {}


def find_text_file(files: list) -> Optional[dict]:
    """Find the best text file from item files."""
    # Priority order
    for suffix in ["_djvu.txt", ".txt", "_ocr.txt"]:
        for f in files:
            name = f.get("name", "")
            if name.endswith(suffix):
                return f
    return None


def download_text(identifier: str, filename: str, rate_limiter: RateLimiter) -> Optional[str]:
    """Download text content."""
    url = f"{IA_DOWNLOAD_BASE}/{identifier}/{filename}"
    return fetch_with_retry(url, rate_limiter)


def estimate_ocr_quality(text: str) -> float:
    """Estimate OCR quality from common error patterns."""
    if len(text) < 1000:
        return 0.0

    sample = text[:50000] if len(text) > 50000 else text
    sample_lower = sample.lower()

    score = 1.0

    # Check "the" errors
    the_correct = sample_lower.count(" the ")
    the_errors = sum(
        sample_lower.count(x) for x in [" tlie ", " tbe ", " tiie ", " ihe ", " tne ", " thc "]
    )
    if the_correct > 10:
        error_rate = the_errors / (the_correct + the_errors)
        score -= error_rate * 0.3

    # Check character quality
    printable = sum(c.isprintable() or c in "\n\r\t" for c in sample)
    char_quality = printable / len(sample)
    score -= (1 - char_quality) * 0.3

    # Check for excessive short words (OCR artifacts)
    words = sample.split()
    if words:
        single_char_ratio = sum(1 for w in words if len(w) == 1) / len(words)
        if single_char_ratio > 0.15:
            score -= (single_char_ratio - 0.15) * 0.4

    return max(0.0, min(1.0, score))


def collection_quality_score(collections: list) -> float:
    """Score based on which collection(s) item belongs to."""
    if not collections:
        return 0.5  # Unknown

    best_score = 0.5
    for coll in collections:
        coll_lower = coll.lower()
        for known_coll, score in QUALITY_COLLECTIONS.items():
            if known_coll in coll_lower:
                best_score = max(best_score, score)

    return best_score


def get_cache_path(output_dir: Path, content_type: str, year_end: int) -> Path:
    """Get path for cached search results."""
    cache_dir = output_dir / "metadata" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"search_{content_type}_{year_end}.json"


def load_cached_search(cache_path: Path) -> Optional[List[dict]]:
    """Load cached search results if they exist."""
    if not cache_path.exists():
        return None
    try:
        with open(cache_path) as f:
            data = json.load(f)
        items = data.get("items", [])
        print(f"Loaded {len(items)} items from cache ({cache_path.name})")
        return items
    except Exception as e:
        print(f"Cache load failed: {e}")
        return None


def save_search_cache(cache_path: Path, items: List[dict], query: str):
    """Save search results to cache."""
    data = {
        "cached_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "query": query,
        "count": len(items),
        "items": items,
    }
    with open(cache_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Cached {len(items)} search results to {cache_path.name}")


def search_all_items(
    query: str,
    fields: List[str],
    rate_limiter: RateLimiter,
    verbose: bool = False,
) -> List[dict]:
    """
    Search Internet Archive and return ALL matching items.

    This function searches all pages until no more results are found.
    Results are returned as a list of raw search result dictionaries.
    """
    all_items = []
    page = 1

    print(f"\nSearching Internet Archive (rate limit: {rate_limiter.base_delay}s base)...")

    try:
        while True:
            result = search_ia(query, fields, rows=50, page=page, rate_limiter=rate_limiter)
            docs = result.get("response", {}).get("docs", [])

            if not docs:
                if verbose:
                    print(f"No more results at page {page}")
                break

            total_found = result.get("response", {}).get("numFound", 0)
            all_items.extend(docs)

            if page == 1 or page % 10 == 0:
                print(f"  Page {page}: {len(all_items):,} / {total_found:,} items")

            page += 1

            # Safety limit - prevent infinite loops
            if page > 1000:
                print("  Reached page limit (1000)")
                break

    except BannedException:
        print(f"\n⚠️  Ban detected during search after {len(all_items)} items")
        print("  Returning partial results - you can resume later")
        # Return what we have so far

    print(f"\nSearch complete: {len(all_items):,} total items found")
    return all_items


def download_single_item(
    doc: dict,
    output_dir: Path,
    rate_limiter: RateLimiter,
    existing: ExistingCorpus,
    args,
) -> Optional[dict]:
    """
    Download a single item. Returns metadata dict on success, None on failure/skip.

    This function encapsulates the entire download logic for one item.
    """
    identifier = doc.get("identifier", "")
    title = doc.get("title", "Unknown")
    if isinstance(title, list):
        title = title[0]

    creator = doc.get("creator", "")
    if isinstance(creator, list):
        creator = "; ".join(creator)

    # Extract metadata for tracking
    collections = doc.get("collection", [])
    if isinstance(collections, str):
        collections = [collections]

    subjects = doc.get("subject", [])
    if isinstance(subjects, str):
        subjects = [subjects]

    description = doc.get("description", "")
    if isinstance(description, list):
        description = " ".join(description)

    contributor = doc.get("contributor", "")
    if isinstance(contributor, list):
        contributor = contributor[0] if contributor else ""

    scanner = doc.get("scanner", "")

    # Infer content type
    content_type = infer_content_type(title, " ".join(subjects), description)

    # Pre-download duplicate check
    if existing.is_duplicate(title, creator):
        if args.verbose:
            print(f"  SKIP (dupe): {title[:50]}...")
        return None

    # Collection quality check
    coll_score = collection_quality_score(collections)

    # Get full metadata and find text file
    if args.verbose:
        print(f"  Fetching metadata: {identifier}")

    item_meta = get_item_metadata(identifier, rate_limiter)
    if not item_meta:
        return None

    files = item_meta.get("files", [])
    text_file = find_text_file(files)

    if not text_file:
        if args.verbose:
            print(f"  SKIP (no text): {title[:50]}...")
        return None

    # Download text
    print(f"  Downloading: {title[:50]}...")
    content = download_text(identifier, text_file["name"], rate_limiter)

    if not content:
        return None

    # Length check
    if len(content) < args.min_length:
        print(f"    Too short: {len(content):,} chars")
        return None

    # OCR quality check
    ocr_quality = estimate_ocr_quality(content)
    combined_quality = (ocr_quality * 0.7) + (coll_score * 0.3)

    if combined_quality < args.min_quality:
        print(f"    Low quality: OCR={ocr_quality:.2f}, coll={coll_score:.2f}")
        return None

    # Save
    safe_id = re.sub(r"[^\w\-]", "_", identifier)[:100]
    filepath = output_dir / f"{safe_id}.txt"

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    word_count = len(content.split())
    print(f"    Saved: {word_count:,} words, quality={combined_quality:.2f}")

    # Return metadata
    return {
        "identifier": identifier,
        "title": title,
        "creator": creator,
        "date": doc.get("date", ""),
        "language": doc.get("language", ""),
        "collections": collections,
        "subjects": subjects[:10],  # Limit
        "content_type": content_type,
        "contributor": contributor,
        "scanner": scanner,
        "description": description[:500],  # Truncate
        "filepath": str(filepath.name),
        "file_size": filepath.stat().st_size,
        "word_count": word_count,
        "ocr_quality": round(ocr_quality, 3),
        "collection_score": round(coll_score, 3),
        "combined_quality": round(combined_quality, 3),
        "download_timestamp": datetime.now().isoformat(),
    }


def download_items(
    items: List[dict],
    output_dir: Path,
    rate_limiter: RateLimiter,
    existing: ExistingCorpus,
    args,
    max_downloads: int,
) -> tuple[List[dict], List[dict]]:
    """
    Download multiple items (either sequential or parallel based on args.workers).

    Returns: (successful_metadata_records, failed_items)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    metadata_records = []
    failed_items = []
    downloads_completed = 0

    if args.workers == 1:
        # Sequential mode
        print(f"\nDownloading {min(len(items), max_downloads)} items (sequential)...")
        for i, doc in enumerate(items[:max_downloads], 1):
            if downloads_completed >= max_downloads:
                break

            identifier = doc.get("identifier", "")
            try:
                result = download_single_item(doc, output_dir, rate_limiter, existing, args)
                if result:
                    metadata_records.append(result)
                    downloads_completed += 1
                    print(f"    [{downloads_completed}/{max_downloads}] ✓")
                else:
                    # Skipped for quality/duplicate/etc reasons (normal)
                    pass
            except BannedException:
                print(f"\n⚠️  Ban detected after {downloads_completed} downloads")
                print("  Saving progress and exiting gracefully")
                raise
            except Exception as e:
                print(f"    [{i}/{max_downloads}] ✗ {identifier}: {e}")
                failed_items.append({"identifier": identifier, "error": str(e)})

    else:
        # Parallel mode
        print(f"\nDownloading {min(len(items), max_downloads)} items ({args.workers} workers)...")

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            # Submit up to max_downloads items
            futures = {
                executor.submit(
                    download_single_item, doc, output_dir, rate_limiter, existing, args
                ): doc
                for doc in items[:max_downloads]
            }

            for future in as_completed(futures):
                if downloads_completed >= max_downloads:
                    break

                doc = futures[future]
                identifier = doc.get("identifier", "")
                try:
                    result = future.result()
                    if result:
                        metadata_records.append(result)
                        downloads_completed += 1
                        if downloads_completed <= 3 or downloads_completed % 10 == 0:
                            print(f"    [{downloads_completed}/{max_downloads}] ✓")
                except BannedException:
                    print(f"\n⚠️  Ban detected after {downloads_completed} downloads")
                    print("  Cancelling remaining downloads and saving progress")
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise
                except Exception as e:
                    print(f"    ✗ {identifier}: {e}")
                    failed_items.append({"identifier": identifier, "error": str(e)})

    print(f"\nDownload complete: {len(metadata_records)} successful, {len(failed_items)} failed")
    return metadata_records, failed_items


def main():
    parser = argparse.ArgumentParser(
        description="Download texts from Internet Archive",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Rate Limiting:
  This tool implements adaptive rate limiting starting at 2 seconds
  between requests. It will slow down on errors and speed up after
  consistent success. For large projects, contact info@archive.org.

Metadata Tracked:
  - collection: IA collection (for quality scoring)
  - content_type: inferred type (newspaper, book, etc.)
  - contributor: Source institution
  - scanner: Digitization equipment/center
  - ocr_quality: Estimated OCR accuracy

Examples:
  # Download pre-1914 newspapers
  python ia_collector.py --year-end 1914 --content-type newspaper -o ./newspapers

  # Focus on specific collection
  python ia_collector.py --collection americana --year-end 1900 -o ./americana

  # Dry run to see what's available
  python ia_collector.py --year-end 1914 --dry-run --max-items 100
        """,
    )

    parser.add_argument("-o", "--output", default="./corpus/ia")
    parser.add_argument(
        "--gutenberg-metadata", help="Path to Gutenberg metadata CSV for deduplication"
    )
    parser.add_argument("--year-end", type=int, default=1914)
    parser.add_argument("--year-start", type=int, default=1500)
    parser.add_argument("--language", default="eng")
    parser.add_argument("--collection", help="Specific IA collection")
    parser.add_argument(
        "--content-type",
        choices=["newspaper", "magazine", "government", "book", "any"],
        default="any",
        help="Filter by content type",
    )
    parser.add_argument("--min-quality", type=float, default=0.75)
    parser.add_argument("--min-length", type=int, default=10000)
    parser.add_argument(
        "--max-items",
        type=int,
        default=500,
        help="Maximum NEW items to download (excludes already downloaded)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel download workers (default: 1, sequential)",
    )
    parser.add_argument(
        "--base-delay", type=float, default=3.0, help="Base delay between requests (seconds)"
    )
    parser.add_argument(
        "--refresh", action="store_true", help="Refresh search cache (ignore cached results)"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    # Initialize
    rate_limiter = RateLimiter(base_delay=args.base_delay)
    existing = ExistingCorpus()

    if args.gutenberg_metadata:
        existing.add_from_gutenberg_metadata(Path(args.gutenberg_metadata))

    # Build search query
    query_parts = [
        f"date:[{args.year_start} TO {args.year_end}]",
        "mediatype:texts",
        f"language:{args.language}",
    ]

    if args.collection:
        query_parts.append(f"collection:{args.collection}")

    # Content type filtering via subject/title patterns
    if args.content_type == "newspaper":
        query_parts.append("(subject:newspaper OR title:newspaper OR title:gazette OR title:times)")
    elif args.content_type == "magazine":
        query_parts.append("(subject:periodical OR subject:magazine OR title:magazine)")
    elif args.content_type == "government":
        query_parts.append("(subject:government OR collection:us_government)")

    query = " AND ".join(query_parts)
    print(f"Search query: {query}")

    # Search fields - grab everything we need for metadata
    fields = [
        "identifier",
        "title",
        "creator",
        "date",
        "language",
        "collection",
        "subject",
        "description",
        "publisher",
        "contributor",
        "scanner",
        "mediatype",
        "downloads",
    ]

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ========================================================================
    # PHASE 1: SEARCH (with caching)
    # ========================================================================

    cache_path = get_cache_path(output_dir, args.content_type, args.year_end)
    all_items = None

    if not args.refresh and cache_path.exists():
        all_items = load_cached_search(cache_path)

    if all_items is None:
        try:
            all_items = search_all_items(query, fields, rate_limiter, args.verbose)
            # Save to cache for future runs
            save_search_cache(cache_path, all_items, query)
        except BannedException as e:
            print(f"\n⚠️  BANNED during search: {e}")
            print("  Wait a few hours and try resuming later")
            sys.exit(1)

    if not all_items:
        print("No items found matching criteria")
        return

    # ========================================================================
    # PHASE 2: FILTER & DOWNLOAD
    # ========================================================================

    # Count existing files for resume support
    existing_files = {f.stem for f in output_dir.glob("*.txt")}
    print(f"\nFound {len(existing_files)} already downloaded files")

    # Filter to items not yet downloaded
    items_to_download = []
    for doc in all_items:
        identifier = doc.get("identifier", "")
        # Convert identifier to filename format for comparison
        safe_id = re.sub(r"[^\w\-]", "_", identifier)[:100]
        if safe_id not in existing_files:
            items_to_download.append(doc)

    print(
        f"Items to download: {len(items_to_download)} NEW items (filtered from {len(all_items)} search results)"
    )

    if not items_to_download:
        print("No new items to download - all search results already exist locally")
        return

    # Limit to max_items
    if len(items_to_download) > args.max_items:
        print(
            f"Limiting download to {args.max_items} items (out of {len(items_to_download)} available)"
        )
        items_to_download = items_to_download[: args.max_items]

    # Handle dry-run
    if args.dry_run:
        print("\n=== DRY RUN - Items that would be downloaded ===")
        for i, doc in enumerate(items_to_download[:20], 1):
            title = doc.get("title", "Unknown")
            if isinstance(title, list):
                title = title[0]
            creator = doc.get("creator", "Unknown")
            if isinstance(creator, list):
                creator = creator[0]
            collections = doc.get("collection", [])
            if isinstance(collections, str):
                collections = [collections]
            coll_score = collection_quality_score(collections)
            print(f"  {i}. {title[:60]}")
            print(f"     Creator: {creator[:40]}, Quality: {coll_score:.2f}")
        if len(items_to_download) > 20:
            print(f"  ... and {len(items_to_download) - 20} more")
        return

    # Download items
    metadata_records = []
    failed_items = []

    try:
        metadata_records, failed_items = download_items(
            items_to_download,
            output_dir,
            rate_limiter,
            existing,
            args,
            args.max_items,
        )
    except BannedException as e:
        print("\n⚠️  BANNED during download:")
        print(f"  {e}")
        print(
            "  Progress saved. Resume later with the same command - already downloaded files will be skipped"
        )
        # Don't exit yet - save what we have

    # Save failed items for retry
    if failed_items:
        failed_file = output_dir / "metadata" / "failed_items.json"
        failed_file.parent.mkdir(parents=True, exist_ok=True)
        with open(failed_file, "w") as f:
            json.dump(
                {
                    "failed_at": datetime.now().isoformat(),
                    "count": len(failed_items),
                    "items": failed_items,
                },
                f,
                indent=2,
            )
        print(f"\n{len(failed_items)} failed items saved to {failed_file}")

    # Save metadata
    if metadata_records:
        metadata_file = output_dir / "metadata.json"
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "source": "Internet Archive",
                    "query": query,
                    "parameters": {
                        "year_range": [args.year_start, args.year_end],
                        "language": args.language,
                        "collection": args.collection,
                        "content_type": args.content_type,
                        "min_quality": args.min_quality,
                    },
                    "collection_timestamp": datetime.now().isoformat(),
                    "texts": metadata_records,
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        # Also save as CSV for easy analysis
        csv_file = output_dir / "metadata.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            if metadata_records:
                # Flatten collections/subjects for CSV
                flat_records = []
                for rec in metadata_records:
                    flat = rec.copy()
                    flat["collections"] = "; ".join(rec["collections"])
                    flat["subjects"] = "; ".join(rec["subjects"])
                    flat_records.append(flat)

                writer = csv.DictWriter(f, fieldnames=flat_records[0].keys())
                writer.writeheader()
                writer.writerows(flat_records)

    # Summary
    print(f"\n{'=' * 60}")
    print("Collection Summary")
    print(f"{'=' * 60}")
    print(f"  Total search results: {len(all_items):,}")
    print(f"  Already downloaded: {len(existing_files):,}")
    print(f"  Newly downloaded: {len(metadata_records):,}")
    print(f"  Failed downloads: {len(failed_items):,}")

    if metadata_records:
        by_type = {}
        for rec in metadata_records:
            t = rec["content_type"]
            by_type[t] = by_type.get(t, 0) + 1

        print("\nBy content type:")
        for t, count in sorted(by_type.items(), key=lambda x: -x[1]):
            print(f"  {t}: {count}")

        total_words = sum(r["word_count"] for r in metadata_records)
        avg_quality = sum(r["combined_quality"] for r in metadata_records) / len(metadata_records)
        print(f"\nTotal words: {total_words:,}")
        print(f"Average quality score: {avg_quality:.2f}")


if __name__ == "__main__":
    main()
