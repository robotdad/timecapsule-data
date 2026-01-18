#!/usr/bin/env python3
"""
Internet Archive Index Builder

Builds a complete catalog of all IA items in a date range using the Scraping API.
This replaces the search-as-you-go approach with a one-time index build.

The Scraping API uses cursor-based pagination with NO result limits, allowing us
to retrieve all 2.3M+ items without hitting the 10k pagination wall.

Usage:
    tc-ia-index --year-start 1800 --year-end 1914 -o /path/to/corpus/

Creates: corpus/metadata/ia_index_1800_1914.json

The index contains ALL metadata needed for filtering and downloading,
eliminating the need for per-item metadata API calls.
"""

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def scrape_batch(query, fields, cursor=None, count=10000, timeout=300):
    """
    Fetch a batch using IA's Scraping API (cursor-based, no 10k limit).

    Args:
        query: Search query string
        fields: Comma-separated field names
        cursor: Cursor from previous batch (None for first batch)
        count: Number of results per request (minimum 100)
        timeout: Request timeout in seconds

    Returns:
        dict with 'items', 'total', 'count', 'cursor' (or None on error)
    """
    # Build parameters
    params = {
        "q": query,
        "fields": fields,
        "count": count,
    }
    if cursor:
        params["cursor"] = cursor

    url = f"https://archive.org/services/search/v1/scrape?{urlencode(params)}"

    time.sleep(2)  # Rate limit - be respectful

    try:
        req = Request(url, headers={"User-Agent": "TimeCapsuleLLM-Research/1.0"})
        with urlopen(req, timeout=timeout) as response:
            data = json.loads(response.read().decode("utf-8"))
            return data
    except (HTTPError, URLError, TimeoutError) as e:
        print(f"  Error: {e}")
        return None


def build_index(year_start, year_end, output_dir, batch_size=10000):
    """
    Build complete IA index for date range using cursor-based scraping.

    Args:
        year_start: Start year (e.g., 1800)
        year_end: End year (e.g., 1914)
        output_dir: Base output directory
        batch_size: Items per request (default 10000, min 100)
    """
    # Build query
    query = (
        f"date:[{year_start} TO {year_end}] "
        f"AND mediatype:texts "
        f"AND language:eng "
        f'AND (format:DjVu OR format:Text OR format:"Abbyy GZ")'
    )

    # Request fields (comma-separated for Scraping API)
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

    print("=" * 80)
    print("INTERNET ARCHIVE INDEX BUILDER (Scraping API)")
    print("=" * 80)
    print(f"Date range: {year_start}-{year_end}")
    print(f"Query: {query}")
    print()

    # First request to get total count
    print("Fetching first batch to determine total size...")
    first_batch = scrape_batch(query, fields, cursor=None, count=batch_size)

    if not first_batch:
        print("Failed to fetch first batch")
        return None

    total_found = first_batch.get("total", 0)
    items = first_batch.get("items", [])
    cursor = first_batch.get("cursor")

    print(f"  Total items in IA: {total_found:,}")
    print(f"  Batch size: {batch_size:,}")
    estimated_requests = (total_found // batch_size) + 1
    print(f"  Estimated requests: {estimated_requests}")
    print()

    if total_found == 0:
        print("No items found matching query")
        return None

    all_items = items.copy()
    batch_num = 1

    # Prepare index file path
    index_dir = Path(output_dir) / "metadata"
    index_dir.mkdir(parents=True, exist_ok=True)
    index_file = index_dir / f"ia_index_{year_start}_{year_end}.json"

    print("Scraping with cursor-based pagination...")
    print(f"Writing to: {index_file}")
    print()

    # Continue fetching until no more cursor
    while cursor:
        batch_num += 1
        batch = scrape_batch(query, fields, cursor=cursor, count=batch_size)

        if not batch:
            print(f"  Failed to fetch batch {batch_num}, stopping")
            break

        items_in_batch = len(batch.get("items", []))
        all_items.extend(batch.get("items", []))
        cursor = batch.get("cursor")

        # Write to disk after EVERY batch (resume support)
        index_data = {
            "query": query,
            "date_range": [year_start, year_end],
            "exported_at": datetime.now().isoformat(),
            "total_found": total_found,
            "total_exported": len(all_items),
            "items": all_items,
        }

        with open(index_file, "w") as f:
            json.dump(index_data, f, indent=2)

        # Progress reporting
        if batch_num % 10 == 0 or batch_num <= 5:
            pct = len(all_items) / total_found * 100 if total_found > 0 else 0
            file_size = index_file.stat().st_size / 1024 / 1024
            remaining_items = total_found - len(all_items)
            remaining_batches = remaining_items // batch_size if batch_size > 0 else 0
            eta_seconds = remaining_batches * 3  # Rough estimate with 2s delay + 1s request
            eta_minutes = eta_seconds // 60
            print(
                f"  Batch {batch_num} - got {items_in_batch} items - "
                f"TOTAL: {len(all_items):,}/{total_found:,} ({pct:.1f}%) - "
                f"{file_size:.1f} MB - ETA: {eta_minutes}m"
            )

        if items_in_batch == 0:
            print(f"  Batch {batch_num} returned 0 items - stopping")
            break

        # Safety: stop if we've gotten everything
        if len(all_items) >= total_found:
            break

    print()
    print(f"✓ Export complete: {len(all_items):,} items retrieved")

    # Add enrichment fields to each item
    for item in all_items:
        item["text_filename"] = None
        item["enriched_at"] = None
        item["quality_score"] = None  # Will be calculated during enrichment

    # Final save
    index_data = {
        "query": query,
        "date_range": [year_start, year_end],
        "exported_at": datetime.now().isoformat(),
        "total_found": total_found,
        "total_exported": len(all_items),
        "enrichment_status": {
            "total_enriched": 0,
            "last_enriched_at": None,
            "quality_thresholds_completed": [],
        },
        "items": all_items,
    }

    print(f"Saving final index to {index_file}...")
    with open(index_file, "w") as f:
        json.dump(index_data, f, indent=2)

    print(f"✓ Index saved: {index_file.stat().st_size / 1024 / 1024:.1f} MB")

    # Analyze what we got
    print()
    print("=" * 80)
    print("INDEX ANALYSIS")
    print("=" * 80)

    # Year distribution
    year_counts = {}
    for item in all_items:
        year = item.get("year")
        if year:
            year_counts[year] = year_counts.get(year, 0) + 1

    if year_counts:
        print(f"Year range in results: {min(year_counts.keys())} - {max(year_counts.keys())}")
        print(f"Items with year field: {sum(year_counts.values()):,} / {len(all_items):,}")
    print()

    # Format analysis
    text_formats = ["DjVuTXT", "Text PDF", "Abbyy GZ", "hOCR", "OCR Search Text"]
    has_text = 0
    no_text = 0

    for item in all_items:
        formats = item.get("format", [])
        if isinstance(formats, str):
            formats = [formats]

        if any(fmt in text_formats for fmt in formats):
            has_text += 1
        else:
            no_text += 1

    print(f"Items with text formats: {has_text:,}")
    print(f"Items without text formats: {no_text:,}")
    print()

    # Quality estimation (collection-based heuristic)
    # NOTE: This is a PROXY based on source collection, not actual OCR quality.
    # Real OCR quality requires downloading text and analyzing it.
    # Known collections from empirical analysis:
    quality_collections = {
        # Major institutional libraries (excellent quality)
        "americana": 0.9,  # American Libraries - generally excellent
        "library_of_congress": 0.9,  # Library of Congress - excellent
        # Academic/University libraries (high quality)
        "toronto": 0.85,  # University of Toronto
        "europeanlibraries": 0.85,  # European Libraries
        "jstor": 0.85,  # JSTOR academic journals (all jstor_* collections)
        # National/research libraries (good quality)
        "blc": 0.8,  # British Library
        "bplscas": 0.8,  # Boston Public Library
        "biodiversity": 0.8,  # Biodiversity Heritage Library
        "medicalheritage": 0.8,  # Medical Heritage Library
        "digitallibraryindia": 0.75,  # Digital Library of India
        # Newspaper archives (variable but generally usable)
        "newspaper": 0.7,  # Catches: newspapers, newspaperarchive, kentuckynewspapers, etc.
        # General book collections (moderate quality)
        "internetarchivebooks": 0.65,  # General IA book uploads
        "jaigyan": 0.65,  # JaiGyan digital library
        "journal": 0.65,  # General journals collection
        # User/community uploads (unknown quality - low default)
        "opensource": 0.5,  # Community contributions
        "folkscanomy": 0.5,  # User scanning projects
    }

    quality_scores = []
    collection_matches = {}  # Track which collections items belong to

    for item in all_items:
        collections = item.get("collection", [])
        if isinstance(collections, str):
            collections = [collections]

        best_score = 0.5  # Default for unknown collections
        matched_collection = "unknown"

        for coll in collections:
            coll_lower = coll.lower()
            for known, score in quality_collections.items():
                if known in coll_lower:
                    if score > best_score:
                        best_score = score
                        matched_collection = known

        quality_scores.append(best_score)
        collection_matches[matched_collection] = collection_matches.get(matched_collection, 0) + 1

    # Quality distribution (bidirectional view)
    thresholds = [0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9]
    print("Quality score distribution (collection-based heuristic):")
    print("  NOTE: This is based on source collection, NOT actual OCR quality.")
    print("  Real OCR analysis happens during download when text is available.")
    print()

    for threshold in thresholds:
        count_above = sum(1 for s in quality_scores if s >= threshold)
        count_below = sum(1 for s in quality_scores if s < threshold)
        pct_above = count_above / len(all_items) * 100 if all_items else 0
        pct_below = count_below / len(all_items) * 100 if all_items else 0
        print(
            f"  {threshold:.2f}: {count_above:7,} above ({pct_above:5.1f}%) | {count_below:7,} below ({pct_below:5.1f}%)"
        )

    print()
    print("Collection breakdown (top collections):")
    sorted_collections = sorted(collection_matches.items(), key=lambda x: -x[1])[:10]
    for coll_name, count in sorted_collections:
        pct = count / len(all_items) * 100 if all_items else 0
        score = quality_collections.get(coll_name, 0.5)
        print(f"  {coll_name:20s} (score={score:.2f}): {count:7,} items ({pct:5.1f}%)")

    print()
    print("✓ Index build complete!")
    print(f"✓ Saved to: {index_file}")

    return index_file


def main():
    parser = argparse.ArgumentParser(
        description="Build IA item index using cursor-based Scraping API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build index for 1800-1914
  tc-ia-index --year-start 1800 --year-end 1914 -o /path/to/corpus

  # Refresh existing index
  tc-ia-index --year-start 1800 --year-end 1914 --refresh -o /path/to/corpus

Notes:
  - Uses the Scraping API (cursor-based, NO 10k limit)
  - Can retrieve ALL 2.3M+ items without pagination issues
  - Writes progress after every batch (safe to interrupt)
        """,
    )

    parser.add_argument("-o", "--output", required=True, help="Output directory (corpus base)")
    parser.add_argument("--year-start", type=int, default=1800, help="Start year (default: 1800)")
    parser.add_argument("--year-end", type=int, default=1914, help="End year (default: 1914)")
    parser.add_argument("--refresh", action="store_true", help="Rebuild index even if exists")
    parser.add_argument(
        "--batch-size", type=int, default=10000, help="Items per request (default: 10000, min: 100)"
    )

    args = parser.parse_args()

    # Validate batch size
    if args.batch_size < 100:
        print("Error: batch-size must be at least 100")
        sys.exit(1)

    # Check if index already exists
    index_file = Path(args.output) / "metadata" / f"ia_index_{args.year_start}_{args.year_end}.json"

    if index_file.exists() and not args.refresh:
        print(f"Index already exists: {index_file}")
        print("Use --refresh to rebuild")

        # Load and show summary
        with open(index_file) as f:
            data = json.load(f)

        print()
        print("Existing index:")
        print(f"  Created: {data.get('exported_at', 'unknown')}")
        print(f"  Items: {data.get('total_exported', 0):,}")
        print(f"  File size: {index_file.stat().st_size / 1024 / 1024:.1f} MB")
        return

    # Build index
    result = build_index(args.year_start, args.year_end, args.output, args.batch_size)

    if not result:
        sys.exit(1)


if __name__ == "__main__":
    main()
