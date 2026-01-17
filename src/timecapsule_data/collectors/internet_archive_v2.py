#!/usr/bin/env python3
"""
Internet Archive Collector v2 - Using Official Library

Uses the official `internetarchive` library for:
- Proper rate limit handling
- Automatic retries
- Parallel downloads
- Better API coverage
"""

import argparse
import csv
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import internetarchive as ia

# Import our unified schema
try:
    from ..corpus_schema import TextMetadata, CorpusMetadataWriter
except ImportError:
    TextMetadata = None
    CorpusMetadataWriter = None


# Quality scoring based on collection
COLLECTION_QUALITY = {
    'gutenberg': 0.95,
    'americana': 0.90,
    'biodiversity': 0.85,
    'medicalheritagelibrary': 0.85,
    'europeanlibraries': 0.80,
    'toronto': 0.80,
    'cdl': 0.75,
    'internetarchivebooks': 0.70,
    'microfilm': 0.50,
    'additional_collections': 0.50,
}


@dataclass
class IAItem:
    """Represents an Internet Archive item."""
    identifier: str
    title: str
    creator: str
    date: str
    year: Optional[int]
    language: str
    mediatype: str
    collections: list[str]
    subject: list[str]
    quality_score: float
    source_type: str  # newspaper, book, magazine, etc.


def estimate_quality(item: dict) -> float:
    """Estimate quality based on collection membership."""
    collections = item.get('collection', [])
    if isinstance(collections, str):
        collections = [collections]
    
    best_score = 0.5
    for coll in collections:
        coll_lower = coll.lower()
        for key, score in COLLECTION_QUALITY.items():
            if key in coll_lower:
                best_score = max(best_score, score)
    
    return best_score


def parse_year(date_str: str) -> Optional[int]:
    """Extract year from various date formats."""
    if not date_str:
        return None
    
    import re
    # Try to find a 4-digit year
    match = re.search(r'\b(1[0-9]{3})\b', str(date_str))
    if match:
        return int(match.group(1))
    return None


def search_items(
    year_start: int = 1500,
    year_end: int = 1914,
    language: str = "eng",
    content_type: Optional[str] = None,
    min_quality: float = 0.6,
    max_items: int = 0,
    exclude_ids: Optional[set] = None,
) -> list[IAItem]:
    """
    Search Internet Archive for items matching criteria.
    
    Args:
        year_start: Earliest publication year
        year_end: Latest publication year
        language: Language code
        content_type: Filter by type (newspaper, book, etc.)
        min_quality: Minimum quality score threshold
        max_items: Maximum items to return (0 = unlimited)
        exclude_ids: Set of identifiers to skip (e.g., already in Gutenberg)
    """
    exclude_ids = exclude_ids or set()
    
    # Build search query
    query_parts = [
        f"date:[{year_start} TO {year_end}]",
        "mediatype:texts",
        f"language:{language}",
    ]
    
    if content_type:
        if content_type == "newspaper":
            query_parts.append("(subject:newspaper OR title:newspaper OR title:gazette OR title:times)")
        elif content_type == "book":
            query_parts.append("NOT (subject:newspaper OR subject:magazine OR subject:periodical)")
    
    query = " AND ".join(query_parts)
    print(f"Search query: {query}")
    
    # Use official library search
    search = ia.search_items(query, fields=[
        'identifier', 'title', 'creator', 'date', 'language',
        'mediatype', 'collection', 'subject'
    ])
    
    items = []
    seen = 0
    
    print(f"\nSearching Internet Archive...")
    
    for result in search:
        seen += 1
        
        identifier = result.get('identifier', '')
        
        # Skip if in exclude list
        if identifier in exclude_ids:
            continue
        
        # Parse and filter by year
        date_str = result.get('date', '')
        year = parse_year(date_str)
        if year and (year < year_start or year > year_end):
            continue
        
        # Estimate quality
        quality = estimate_quality(result)
        if quality < min_quality:
            continue
        
        # Determine source type
        subjects = result.get('subject', [])
        if isinstance(subjects, str):
            subjects = [subjects]
        
        source_type = "unknown"
        title_lower = result.get('title', '').lower()
        subjects_lower = ' '.join(str(s).lower() for s in subjects)
        
        if 'newspaper' in subjects_lower or 'newspaper' in title_lower:
            source_type = "newspaper"
        elif 'magazine' in subjects_lower or 'periodical' in subjects_lower:
            source_type = "magazine"
        else:
            source_type = "book"
        
        collections = result.get('collection', [])
        if isinstance(collections, str):
            collections = [collections]
        
        item = IAItem(
            identifier=identifier,
            title=result.get('title', 'Unknown'),
            creator=result.get('creator', 'Unknown'),
            date=date_str,
            year=year,
            language=result.get('language', language),
            mediatype=result.get('mediatype', 'texts'),
            collections=collections,
            subject=subjects,
            quality_score=quality,
            source_type=source_type,
        )
        
        items.append(item)
        
        if seen % 100 == 0:
            print(f"  Scanned {seen} items, {len(items)} passed filters...")
        
        if max_items > 0 and len(items) >= max_items:
            break
    
    print(f"\nFound {len(items)} items matching criteria (scanned {seen})")
    return items


def get_text_file(identifier: str) -> Optional[str]:
    """Get the best text file for an item."""
    try:
        item = ia.get_item(identifier)
        
        # Look for text files in preference order
        text_files = []
        for f in item.files:
            name = f.get('name', '')
            if name.endswith('_djvu.txt'):
                text_files.append((name, 1))  # Highest priority
            elif name.endswith('.txt') and not name.endswith('_meta.txt'):
                text_files.append((name, 2))
        
        if not text_files:
            return None
        
        # Sort by priority
        text_files.sort(key=lambda x: x[1])
        return text_files[0][0]
    
    except Exception as e:
        print(f"  Error getting files for {identifier}: {e}")
        return None


def download_item(item: IAItem, output_dir: Path, delay: float = 0.3) -> Optional[dict]:
    """
    Download text content for an item.
    
    Uses the official library which handles rate limits automatically.
    """
    try:
        # Find text file
        text_filename = get_text_file(item.identifier)
        if not text_filename:
            return None
        
        # Download using official library
        ia_item = ia.get_item(item.identifier)
        text_file = ia_item.get_file(text_filename)
        
        if not text_file:
            return None
        
        # Download content
        content = text_file.download(return_responses=True)
        if hasattr(content, 'text'):
            text = content.text
        else:
            # It's a generator of responses
            text = b''.join(r.content for r in content).decode('utf-8', errors='replace')
        
        if not text or len(text) < 100:
            return None
        
        # Save to output
        safe_id = item.identifier.replace('/', '_')
        out_path = output_dir / f"{safe_id}.txt"
        out_path.write_text(text, encoding='utf-8')
        
        # Small delay between items (library handles rate limits, but be polite)
        time.sleep(delay)
        
        return {
            'identifier': item.identifier,
            'title': item.title,
            'creator': item.creator,
            'date': item.date,
            'year': item.year,
            'language': item.language,
            'source_type': item.source_type,
            'quality_score': item.quality_score,
            'collections': item.collections,
            'file': str(out_path),
            'size_bytes': len(text),
        }
    
    except Exception as e:
        print(f"  Error downloading {item.identifier}: {e}")
        return None


def download_parallel(
    items: list[IAItem],
    output_dir: Path,
    workers: int = 4,
    delay: float = 0.3,
) -> list[dict]:
    """Download multiple items in parallel."""
    results = []
    failed = 0
    
    print(f"\nDownloading {len(items)} items with {workers} workers...")
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(download_item, item, output_dir, delay): item
            for item in items
        }
        
        for i, future in enumerate(as_completed(futures), 1):
            item = futures[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                    print(f"  [{i}/{len(items)}] ✓ {item.title[:50]}...")
                else:
                    failed += 1
                    print(f"  [{i}/{len(items)}] ✗ {item.title[:50]} (no text)")
            except Exception as e:
                failed += 1
                print(f"  [{i}/{len(items)}] ✗ {item.title[:50]}: {e}")
    
    print(f"\nDownloaded {len(results)} items, {failed} failed")
    return results


def load_gutenberg_ids(metadata_path: Path) -> set[str]:
    """Load Gutenberg identifiers to exclude from IA search."""
    if not metadata_path.exists():
        return set()
    
    ids = set()
    with open(metadata_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Gutenberg titles/authors could be used for fuzzy matching
            # For now, we don't have direct ID mapping
            pass
    
    return ids


def main():
    parser = argparse.ArgumentParser(
        description='Download texts from Internet Archive (v2 - official library)'
    )
    parser.add_argument('-o', '--output', default='./corpus/ia',
                        help='Output directory')
    parser.add_argument('--year-start', type=int, default=1500,
                        help='Earliest year (default: 1500)')
    parser.add_argument('--year-end', type=int, default=1914,
                        help='Latest year (default: 1914)')
    parser.add_argument('--language', default='eng',
                        help='Language code (default: eng)')
    parser.add_argument('--content-type', choices=['newspaper', 'book', 'all'],
                        help='Filter by content type')
    parser.add_argument('--min-quality', type=float, default=0.6,
                        help='Minimum quality score (default: 0.6)')
    parser.add_argument('--max-items', type=int, default=0,
                        help='Maximum items to download (0=unlimited)')
    parser.add_argument('--workers', type=int, default=4,
                        help='Parallel download workers (default: 4)')
    parser.add_argument('--delay', type=float, default=0.3,
                        help='Delay between downloads in seconds (default: 0.3)')
    parser.add_argument('--gutenberg-metadata', type=Path,
                        help='Path to Gutenberg metadata.csv to exclude duplicates')
    parser.add_argument('--dry-run', action='store_true',
                        help='Search only, do not download')
    
    args = parser.parse_args()
    
    # Load exclusion list
    exclude_ids = set()
    if args.gutenberg_metadata:
        exclude_ids = load_gutenberg_ids(args.gutenberg_metadata)
        if exclude_ids:
            print(f"Loaded {len(exclude_ids)} Gutenberg IDs to exclude")
    
    # Search
    content_type = args.content_type if args.content_type != 'all' else None
    items = search_items(
        year_start=args.year_start,
        year_end=args.year_end,
        language=args.language,
        content_type=content_type,
        min_quality=args.min_quality,
        max_items=args.max_items,
        exclude_ids=exclude_ids,
    )
    
    if not items:
        print("No items found matching criteria")
        return
    
    if args.dry_run:
        print("\n=== DRY RUN - Items that would be downloaded ===")
        for item in items[:20]:
            print(f"  [{item.source_type}] {item.title[:60]}...")
            print(f"      Year: {item.year}, Quality: {item.quality_score:.2f}")
        if len(items) > 20:
            print(f"  ... and {len(items) - 20} more")
        return
    
    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Download
    results = download_parallel(
        items, output_dir,
        workers=args.workers,
        delay=args.delay,
    )
    
    # Save metadata
    if results:
        metadata_path = output_dir / 'metadata.json'
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        print(f"\nMetadata saved to {metadata_path}")
        
        # Also save CSV for compatibility
        csv_path = output_dir / 'metadata.csv'
        with open(csv_path, 'w', encoding='utf-8', newline='') as f:
            if results:
                writer = csv.DictWriter(f, fieldnames=[
                    'identifier', 'title', 'creator', 'date', 'year',
                    'language', 'source_type', 'quality_score', 'file', 'size_bytes'
                ])
                writer.writeheader()
                for r in results:
                    row = {k: v for k, v in r.items() if k != 'collections'}
                    writer.writerow(row)
        print(f"CSV metadata saved to {csv_path}")


if __name__ == '__main__':
    main()
