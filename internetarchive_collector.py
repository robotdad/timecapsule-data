#!/usr/bin/env python3
"""
Internet Archive Temporal Dataset Collector

Downloads texts from Internet Archive with temporal filtering.
Can work with:
  1. Pre-curated ID lists (like TimeCapsuleLLM's internet_archive_ids.txt)
  2. Automated search queries with date filtering

Usage:
    # From ID list
    python internetarchive_collector.py --ids ../TimeCapsuleLLM/internet_archive_ids.txt -o ./ia_corpus
    
    # From search query (requires API key for large queries)
    python internetarchive_collector.py --search "london" --date-range 1800-1875 -o ./ia_london
    
    # Estimate size before downloading
    python internetarchive_collector.py --search "london" --date-range 1800-1875 --estimate-only
"""

import argparse
import csv
import json
import os
import sys
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Internet Archive APIs
IA_SEARCH_URL = "https://archive.org/advancedsearch.php"
IA_METADATA_URL = "https://archive.org/metadata/{item_id}"
IA_DOWNLOAD_URL = "https://archive.org/download/{item_id}/{filename}"

DEFAULT_CONCURRENCY = 4
DEFAULT_TIMEOUT = 60
REQUEST_DELAY = 0.5


@dataclass
class IAItem:
    identifier: str
    title: str
    date: Optional[str] = None
    creator: Optional[str] = None
    mediatype: str = "texts"
    item_size: int = 0


def setup_logger(verbose: bool = False) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger("ia_collector")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def create_session(api_key: Optional[str] = None) -> requests.Session:
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1.0, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "TimeCapsuleLLM-Collector/1.0 (+https://github.com/haykgrigo3/TimeCapsuleLLM)"
    })
    if api_key:
        session.headers["Authorization"] = f"LOW {api_key}"
    return session


def search_archive(session: requests.Session, query: str, date_start: int, 
                   date_end: int, logger: logging.Logger, max_results: int = 10000) -> list:
    """
    Search Internet Archive for texts within date range.
    
    Uses Advanced Search API with date filtering.
    """
    # Build query with date range
    # IA uses 'date' field for publication date
    full_query = f'{query} AND mediatype:texts AND date:[{date_start}-01-01 TO {date_end}-12-31]'
    
    logger.info(f"Searching IA: {full_query}")
    
    items = []
    rows = 500  # Results per page
    page = 1
    
    while len(items) < max_results:
        params = {
            "q": full_query,
            "fl[]": ["identifier", "title", "date", "creator", "mediatype", "item_size"],
            "sort[]": "date asc",
            "rows": rows,
            "page": page,
            "output": "json",
        }
        
        try:
            resp = session.get(IA_SEARCH_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            
            docs = data.get("response", {}).get("docs", [])
            if not docs:
                break
                
            for doc in docs:
                items.append(IAItem(
                    identifier=doc.get("identifier", ""),
                    title=doc.get("title", "Unknown"),
                    date=doc.get("date"),
                    creator=doc.get("creator"),
                    mediatype=doc.get("mediatype", "texts"),
                    item_size=doc.get("item_size", 0),
                ))
            
            total = data.get("response", {}).get("numFound", 0)
            logger.info(f"  Page {page}: fetched {len(docs)}, total so far {len(items)}/{total}")
            
            if len(items) >= total:
                break
                
            page += 1
            time.sleep(REQUEST_DELAY)
            
        except Exception as e:
            logger.error(f"Search error on page {page}: {e}")
            break
    
    return items[:max_results]


def load_ids_from_file(filepath: Path, logger: logging.Logger) -> list:
    """Load item IDs from a text file (one per line)."""
    logger.info(f"Loading IDs from {filepath}")
    ids = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                ids.append(line)
    logger.info(f"Loaded {len(ids)} IDs")
    return [IAItem(identifier=id, title="") for id in ids]


def get_text_filename(session: requests.Session, item_id: str, 
                      logger: logging.Logger) -> Optional[str]:
    """Find the best text file for an item."""
    candidates = [
        f"{item_id}_djvu.txt",
        f"{item_id}.txt",
        f"{item_id}_ocr.txt",
        f"{item_id}-text.txt",
    ]
    
    # First try common patterns
    for filename in candidates:
        return filename  # Return first candidate to try
    
    return None


def download_item(item: IAItem, session: requests.Session, output_dir: Path,
                  logger: logging.Logger) -> tuple:
    """Download text for an item. Returns (id, success, message, size)."""
    
    candidates = [
        f"{item.identifier}_djvu.txt",
        f"{item.identifier}.txt",
        f"{item.identifier}_ocr.txt",
        f"{item.identifier}-text.txt",
    ]
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for filename in candidates:
        dest = output_dir / f"{item.identifier}.txt"
        
        if dest.exists() and dest.stat().st_size > 100:
            return (item.identifier, True, "skipped (exists)", dest.stat().st_size)
        
        url = IA_DOWNLOAD_URL.format(item_id=item.identifier, filename=filename)
        
        try:
            resp = session.get(url, timeout=DEFAULT_TIMEOUT, stream=True)
            content_type = resp.headers.get("Content-Type", "").lower()
            
            if resp.status_code == 200 and "text" in content_type:
                content = resp.text
                
                # Basic cleaning - remove IA boilerplate if present
                content = clean_ia_text(content)
                
                if len(content) < 500:
                    continue
                
                dest.write_text(content, encoding='utf-8')
                time.sleep(REQUEST_DELAY)
                return (item.identifier, True, f"downloaded ({len(content):,} chars)", len(content))
                
        except Exception as e:
            logger.debug(f"Failed {url}: {e}")
            continue
    
    return (item.identifier, False, "no text file found", 0)


def clean_ia_text(text: str) -> str:
    """Basic cleaning of Internet Archive text files."""
    import re
    
    # Remove common IA headers/footers
    # These vary by digitization source, so we're conservative
    
    # Remove lines with archive.org references
    lines = text.split('\n')
    clean_lines = []
    for line in lines:
        if 'archive.org' in line.lower():
            continue
        if 'internet archive' in line.lower() and len(line) < 200:
            continue
        if 'digitized by' in line.lower() and len(line) < 200:
            continue
        clean_lines.append(line)
    
    return '\n'.join(clean_lines)


def estimate_corpus_size(items: list, logger: logging.Logger):
    """Estimate total download size."""
    total_bytes = sum(item.item_size for item in items if item.item_size)
    items_with_size = sum(1 for item in items if item.item_size)
    
    logger.info(f"\n{'='*60}")
    logger.info("CORPUS SIZE ESTIMATE")
    logger.info(f"{'='*60}")
    logger.info(f"Items found: {len(items)}")
    logger.info(f"Items with size info: {items_with_size}")
    
    if items_with_size > 0:
        avg_size = total_bytes / items_with_size
        estimated_total = avg_size * len(items)
        logger.info(f"Total reported size: {total_bytes / 1024**3:.2f} GB")
        logger.info(f"Average item size: {avg_size / 1024**2:.2f} MB")
        logger.info(f"Estimated corpus size: {estimated_total / 1024**3:.2f} GB")
        logger.info(f"\nNote: Actual text files are typically 5-20% of total item size")
        logger.info(f"Expected text size: {estimated_total * 0.1 / 1024**3:.2f} - {estimated_total * 0.2 / 1024**3:.2f} GB")
    else:
        logger.info("No size information available - estimates not possible")


def collect_corpus(items: list, output_dir: Path, concurrency: int, 
                   limit: Optional[int], logger: logging.Logger):
    """Download all items in the list."""
    
    if limit:
        items = items[:limit]
        logger.info(f"Limited to {limit} items")
    
    logger.info(f"Downloading {len(items)} items with {concurrency} workers...")
    
    session = create_session()
    results = {"success": 0, "failed": 0, "skipped": 0}
    total_size = 0
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(download_item, item, session, output_dir, logger): item
            for item in items
        }
        
        for i, future in enumerate(as_completed(futures)):
            item = futures[future]
            try:
                item_id, success, message, size = future.result()
                if success:
                    if "skipped" in message:
                        results["skipped"] += 1
                    else:
                        results["success"] += 1
                    total_size += size
                else:
                    results["failed"] += 1
                
                if (i + 1) % 100 == 0:
                    logger.info(f"Progress: {i+1}/{len(items)} ({total_size/1024**2:.1f} MB)")
                    
            except Exception as e:
                logger.error(f"Item {item.identifier}: {e}")
                results["failed"] += 1
    
    # Write metadata
    metadata_file = output_dir / "metadata.csv"
    with open(metadata_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["identifier", "title", "date", "creator", "downloaded"])
        for item in items:
            downloaded = (output_dir / f"{item.identifier}.txt").exists()
            writer.writerow([item.identifier, item.title, item.date, 
                           item.creator, downloaded])
    
    logger.info(f"\nComplete!")
    logger.info(f"  Success: {results['success']}")
    logger.info(f"  Skipped: {results['skipped']}")
    logger.info(f"  Failed: {results['failed']}")
    logger.info(f"  Total size: {total_size/1024**2:.1f} MB")
    logger.info(f"Metadata: {metadata_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Collect texts from Internet Archive",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # From pre-curated ID list
  python internetarchive_collector.py --ids ids.txt -o ./corpus
  
  # Search for London texts 1800-1875
  python internetarchive_collector.py --search "london" --date-range 1800-1875 -o ./london
  
  # Estimate size before downloading
  python internetarchive_collector.py --search "london" --date-range 1800-1875 --estimate-only
  
  # Test with limit
  python internetarchive_collector.py --ids ids.txt --limit 50 -o ./test_corpus
        """)
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--ids", "-i", type=Path, help="Path to ID list file")
    group.add_argument("--search", "-s", type=str, help="Search query")
    
    parser.add_argument("--date-range", "-d", type=str, 
                       help="Date range YYYY-YYYY (for search mode)")
    parser.add_argument("--output-dir", "-o", type=Path, default=Path("./ia_corpus"))
    parser.add_argument("--concurrency", "-c", type=int, default=4)
    parser.add_argument("--limit", type=int, help="Limit items (for testing)")
    parser.add_argument("--estimate-only", "-e", action="store_true",
                       help="Only estimate size, don't download")
    parser.add_argument("--verbose", "-v", action="store_true")
    
    args = parser.parse_args()
    logger = setup_logger(args.verbose)
    
    logger.info("=" * 60)
    logger.info("Internet Archive Temporal Dataset Collector")
    logger.info("=" * 60)
    
    session = create_session()
    
    # Get items either from file or search
    if args.ids:
        items = load_ids_from_file(args.ids, logger)
    else:
        if not args.date_range:
            logger.error("--date-range required for search mode")
            sys.exit(1)
        
        try:
            date_start, date_end = map(int, args.date_range.split('-'))
        except ValueError:
            logger.error("Invalid date range format. Use YYYY-YYYY")
            sys.exit(1)
        
        items = search_archive(session, args.search, date_start, date_end, 
                             logger, max_results=args.limit or 10000)
    
    if not items:
        logger.error("No items found")
        sys.exit(1)
    
    if args.estimate_only:
        estimate_corpus_size(items, logger)
    else:
        collect_corpus(items, args.output_dir, args.concurrency, args.limit, logger)


if __name__ == "__main__":
    main()
