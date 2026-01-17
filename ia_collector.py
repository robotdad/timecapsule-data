#!/usr/bin/env python3
"""
Internet Archive Collector with Pre-Download Filtering

Key insight: Check against existing corpus BEFORE downloading.
Don't fetch something we already have from Gutenberg.

Pipeline:
1. Load existing corpus metadata (Gutenberg titles/authors)
2. Query IA with filters (date, language, format)
3. Skip items that match existing corpus
4. Download only unique items
5. Apply OCR quality filter post-download
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Set
from urllib.parse import quote
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError


IA_SEARCH_API = "https://archive.org/advancedsearch.php"
IA_METADATA_API = "https://archive.org/metadata"
IA_DOWNLOAD_BASE = "https://archive.org/download"


@dataclass
class ExistingCorpus:
    """Track what we already have to avoid re-downloading."""
    titles: Set[str] = field(default_factory=set)           # Normalized titles
    title_author_pairs: Set[tuple] = field(default_factory=set)  # (title, author) tuples
    content_hashes: Set[str] = field(default_factory=set)   # MD5 hashes
    
    def normalize_title(self, title: str) -> str:
        """Normalize title for comparison."""
        # Lowercase, remove punctuation, collapse whitespace
        title = title.lower()
        title = re.sub(r'[^\w\s]', '', title)
        title = re.sub(r'\s+', ' ', title).strip()
        # Remove common prefixes/suffixes
        title = re.sub(r'^(the|a|an)\s+', '', title)
        title = re.sub(r'\s+(a|the)\s+.*$', '', title)  # "X: A Novel" -> "X"
        return title
    
    def normalize_author(self, author: str) -> str:
        """Normalize author for comparison."""
        author = author.lower()
        author = re.sub(r'[^\w\s]', '', author)
        # Handle "Last, First" -> "first last"
        if ',' in author:
            parts = author.split(',', 1)
            author = f"{parts[1].strip()} {parts[0].strip()}"
        author = re.sub(r'\s+', ' ', author).strip()
        return author
    
    def add_from_gutenberg_metadata(self, metadata_path: Path):
        """Load existing Gutenberg corpus metadata."""
        if not metadata_path.exists():
            print(f"Warning: Gutenberg metadata not found at {metadata_path}")
            return
        
        with open(metadata_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                title = self.normalize_title(row.get('title', ''))
                if title:
                    self.titles.add(title)
                
                authors = row.get('authors', '')
                for author in authors.split(';'):
                    author = self.normalize_author(author)
                    if author and title:
                        self.title_author_pairs.add((title, author))
        
        print(f"Loaded {len(self.titles)} titles, {len(self.title_author_pairs)} title-author pairs from Gutenberg")
    
    def is_duplicate(self, title: str, author: str = "") -> bool:
        """Check if this item is likely a duplicate."""
        norm_title = self.normalize_title(title)
        norm_author = self.normalize_author(author) if author else ""
        
        # Exact title match
        if norm_title in self.titles:
            return True
        
        # Title + author match (more permissive on title)
        if norm_author:
            for existing_title, existing_author in self.title_author_pairs:
                # Check if authors match and titles are similar
                if existing_author == norm_author:
                    # Allow partial title match
                    if norm_title in existing_title or existing_title in norm_title:
                        return True
        
        return False


def fetch_json(url: str, retries: int = 3) -> dict:
    """Fetch JSON from URL with retries."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={'User-Agent': 'TimeCapsuleCollector/1.0 (research)'})
            with urlopen(req, timeout=60) as response:
                return json.loads(response.read().decode('utf-8'))
        except (HTTPError, URLError, json.JSONDecodeError) as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
    return {}


def search_ia(query: str, fields: list, rows: int = 100, page: int = 1) -> dict:
    """Search Internet Archive."""
    params = {
        'q': query,
        'fl[]': fields,
        'rows': rows,
        'page': page,
        'output': 'json',
    }
    
    # Build URL
    field_params = '&'.join(f'fl[]={f}' for f in fields)
    url = f"{IA_SEARCH_API}?q={quote(query)}&{field_params}&rows={rows}&page={page}&output=json"
    
    return fetch_json(url)


def get_text_url(identifier: str) -> Optional[str]:
    """Get the best text file URL for an item."""
    url = f"{IA_METADATA_API}/{identifier}"
    try:
        metadata = fetch_json(url)
        files = metadata.get('files', [])
        
        # Prefer plain text, then djvu text
        for fmt in ['_djvu.txt', '.txt']:
            for f in files:
                name = f.get('name', '')
                if name.endswith(fmt) and f.get('format') in ['DjVuTXT', 'Text']:
                    return f"{IA_DOWNLOAD_BASE}/{identifier}/{name}"
        
        return None
    except Exception:
        return None


def download_text(url: str, retries: int = 3) -> str:
    """Download text content."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={'User-Agent': 'TimeCapsuleCollector/1.0'})
            with urlopen(req, timeout=120) as response:
                return response.read().decode('utf-8', errors='replace')
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
    return ""


def estimate_ocr_quality(text: str) -> float:
    """Estimate OCR quality from common error patterns."""
    if len(text) < 1000:
        return 0.0
    
    # Sample the text (faster for large files)
    sample = text[:50000] if len(text) > 50000 else text
    sample_lower = sample.lower()
    
    errors = 0
    checks = 0
    
    # Check "the" - most common English word
    the_correct = sample_lower.count(' the ')
    the_errors = sum(sample_lower.count(x) for x in 
                     [' tlie ', ' tbe ', ' tiie ', ' ihe ', ' tne ', ' thc '])
    if the_correct > 10:
        error_rate = the_errors / (the_correct + the_errors)
        errors += error_rate * 100
        checks += 100
    
    # Check "and"
    and_correct = sample_lower.count(' and ')
    and_errors = sum(sample_lower.count(x) for x in 
                     [' arid ', ' aud ', ' nnd ', ' aiid '])
    if and_correct > 10:
        error_rate = and_errors / (and_correct + and_errors)
        errors += error_rate * 50
        checks += 50
    
    # Character quality - printable ratio
    printable = sum(c.isprintable() or c in '\n\r\t' for c in sample)
    char_quality = printable / len(sample)
    
    # Word length distribution (OCR errors create weird short "words")
    words = sample.split()
    if words:
        single_char_ratio = sum(1 for w in words if len(w) == 1) / len(words)
        # Normal text has ~5-10% single char words (I, a, etc.)
        word_quality = 1.0 if single_char_ratio < 0.15 else max(0, 1 - (single_char_ratio - 0.15) * 5)
    else:
        word_quality = 0
    
    # Combine scores
    if checks > 0:
        ocr_score = 1 - (errors / checks)
    else:
        ocr_score = 0.8  # No checks possible, assume medium quality
    
    return (ocr_score * 0.5 + char_quality * 0.3 + word_quality * 0.2)


def build_ia_query(
    year_end: int = 1914,
    year_start: int = 1500,
    language: str = "eng",
    collection: Optional[str] = None,
) -> str:
    """Build Internet Archive search query."""
    parts = [
        f"date:[{year_start} TO {year_end}]",
        "mediatype:texts",
        f"language:{language}",
    ]
    
    if collection:
        parts.append(f"collection:{collection}")
    
    return ' AND '.join(parts)


def main():
    parser = argparse.ArgumentParser(description='Download texts from Internet Archive')
    parser.add_argument('-o', '--output', default='./corpus/ia',
                        help='Output directory')
    parser.add_argument('--gutenberg-metadata', 
                        help='Path to Gutenberg metadata CSV for deduplication')
    parser.add_argument('--year-end', type=int, default=1914,
                        help='Latest publication year (default: 1914)')
    parser.add_argument('--year-start', type=int, default=1500,
                        help='Earliest publication year (default: 1500)')
    parser.add_argument('--language', default='eng',
                        help='Language code (default: eng)')
    parser.add_argument('--collection', 
                        help='Specific IA collection to search')
    parser.add_argument('--min-quality', type=float, default=0.80,
                        help='Minimum OCR quality score (default: 0.80)')
    parser.add_argument('--min-length', type=int, default=10000,
                        help='Minimum text length in characters')
    parser.add_argument('--max-items', type=int, default=1000,
                        help='Maximum items to download')
    parser.add_argument('--delay', type=float, default=1.0,
                        help='Delay between downloads (seconds)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be downloaded without downloading')
    
    args = parser.parse_args()
    
    # Load existing corpus for deduplication
    existing = ExistingCorpus()
    if args.gutenberg_metadata:
        existing.add_from_gutenberg_metadata(Path(args.gutenberg_metadata))
    
    # Build search query
    query = build_ia_query(
        year_end=args.year_end,
        year_start=args.year_start,
        language=args.language,
        collection=args.collection,
    )
    print(f"Search query: {query}")
    
    # Search IA
    fields = ['identifier', 'title', 'creator', 'date', 'language', 'downloads']
    
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    metadata_records = []
    downloaded = 0
    skipped_dupe = 0
    skipped_quality = 0
    skipped_length = 0
    skipped_notext = 0
    page = 1
    
    print(f"\nSearching Internet Archive...")
    
    while downloaded < args.max_items:
        result = search_ia(query, fields, rows=100, page=page)
        docs = result.get('response', {}).get('docs', [])
        
        if not docs:
            print(f"No more results at page {page}")
            break
        
        print(f"\nPage {page}: {len(docs)} items")
        
        for doc in docs:
            if downloaded >= args.max_items:
                break
            
            identifier = doc.get('identifier', '')
            title = doc.get('title', 'Unknown')
            creator = doc.get('creator', '')
            if isinstance(creator, list):
                creator = '; '.join(creator)
            date = doc.get('date', '')
            
            # Pre-download duplicate check
            if existing.is_duplicate(title, creator):
                skipped_dupe += 1
                if skipped_dupe <= 10:  # Show first 10
                    print(f"  SKIP (dupe): {title[:50]}...")
                elif skipped_dupe == 11:
                    print(f"  ... (suppressing further dupe messages)")
                continue
            
            if args.dry_run:
                print(f"  WOULD DOWNLOAD: {title[:60]}... by {creator[:30]}")
                downloaded += 1
                continue
            
            # Get text URL
            text_url = get_text_url(identifier)
            if not text_url:
                skipped_notext += 1
                continue
            
            # Download
            print(f"  Downloading: {title[:50]}...")
            try:
                time.sleep(args.delay)
                content = download_text(text_url)
            except Exception as e:
                print(f"    Error: {e}")
                continue
            
            # Length check
            if len(content) < args.min_length:
                skipped_length += 1
                print(f"    Too short: {len(content)} chars")
                continue
            
            # OCR quality check
            quality = estimate_ocr_quality(content)
            if quality < args.min_quality:
                skipped_quality += 1
                print(f"    Low quality: {quality:.2f}")
                continue
            
            # Save
            safe_id = re.sub(r'[^\w\-]', '_', identifier)
            filepath = output_dir / f"{safe_id}.txt"
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"# {title}\n")
                f.write(f"# Author: {creator}\n")
                f.write(f"# Date: {date}\n")
                f.write(f"# Identifier: {identifier}\n")
                f.write(f"# Source: Internet Archive\n")
                f.write(f"# OCR Quality: {quality:.2f}\n")
                f.write(f"# URL: https://archive.org/details/{identifier}\n")
                f.write("\n" + "="*60 + "\n\n")
                f.write(content)
            
            word_count = len(content.split())
            print(f"    Saved: {word_count:,} words, quality={quality:.2f}")
            
            metadata_records.append({
                'identifier': identifier,
                'title': title,
                'creator': creator,
                'date': date,
                'filepath': str(filepath.name),
                'file_size': filepath.stat().st_size,
                'word_count': word_count,
                'ocr_quality': round(quality, 3),
            })
            
            downloaded += 1
        
        page += 1
        
        # Safety limit on pages
        if page > 100:
            print("Reached page limit (100)")
            break
    
    # Save metadata
    if not args.dry_run:
        metadata_file = output_dir / 'metadata.json'
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump({
                'source': 'Internet Archive',
                'query': query,
                'year_range': [args.year_start, args.year_end],
                'language': args.language,
                'min_quality': args.min_quality,
                'texts': metadata_records,
            }, f, indent=2, ensure_ascii=False)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"  Downloaded: {downloaded}")
    print(f"  Skipped (duplicate of Gutenberg): {skipped_dupe}")
    print(f"  Skipped (low OCR quality): {skipped_quality}")
    print(f"  Skipped (too short): {skipped_length}")
    print(f"  Skipped (no text file): {skipped_notext}")
    
    if metadata_records:
        total_words = sum(r['word_count'] for r in metadata_records)
        avg_quality = sum(r['ocr_quality'] for r in metadata_records) / len(metadata_records)
        print(f"  Total words: {total_words:,}")
        print(f"  Average OCR quality: {avg_quality:.2f}")


if __name__ == '__main__':
    main()
