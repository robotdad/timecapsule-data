#!/usr/bin/env python3
"""
Corpus Deduplication Module

Identifies and handles duplicate texts across multiple sources
(Gutenberg, Internet Archive, HathiTrust, etc.)

Strategies:
1. Metadata matching (title + author fuzzy match)
2. Content fingerprinting (MinHash for near-duplicate detection)
3. Exact hash (MD5 for identical files)

Usage:
    # Analyze duplicates across corpora
    python dedup_corpus.py analyze ./gutenberg_corpus ./ia_corpus ./output_report.json
    
    # Merge corpora with deduplication  
    python dedup_corpus.py merge ./gutenberg_corpus ./ia_corpus -o ./merged_corpus
    
    # Prefer Gutenberg over IA (cleaner text)
    python dedup_corpus.py merge ./gutenberg ./ia -o ./merged --prefer gutenberg
"""

import argparse
import csv
import hashlib
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional
import logging

# Optional: datasketch for MinHash (pip install datasketch)
try:
    from datasketch import MinHash, MinHashLSH
    HAS_MINHASH = True
except ImportError:
    HAS_MINHASH = False


@dataclass
class TextRecord:
    source: str  # gutenberg, ia, hathitrust, etc.
    filepath: Path
    file_id: str
    title: str
    author: str
    size: int
    md5: str = ""
    minhash: Optional[object] = None


def setup_logger(verbose: bool = False) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger("dedup")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def normalize_title(title: str) -> str:
    """Normalize title for comparison."""
    # Lowercase, remove punctuation, collapse whitespace
    title = title.lower()
    title = re.sub(r'[^\w\s]', '', title)
    title = re.sub(r'\s+', ' ', title).strip()
    # Remove common prefixes/suffixes
    title = re.sub(r'^(the|a|an)\s+', '', title)
    title = re.sub(r'\s+(a novel|a romance|a tale)$', '', title)
    return title


def normalize_author(author: str) -> str:
    """Normalize author name for comparison."""
    author = author.lower()
    # Handle "Last, First" vs "First Last"
    if ',' in author:
        parts = author.split(',')
        if len(parts) >= 2:
            author = f"{parts[1].strip()} {parts[0].strip()}"
    # Remove dates, punctuation
    author = re.sub(r'\d{4}', '', author)
    author = re.sub(r'[^\w\s]', '', author)
    author = re.sub(r'\s+', ' ', author).strip()
    return author


def compute_md5(filepath: Path) -> str:
    """Compute MD5 hash of file content."""
    hasher = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


def compute_minhash(filepath: Path, num_perm: int = 128) -> Optional[object]:
    """Compute MinHash signature for near-duplicate detection."""
    if not HAS_MINHASH:
        return None
    
    try:
        text = filepath.read_text(encoding='utf-8', errors='ignore')
        # Use 5-grams (shingles)
        shingles = set()
        words = text.split()
        for i in range(len(words) - 4):
            shingle = ' '.join(words[i:i+5])
            shingles.add(shingle)
        
        if not shingles:
            return None
            
        m = MinHash(num_perm=num_perm)
        for s in shingles:
            m.update(s.encode('utf-8'))
        return m
    except Exception:
        return None


def load_corpus(corpus_dir: Path, source_name: str, logger: logging.Logger) -> list:
    """Load all texts from a corpus directory."""
    records = []
    
    # Try to load metadata if available
    metadata = {}
    metadata_file = corpus_dir / "metadata.csv"
    if metadata_file.exists():
        with open(metadata_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                file_id = row.get('id') or row.get('identifier', '')
                metadata[str(file_id)] = {
                    'title': row.get('title', ''),
                    'author': row.get('authors', row.get('creator', '')),
                }
    
    # Find all text files (handle both flat and language-subdirectory structures)
    txt_files = list(corpus_dir.rglob("*.txt"))
    txt_files = [f for f in txt_files if f.name != "metadata.csv"]
    
    logger.info(f"Loading {len(txt_files)} files from {source_name}...")
    
    for filepath in txt_files:
        file_id = filepath.stem
        meta = metadata.get(file_id, {})
        
        records.append(TextRecord(
            source=source_name,
            filepath=filepath,
            file_id=file_id,
            title=meta.get('title', ''),
            author=meta.get('author', ''),
            size=filepath.stat().st_size,
        ))
    
    return records


def find_duplicates_by_metadata(records: list, logger: logging.Logger) -> dict:
    """Find potential duplicates by normalized title+author."""
    
    # Group by normalized title
    title_groups = defaultdict(list)
    for rec in records:
        if rec.title:
            norm_title = normalize_title(rec.title)
            if norm_title:
                title_groups[norm_title].append(rec)
    
    # Find groups with multiple sources
    duplicates = {}
    for norm_title, group in title_groups.items():
        sources = set(r.source for r in group)
        if len(sources) > 1:
            duplicates[norm_title] = {
                'records': group,
                'sources': list(sources),
                'match_type': 'title',
            }
    
    logger.info(f"Found {len(duplicates)} potential duplicate groups by title")
    return duplicates


def find_duplicates_by_hash(records: list, logger: logging.Logger) -> dict:
    """Find exact duplicates by MD5 hash."""
    
    logger.info("Computing MD5 hashes...")
    hash_groups = defaultdict(list)
    
    for i, rec in enumerate(records):
        rec.md5 = compute_md5(rec.filepath)
        hash_groups[rec.md5].append(rec)
        
        if (i + 1) % 500 == 0:
            logger.info(f"  Hashed {i+1}/{len(records)}")
    
    # Find groups with multiple files
    duplicates = {}
    for md5, group in hash_groups.items():
        if len(group) > 1:
            duplicates[md5] = {
                'records': group,
                'sources': list(set(r.source for r in group)),
                'match_type': 'exact_hash',
            }
    
    logger.info(f"Found {len(duplicates)} exact duplicate groups by MD5")
    return duplicates


def analyze_corpora(corpus_dirs: list, logger: logging.Logger) -> dict:
    """Analyze multiple corpora for duplicates."""
    
    all_records = []
    for corpus_dir, source_name in corpus_dirs:
        records = load_corpus(Path(corpus_dir), source_name, logger)
        all_records.extend(records)
    
    logger.info(f"\nTotal files across all corpora: {len(all_records)}")
    
    # Find duplicates
    metadata_dups = find_duplicates_by_metadata(all_records, logger)
    hash_dups = find_duplicates_by_hash(all_records, logger)
    
    # Compute statistics
    stats = {
        'total_files': len(all_records),
        'by_source': defaultdict(int),
        'total_size_bytes': sum(r.size for r in all_records),
        'metadata_duplicate_groups': len(metadata_dups),
        'exact_duplicate_groups': len(hash_dups),
        'metadata_duplicates': [
            {
                'normalized_title': title,
                'sources': info['sources'],
                'files': [{'source': r.source, 'id': r.file_id, 'title': r.title} 
                         for r in info['records']]
            }
            for title, info in list(metadata_dups.items())[:100]  # Limit for report
        ],
        'exact_duplicates': [
            {
                'md5': md5,
                'sources': info['sources'],
                'files': [{'source': r.source, 'id': r.file_id, 'size': r.size} 
                         for r in info['records']]
            }
            for md5, info in list(hash_dups.items())[:100]
        ],
    }
    
    for r in all_records:
        stats['by_source'][r.source] += 1
    
    return stats


def merge_corpora(corpus_dirs: list, output_dir: Path, prefer: str,
                  logger: logging.Logger):
    """Merge multiple corpora with deduplication."""
    
    all_records = []
    for corpus_dir, source_name in corpus_dirs:
        records = load_corpus(Path(corpus_dir), source_name, logger)
        all_records.extend(records)
    
    logger.info(f"Total input files: {len(all_records)}")
    
    # Compute hashes for deduplication
    logger.info("Computing hashes...")
    for i, rec in enumerate(all_records):
        rec.md5 = compute_md5(rec.filepath)
        if (i + 1) % 500 == 0:
            logger.info(f"  {i+1}/{len(all_records)}")
    
    # Group by hash, keeping preferred source
    hash_to_best = {}
    preference_order = [prefer] + [s for _, s in corpus_dirs if s != prefer]
    
    for rec in all_records:
        if rec.md5 not in hash_to_best:
            hash_to_best[rec.md5] = rec
        else:
            existing = hash_to_best[rec.md5]
            # Keep the one from preferred source
            existing_pref = preference_order.index(existing.source) if existing.source in preference_order else 999
            new_pref = preference_order.index(rec.source) if rec.source in preference_order else 999
            if new_pref < existing_pref:
                hash_to_best[rec.md5] = rec
    
    # Copy deduplicated files to output
    output_dir.mkdir(parents=True, exist_ok=True)
    
    kept_records = list(hash_to_best.values())
    logger.info(f"After deduplication: {len(kept_records)} files")
    
    import shutil
    for rec in kept_records:
        dest = output_dir / f"{rec.source}_{rec.file_id}.txt"
        shutil.copy2(rec.filepath, dest)
    
    # Write merged metadata
    metadata_file = output_dir / "metadata.csv"
    with open(metadata_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["source", "original_id", "title", "author", "size", "md5"])
        for rec in kept_records:
            writer.writerow([rec.source, rec.file_id, rec.title, rec.author, rec.size, rec.md5])
    
    logger.info(f"Merged corpus written to {output_dir}")
    logger.info(f"Duplicates removed: {len(all_records) - len(kept_records)}")


def main():
    parser = argparse.ArgumentParser(description="Corpus deduplication tools")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Analyze command
    analyze_p = subparsers.add_parser("analyze", help="Analyze corpora for duplicates")
    analyze_p.add_argument("corpora", nargs="+", help="Corpus directories (format: path:name)")
    analyze_p.add_argument("-o", "--output", type=Path, help="Output report JSON")
    analyze_p.add_argument("-v", "--verbose", action="store_true")
    
    # Merge command
    merge_p = subparsers.add_parser("merge", help="Merge corpora with deduplication")
    merge_p.add_argument("corpora", nargs="+", help="Corpus directories (format: path:name)")
    merge_p.add_argument("-o", "--output", type=Path, required=True)
    merge_p.add_argument("--prefer", default="gutenberg", help="Preferred source for duplicates")
    merge_p.add_argument("-v", "--verbose", action="store_true")
    
    args = parser.parse_args()
    logger = setup_logger(args.verbose)
    
    # Parse corpus arguments
    corpus_dirs = []
    for spec in args.corpora:
        if ':' in spec:
            path, name = spec.rsplit(':', 1)
        else:
            path = spec
            name = Path(spec).name
        corpus_dirs.append((path, name))
    
    if args.command == "analyze":
        stats = analyze_corpora(corpus_dirs, logger)
        
        print(f"\n{'='*60}")
        print("DEDUPLICATION ANALYSIS")
        print(f"{'='*60}")
        print(f"Total files: {stats['total_files']:,}")
        print(f"Total size: {stats['total_size_bytes']/1024**3:.2f} GB")
        print(f"\nBy source:")
        for source, count in stats['by_source'].items():
            print(f"  {source}: {count:,}")
        print(f"\nMetadata duplicate groups: {stats['metadata_duplicate_groups']}")
        print(f"Exact duplicate groups: {stats['exact_duplicate_groups']}")
        
        if args.output:
            # Convert defaultdict for JSON serialization
            stats['by_source'] = dict(stats['by_source'])
            with open(args.output, 'w') as f:
                json.dump(stats, f, indent=2, default=str)
            print(f"\nDetailed report: {args.output}")
    
    elif args.command == "merge":
        merge_corpora(corpus_dirs, args.output, args.prefer, logger)


if __name__ == "__main__":
    main()
