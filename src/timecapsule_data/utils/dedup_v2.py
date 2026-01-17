#!/usr/bin/env python3
"""
Deduplication Module v2 - Exact + Fuzzy (MinHash)

Provides both exact hash matching and fuzzy MinHash LSH deduplication
for detecting near-duplicate documents (e.g., same text with different OCR errors).
"""

import argparse
import csv
import hashlib
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from datasketch import MinHash, MinHashLSH


@dataclass
class Document:
    """Represents a document for deduplication."""
    path: Path
    source: str
    md5: str = ""
    minhash: Optional[MinHash] = None
    size: int = 0
    word_count: int = 0


@dataclass
class DuplicateGroup:
    """A group of duplicate documents."""
    method: str  # "exact" or "fuzzy"
    similarity: float
    documents: list[Document] = field(default_factory=list)


def compute_md5(text: str) -> str:
    """Compute MD5 hash of text."""
    return hashlib.md5(text.encode('utf-8')).hexdigest()


def compute_minhash(text: str, num_perm: int = 128) -> MinHash:
    """
    Compute MinHash signature for text.
    
    Uses 5-gram shingles (standard for document similarity).
    """
    m = MinHash(num_perm=num_perm)
    
    # Normalize text: lowercase, collapse whitespace
    text = text.lower()
    text = re.sub(r'\s+', ' ', text)
    
    # Generate 5-gram shingles
    words = text.split()
    for i in range(len(words) - 4):
        shingle = ' '.join(words[i:i+5])
        m.update(shingle.encode('utf-8'))
    
    return m


def load_documents(
    corpus_path: Path,
    source_name: str,
    compute_fuzzy: bool = True,
    num_perm: int = 128,
) -> list[Document]:
    """Load documents from a corpus directory."""
    docs = []
    
    # Find all text files
    if corpus_path.is_file():
        files = [corpus_path]
    else:
        files = list(corpus_path.rglob('*.txt'))
    
    print(f"Loading {len(files)} files from {source_name}...")
    
    for i, path in enumerate(files, 1):
        try:
            text = path.read_text(encoding='utf-8', errors='replace')
            
            doc = Document(
                path=path,
                source=source_name,
                md5=compute_md5(text),
                size=len(text),
                word_count=len(text.split()),
            )
            
            if compute_fuzzy and doc.word_count >= 50:
                doc.minhash = compute_minhash(text, num_perm)
            
            docs.append(doc)
            
            if i % 100 == 0:
                print(f"  Loaded {i}/{len(files)} files...")
                
        except Exception as e:
            print(f"  Warning: Could not load {path}: {e}")
    
    return docs


def find_exact_duplicates(documents: list[Document]) -> list[DuplicateGroup]:
    """Find exact duplicates by MD5 hash."""
    by_hash: dict[str, list[Document]] = {}
    
    for doc in documents:
        if doc.md5 not in by_hash:
            by_hash[doc.md5] = []
        by_hash[doc.md5].append(doc)
    
    groups = []
    for md5, docs in by_hash.items():
        if len(docs) > 1:
            groups.append(DuplicateGroup(
                method="exact",
                similarity=1.0,
                documents=docs,
            ))
    
    return groups


def find_fuzzy_duplicates(
    documents: list[Document],
    threshold: float = 0.8,
    num_perm: int = 128,
) -> list[DuplicateGroup]:
    """
    Find near-duplicates using MinHash LSH.
    
    Args:
        documents: List of documents with computed minhashes
        threshold: Jaccard similarity threshold (0.8 = 80% similar)
        num_perm: Number of permutations (must match minhash computation)
    """
    # Filter to documents with minhashes
    docs_with_hash = [d for d in documents if d.minhash is not None]
    
    if not docs_with_hash:
        return []
    
    print(f"Building MinHash LSH index for {len(docs_with_hash)} documents...")
    
    # Create LSH index
    lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
    
    for i, doc in enumerate(docs_with_hash):
        lsh.insert(str(doc.path), doc.minhash)
    
    # Find duplicates
    seen_pairs = set()
    groups_dict: dict[str, list[Document]] = {}
    
    print("Querying for near-duplicates...")
    
    for doc in docs_with_hash:
        results = lsh.query(doc.minhash)
        
        if len(results) > 1:  # More than just self
            # Create a sorted tuple key for this group
            key = tuple(sorted(results))
            
            if key not in groups_dict:
                # Get actual documents
                path_to_doc = {str(d.path): d for d in docs_with_hash}
                group_docs = [path_to_doc[r] for r in results if r in path_to_doc]
                groups_dict[key] = group_docs
    
    # Convert to DuplicateGroups, excluding exact duplicates
    exact_hashes = set()
    for doc in documents:
        exact_hashes.add(doc.md5)
    
    groups = []
    for key, docs in groups_dict.items():
        # Check if this is actually a fuzzy match (not all exact same hash)
        unique_hashes = set(d.md5 for d in docs)
        if len(unique_hashes) > 1:  # Different hashes = fuzzy match
            # Compute actual similarity between first two docs
            if len(docs) >= 2 and docs[0].minhash and docs[1].minhash:
                similarity = docs[0].minhash.jaccard(docs[1].minhash)
            else:
                similarity = threshold
            
            groups.append(DuplicateGroup(
                method="fuzzy",
                similarity=similarity,
                documents=docs,
            ))
    
    return groups


def analyze_duplicates(
    corpus_paths: list[Path],
    threshold: float = 0.8,
    num_perm: int = 128,
) -> dict:
    """
    Analyze multiple corpora for duplicates.
    
    Returns analysis results including exact and fuzzy duplicate groups.
    """
    # Load all documents
    all_docs = []
    for i, path in enumerate(corpus_paths):
        source_name = path.name
        docs = load_documents(path, source_name, compute_fuzzy=True, num_perm=num_perm)
        all_docs.extend(docs)
    
    print(f"\nTotal files across all corpora: {len(all_docs)}")
    
    # Find exact duplicates
    print("\nFinding exact duplicates (MD5)...")
    exact_groups = find_exact_duplicates(all_docs)
    print(f"Found {len(exact_groups)} exact duplicate groups")
    
    # Find fuzzy duplicates
    print(f"\nFinding fuzzy duplicates (MinHash, threshold={threshold})...")
    fuzzy_groups = find_fuzzy_duplicates(all_docs, threshold, num_perm)
    print(f"Found {len(fuzzy_groups)} fuzzy duplicate groups")
    
    # Compute statistics
    total_size = sum(d.size for d in all_docs)
    exact_dup_size = sum(
        sum(d.size for d in g.documents[1:])  # Exclude first (kept) doc
        for g in exact_groups
    )
    fuzzy_dup_size = sum(
        sum(d.size for d in g.documents[1:])
        for g in fuzzy_groups
    )
    
    return {
        "total_files": len(all_docs),
        "total_size_bytes": total_size,
        "by_source": {
            source: len([d for d in all_docs if d.source == source])
            for source in set(d.source for d in all_docs)
        },
        "exact_duplicate_groups": len(exact_groups),
        "exact_duplicate_files": sum(len(g.documents) - 1 for g in exact_groups),
        "exact_duplicate_bytes": exact_dup_size,
        "fuzzy_duplicate_groups": len(fuzzy_groups),
        "fuzzy_duplicate_files": sum(len(g.documents) - 1 for g in fuzzy_groups),
        "fuzzy_duplicate_bytes": fuzzy_dup_size,
        "exact_groups": exact_groups,
        "fuzzy_groups": fuzzy_groups,
    }


def merge_corpora(
    corpus_paths: list[Path],
    output_dir: Path,
    prefer_source: Optional[str] = None,
    threshold: float = 0.8,
    method: str = "both",  # "exact", "fuzzy", or "both"
) -> dict:
    """
    Merge multiple corpora, removing duplicates.
    
    Args:
        corpus_paths: List of corpus directories
        output_dir: Where to write merged corpus
        prefer_source: When duplicates found, prefer files from this source
        threshold: Similarity threshold for fuzzy matching
        method: Deduplication method - "exact", "fuzzy", or "both"
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load all documents
    all_docs = []
    for path in corpus_paths:
        source_name = path.name
        compute_fuzzy = method in ("fuzzy", "both")
        docs = load_documents(path, source_name, compute_fuzzy=compute_fuzzy)
        all_docs.extend(docs)
    
    print(f"\nTotal input files: {len(all_docs)}")
    
    # Find duplicates to remove
    to_remove = set()
    
    if method in ("exact", "both"):
        print("Computing exact duplicates...")
        exact_groups = find_exact_duplicates(all_docs)
        for group in exact_groups:
            # Sort by preference
            sorted_docs = sorted(
                group.documents,
                key=lambda d: (0 if d.source == prefer_source else 1, d.path)
            )
            # Keep first, remove rest
            for doc in sorted_docs[1:]:
                to_remove.add(doc.path)
    
    if method in ("fuzzy", "both"):
        print(f"Computing fuzzy duplicates (threshold={threshold})...")
        fuzzy_groups = find_fuzzy_duplicates(all_docs, threshold)
        for group in fuzzy_groups:
            sorted_docs = sorted(
                group.documents,
                key=lambda d: (0 if d.source == prefer_source else 1, d.path)
            )
            for doc in sorted_docs[1:]:
                to_remove.add(doc.path)
    
    # Copy non-duplicate files
    kept = 0
    for doc in all_docs:
        if doc.path not in to_remove:
            # Copy to output
            dest = output_dir / doc.path.name
            # Handle name collisions
            if dest.exists():
                stem = doc.path.stem
                suffix = doc.path.suffix
                counter = 1
                while dest.exists():
                    dest = output_dir / f"{stem}_{counter}{suffix}"
                    counter += 1
            
            dest.write_text(doc.path.read_text(encoding='utf-8', errors='replace'))
            kept += 1
    
    removed = len(to_remove)
    print(f"\nAfter deduplication: {kept} files")
    print(f"Duplicates removed: {removed}")
    print(f"Merged corpus written to {output_dir}")
    
    return {
        "input_files": len(all_docs),
        "output_files": kept,
        "duplicates_removed": removed,
        "method": method,
        "threshold": threshold if method != "exact" else None,
    }


def main():
    parser = argparse.ArgumentParser(
        description='Deduplicate text corpora (exact + fuzzy matching)'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze corpora for duplicates')
    analyze_parser.add_argument('corpora', nargs='+', type=Path,
                                help='Corpus directories to analyze')
    analyze_parser.add_argument('--threshold', type=float, default=0.8,
                                help='Similarity threshold for fuzzy matching (default: 0.8)')
    analyze_parser.add_argument('--method', choices=['exact', 'fuzzy', 'both'], default='both',
                                help='Deduplication method (default: both)')
    analyze_parser.add_argument('--output', '-o', type=Path,
                                help='Save analysis to JSON file')
    
    # Merge command
    merge_parser = subparsers.add_parser('merge', help='Merge corpora removing duplicates')
    merge_parser.add_argument('corpora', nargs='+', type=Path,
                              help='Corpus directories to merge')
    merge_parser.add_argument('-o', '--output', type=Path, required=True,
                              help='Output directory for merged corpus')
    merge_parser.add_argument('--prefer', type=str,
                              help='Prefer files from this source when deduplicating')
    merge_parser.add_argument('--threshold', type=float, default=0.8,
                              help='Similarity threshold for fuzzy matching (default: 0.8)')
    merge_parser.add_argument('--method', choices=['exact', 'fuzzy', 'both'], default='both',
                              help='Deduplication method (default: both)')
    
    args = parser.parse_args()
    
    if args.command == 'analyze':
        results = analyze_duplicates(
            args.corpora,
            threshold=args.threshold,
        )
        
        # Print summary
        print("\n" + "=" * 60)
        print("DEDUPLICATION ANALYSIS (v2)")
        print("=" * 60)
        print(f"Total files: {results['total_files']}")
        print(f"Total size: {results['total_size_bytes'] / 1e9:.2f} GB")
        print(f"\nBy source:")
        for source, count in results['by_source'].items():
            print(f"  {source}: {count}")
        print(f"\nExact duplicate groups: {results['exact_duplicate_groups']}")
        print(f"Exact duplicate files: {results['exact_duplicate_files']}")
        print(f"Exact duplicate bytes: {results['exact_duplicate_bytes']:,}")
        print(f"\nFuzzy duplicate groups: {results['fuzzy_duplicate_groups']}")
        print(f"Fuzzy duplicate files: {results['fuzzy_duplicate_files']}")
        print(f"Fuzzy duplicate bytes: {results['fuzzy_duplicate_bytes']:,}")
        
        # Show sample fuzzy duplicates
        if results['fuzzy_groups']:
            print(f"\nSample fuzzy duplicates (similarity >= {args.threshold}):")
            for group in results['fuzzy_groups'][:3]:
                print(f"\n  Similarity: {group.similarity:.2%}")
                for doc in group.documents[:3]:
                    print(f"    - [{doc.source}] {doc.path.name}")
        
        if args.output:
            # Save to JSON (without the actual group objects)
            output_data = {k: v for k, v in results.items() 
                          if k not in ('exact_groups', 'fuzzy_groups')}
            with open(args.output, 'w') as f:
                json.dump(output_data, f, indent=2)
            print(f"\nAnalysis saved to {args.output}")
    
    elif args.command == 'merge':
        merge_corpora(
            args.corpora,
            args.output,
            prefer_source=args.prefer,
            threshold=args.threshold,
            method=args.method,
        )
    
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
