#!/usr/bin/env python3
"""
Unified Corpus Metadata Schema

All collectors output to this common format for cross-source compatibility.
"""

from dataclasses import dataclass, asdict, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional
import csv
import json


class SourceType(Enum):
    GUTENBERG = "gutenberg"
    INTERNET_ARCHIVE = "internet_archive"
    HATHITRUST = "hathitrust"
    PERSEUS = "perseus"
    GOOGLE_BOOKS = "google_books"
    CUSTOM = "custom"


@dataclass
class TextMetadata:
    """Universal metadata for a corpus text."""
    
    # Identity
    corpus_id: str              # Unique within our corpus: "{source}_{original_id}"
    source: str                 # gutenberg, internet_archive, perseus, etc.
    original_id: str            # ID in original source
    
    # Bibliographic
    title: str
    authors: list = field(default_factory=list)  # List of author names
    
    # Temporal (for filtering)
    publication_year: Optional[int] = None       # Year of this edition
    author_death_year: Optional[int] = None      # Latest author death (for temporal purity)
    author_birth_year: Optional[int] = None      # Earliest author birth
    
    # Language
    language: str = "en"                         # ISO 639-1 code
    original_language: Optional[str] = None      # If translation, original language
    
    # Classification
    subjects: list = field(default_factory=list)  # Subject headings
    genre: Optional[str] = None                   # fiction, nonfiction, poetry, etc.
    
    # File info
    filepath: Optional[str] = None               # Relative path in corpus
    file_size: int = 0                           # Bytes
    word_count: int = 0                          # Approximate
    
    # Quality indicators
    ocr_quality: Optional[float] = None          # 0.0-1.0 if known
    is_proofread: bool = False                   # Human-verified text
    
    # Provenance
    download_url: Optional[str] = None
    download_date: Optional[str] = None
    
    # Deduplication
    content_hash: Optional[str] = None           # MD5 of cleaned content
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        d = asdict(self)
        d['authors'] = ';'.join(self.authors) if self.authors else ''
        d['subjects'] = ';'.join(self.subjects) if self.subjects else ''
        return d
    
    @classmethod
    def from_dict(cls, d: dict) -> 'TextMetadata':
        """Create from dictionary."""
        if isinstance(d.get('authors'), str):
            d['authors'] = [a.strip() for a in d['authors'].split(';') if a.strip()]
        if isinstance(d.get('subjects'), str):
            d['subjects'] = [s.strip() for s in d['subjects'].split(';') if s.strip()]
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


class CorpusMetadataWriter:
    """Write metadata in multiple formats."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.records: list = []
    
    def add(self, meta: TextMetadata):
        self.records.append(meta)
    
    def write_csv(self, filename: str = "metadata.csv"):
        """Write as CSV (primary format for compatibility)."""
        if not self.records:
            return
        
        filepath = self.output_dir / filename
        fieldnames = list(self.records[0].to_dict().keys())
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for rec in self.records:
                writer.writerow(rec.to_dict())
        
        return filepath
    
    def write_jsonl(self, filename: str = "metadata.jsonl"):
        """Write as JSON Lines (for streaming processing)."""
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for rec in self.records:
                f.write(json.dumps(rec.to_dict(), ensure_ascii=False) + '\n')
        
        return filepath
    
    def write_all(self):
        """Write all formats."""
        self.write_csv()
        self.write_jsonl()
        
        # Summary stats
        summary = {
            'total_texts': len(self.records),
            'by_source': {},
            'by_language': {},
            'temporal_range': {
                'earliest_death': min((r.author_death_year for r in self.records 
                                       if r.author_death_year), default=None),
                'latest_death': max((r.author_death_year for r in self.records 
                                    if r.author_death_year), default=None),
            },
            'total_size_bytes': sum(r.file_size for r in self.records),
            'generated': datetime.now().isoformat(),
        }
        
        for rec in self.records:
            summary['by_source'][rec.source] = summary['by_source'].get(rec.source, 0) + 1
            summary['by_language'][rec.language] = summary['by_language'].get(rec.language, 0) + 1
        
        with open(self.output_dir / 'corpus_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)


# Converter functions for each source format

def from_gutenberg_row(row: dict) -> TextMetadata:
    """Convert Gutenberg metadata row to unified format."""
    return TextMetadata(
        corpus_id=f"gutenberg_{row.get('id', '')}",
        source="gutenberg",
        original_id=str(row.get('id', '')),
        title=row.get('title', ''),
        authors=[a.strip() for a in row.get('authors', '').split(';') if a.strip()],
        author_death_year=int(row['death_year']) if row.get('death_year') else None,
        author_birth_year=int(row['birth_year']) if row.get('birth_year') else None,
        language=row.get('language', 'en'),
        subjects=[s.strip() for s in row.get('subjects', '').split(';') if s.strip()],
        is_proofread=True,  # Gutenberg texts are proofread
        ocr_quality=0.99,   # Very high quality
    )


def from_ia_row(row: dict) -> TextMetadata:
    """Convert Internet Archive metadata row to unified format."""
    return TextMetadata(
        corpus_id=f"ia_{row.get('identifier', '')}",
        source="internet_archive",
        original_id=row.get('identifier', ''),
        title=row.get('title', ''),
        authors=[row.get('creator', '')] if row.get('creator') else [],
        publication_year=_parse_year(row.get('date', '')),
        language='en',  # IA metadata often lacks this
        is_proofread=False,
        ocr_quality=0.85,  # Conservative estimate for pre-1900
    )


def from_perseus_row(row: dict) -> TextMetadata:
    """Convert Perseus metadata to unified format."""
    return TextMetadata(
        corpus_id=f"perseus_{row.get('urn', '')}",
        source="perseus",
        original_id=row.get('urn', ''),
        title=row.get('title', ''),
        authors=[row.get('author', '')] if row.get('author') else [],
        language=row.get('language', 'grc'),  # grc=Greek, lat=Latin
        original_language=row.get('language'),
        genre=row.get('genre', ''),
        is_proofread=True,  # Perseus is scholarly edited
        ocr_quality=0.99,
    )


def _parse_year(date_str: str) -> Optional[int]:
    """Extract year from various date formats."""
    import re
    if not date_str:
        return None
    match = re.search(r'\b(1[0-9]{3}|20[0-2][0-9])\b', str(date_str))
    return int(match.group(1)) if match else None


if __name__ == "__main__":
    # Example usage
    meta = TextMetadata(
        corpus_id="gutenberg_11",
        source="gutenberg",
        original_id="11",
        title="Alice's Adventures in Wonderland",
        authors=["Carroll, Lewis"],
        author_death_year=1898,
        language="en",
        subjects=["Fantasy fiction", "Children's stories"],
        file_size=155000,
    )
    print(json.dumps(meta.to_dict(), indent=2))
