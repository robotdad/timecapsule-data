#!/usr/bin/env python3
"""
Gutenberg Temporal Dataset Collector - v3

Downloads and cleans Project Gutenberg texts filtered by author death year.
Supports multiple languages for multilingual corpus building.

Usage:
    # English only (default)
    python gutenberg_collector.py -y 1900 -o ./corpus_en
    
    # Multiple languages
    python gutenberg_collector.py -y 1900 -l en,fr,de,la,el -o ./corpus_multilingual
    
    # All languages
    python gutenberg_collector.py -y 1900 -l all -o ./corpus_all
"""

import argparse
import csv
import io
import re
import sys
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CATALOG_URL = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv"
TEXT_BASE_URL = "https://www.gutenberg.org/ebooks/{id}.txt.utf-8"
MIRROR_BASE_URL = "https://www.gutenberg.org/files/{id}/{id}-0.txt"

DEFAULT_CONCURRENCY = 4
DEFAULT_TIMEOUT = 60
REQUEST_DELAY = 0.5


@dataclass
class BookMetadata:
    id: int
    title: str
    authors: list
    language: str
    subjects: list
    author_death_year: Optional[int] = None
    author_birth_year: Optional[int] = None


def setup_logger(verbose: bool = False) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger("gutenberg_collector")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def create_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1.0, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "TimeCapsuleLLM-Collector/1.0 (+https://github.com/haykgrigo3/TimeCapsuleLLM)"
    })
    return session


def parse_author_years(author_string: str) -> tuple:
    """Extract birth and death years from author string."""
    match = re.search(r',\s*(\d{4})\s*-\s*(\d{4})\s*$', author_string)
    if match:
        return int(match.group(1)), int(match.group(2))
    
    match = re.search(r'(\d{1,4})\??\s*BCE\s*-\s*(\d{1,4})\??\s*BCE', author_string)
    if match:
        return -int(match.group(1)), -int(match.group(2))
    
    match = re.search(r'-\s*(\d{1,4})\??\s*BCE', author_string)
    if match:
        return None, -int(match.group(1))
    
    match = re.search(r'(\d{4})\s*-\s*(\d{4})', author_string)
    if match:
        return int(match.group(1)), int(match.group(2))
    
    return None, None


def get_latest_author_death(authors: list) -> Optional[int]:
    death_years = []
    for author in authors:
        _, death = parse_author_years(author)
        if death is not None:
            death_years.append(death)
    return max(death_years) if death_years else None


def download_catalog(session: requests.Session, logger: logging.Logger) -> str:
    logger.info(f"Downloading catalog from {CATALOG_URL}...")
    resp = session.get(CATALOG_URL, timeout=120)
    resp.raise_for_status()
    logger.info(f"Downloaded catalog: {len(resp.content):,} bytes")
    return resp.text


def parse_catalog(csv_content: str, cutoff_year: int, languages: list, 
                  logger: logging.Logger) -> list:
    """
    Parse catalog CSV and filter by temporal constraints.
    
    Args:
        languages: List of language codes, or ['all'] for all languages
    """
    lang_str = "all" if languages == ['all'] else ",".join(languages)
    logger.info(f"Parsing catalog (cutoff_year={cutoff_year}, languages={lang_str})...")
    
    reader = csv.DictReader(io.StringIO(csv_content))
    books = []
    stats = {"no_year": 0, "after_cutoff": 0, "wrong_lang": 0, "not_text": 0}
    lang_counts = {}
    
    for row in reader:
        if row.get("Type") != "Text":
            stats["not_text"] += 1
            continue
        
        row_lang = row.get("Language", "").lower()
        
        # Language filtering
        if languages != ['all']:
            if row_lang not in [l.lower() for l in languages]:
                stats["wrong_lang"] += 1
                continue
        
        authors_str = row.get("Authors", "")
        authors = [a.strip() for a in authors_str.split(";") if a.strip()]
        
        if not authors:
            stats["no_year"] += 1
            continue
            
        death_year = get_latest_author_death(authors)
        
        if death_year is None:
            stats["no_year"] += 1
            continue
        if death_year > cutoff_year:
            stats["after_cutoff"] += 1
            continue
        
        birth_years = [parse_author_years(a)[0] for a in authors]
        birth_years = [b for b in birth_years if b is not None]
        birth_year = min(birth_years) if birth_years else None
        
        subjects = [s.strip() for s in row.get("Subjects", "").split(";") if s.strip()]
        
        try:
            book_id = int(row.get("Text#", 0))
        except ValueError:
            continue
        
        # Track language distribution
        lang_counts[row_lang] = lang_counts.get(row_lang, 0) + 1
            
        books.append(BookMetadata(
            id=book_id, title=row.get("Title", "Unknown"), authors=authors,
            language=row_lang, subjects=subjects,
            author_birth_year=birth_year, author_death_year=death_year,
        ))
    
    logger.info(f"Catalog parsing complete:")
    logger.info(f"  - Qualifying books: {len(books)}")
    logger.info(f"  - Skipped (no author year): {stats['no_year']}")
    logger.info(f"  - Skipped (after cutoff): {stats['after_cutoff']}")
    logger.info(f"  - Skipped (wrong language): {stats['wrong_lang']}")
    logger.info(f"  - Skipped (not text): {stats['not_text']}")
    
    if len(lang_counts) > 1:
        logger.info(f"  - Language distribution:")
        for lang, count in sorted(lang_counts.items(), key=lambda x: -x[1])[:10]:
            logger.info(f"      {lang}: {count:,}")
    
    return books


def clean_gutenberg_text(raw_text: str) -> str:
    """Remove ALL Gutenberg boilerplate and modern contamination."""
    text = raw_text
    
    # START markers
    start_patterns = [
        r"\*\*\*\s*START OF TH(?:E|IS) PROJECT GUTENBERG EBOOK[^\*]*\*\*\*",
        r"\*\*\*\s*START OF THE PROJECT GUTENBERG[^\*]*\*\*\*",
        r"\*\*\*\s*START OF THIS PROJECT GUTENBERG[^\*]*\*\*\*",
        r"\*{3,}\s*$",
    ]
    
    for pattern in start_patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            text = text[match.end():]
            break
    
    # END markers
    end_patterns = [
        r"\*\*\*\s*END OF TH(?:E|IS) PROJECT GUTENBERG EBOOK[^\*]*\*\*\*",
        r"\*\*\*\s*END OF THE PROJECT GUTENBERG[^\*]*\*\*\*",
        r"End of (?:the )?Project Gutenberg",
        r"End of Project Gutenberg's",
        r"\*\*\* END OF THIS PROJECT GUTENBERG",
    ]
    
    for pattern in end_patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            text = text[:match.start()]
            break
    
    # Remove editorial sections
    text = re.sub(
        r"(?:^|\n)Transcriber'?s?\s+Notes?:?.*?(?=\n\n\n|\n[A-Z]{2,}|\Z)",
        "", text, flags=re.IGNORECASE | re.DOTALL
    )
    text = re.sub(
        r"(?:^|\n)Editor'?s?\s+Notes?:?.*?(?=\n\n\n|\n[A-Z]{2,}|\Z)",
        "", text, flags=re.IGNORECASE | re.DOTALL
    )
    
    # Line-by-line contamination removal
    line_kill_patterns = [
        r"Project Gutenberg", r"Gutenberg Literary Archive", r"gutenberg\.org",
        r"www\.gutenberg", r"Distributed Proofreaders", r"proofreaders\.net",
        r"Internet Archive", r"archive\.org", r"Digitized by",
        r"This file was produced", r"This eBook is for the use of",
        r"E-?text prepared by", r"Produced by", r"Updated editions will replace",
        r"electronic works", r"Archive Foundation", r"Release Date:",
        r"Posting Date:", r"Last [Uu]pdated:", r"Character set encoding:",
        r"Language:\s*English", r"\[E-?[Tt]ext", r"\[Illustration",
        r"^\s*\*\*\*\s*$", r"Transcriber'?s?\s+[Nn]ote", r"Scanner'?s?\s+[Nn]ote",
        r"Editor'?s?\s+[Nn]ote", r"^\s*NOTE:", r"OCR", r"This text has been",
        r"public domain", r"PUBLIC DOMAIN", r"This work is in the",
        r"This ebook", r"This e-book", r"This etext", r"This e-text",
        r"Etext #", r"EBook #",
    ]
    
    lines = text.split('\n')
    clean_lines = []
    
    for line in lines:
        kill_line = False
        for pattern in line_kill_patterns:
            if re.search(pattern, line, re.IGNORECASE):
                kill_line = True
                break
        if not kill_line:
            clean_lines.append(line)
    
    text = '\n'.join(clean_lines)
    
    # Remove editorial date notes
    text = re.sub(r'\[[A-Z]{1,3},\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s*\d{4}:[^\]]*\]', '', text)
    
    # Clean whitespace
    text = re.sub(r'\n{4,}', '\n\n\n', text)
    text = re.sub(r'^\s+', '', text)
    return text.strip()


def download_text(book: BookMetadata, session: requests.Session, 
                  output_dir: Path, logger: logging.Logger) -> tuple:
    # Organize by language
    lang_dir = output_dir / book.language
    lang_dir.mkdir(parents=True, exist_ok=True)
    output_file = lang_dir / f"{book.id}.txt"
    
    if output_file.exists() and output_file.stat().st_size > 100:
        return (book.id, True, "skipped (exists)")
    
    urls = [
        TEXT_BASE_URL.format(id=book.id),
        MIRROR_BASE_URL.format(id=book.id),
        f"https://www.gutenberg.org/files/{book.id}/{book.id}.txt",
    ]
    
    for url in urls:
        try:
            resp = session.get(url, timeout=DEFAULT_TIMEOUT)
            if resp.status_code == 200:
                clean_text = clean_gutenberg_text(resp.text)
                
                if len(clean_text) < 500:
                    continue
                
                output_file.write_text(clean_text, encoding='utf-8')
                time.sleep(REQUEST_DELAY)
                return (book.id, True, f"downloaded ({len(clean_text):,} chars)")
                
        except Exception as e:
            logger.debug(f"Book {book.id}: failed {url}: {e}")
    
    return (book.id, False, "no valid download source")


def collect_corpus(cutoff_year: int, languages: list, output_dir: Path,
                   concurrency: int, limit: Optional[int], logger: logging.Logger):
    output_dir.mkdir(parents=True, exist_ok=True)
    session = create_session()
    
    catalog_csv = download_catalog(session, logger)
    books = parse_catalog(catalog_csv, cutoff_year, languages, logger)
    
    if limit:
        books = books[:limit]
        logger.info(f"Limited to {limit} books for testing")
    
    logger.info(f"Downloading {len(books)} texts with {concurrency} workers...")
    
    results = {"success": 0, "failed": 0, "skipped": 0}
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(download_text, book, session, output_dir, logger): book
            for book in books
        }
        
        for i, future in enumerate(as_completed(futures)):
            book = futures[future]
            try:
                _, success, message = future.result()
                if success:
                    results["skipped" if "skipped" in message else "success"] += 1
                else:
                    results["failed"] += 1
                    
                if (i + 1) % 100 == 0:
                    logger.info(f"Progress: {i+1}/{len(books)}")
                    
            except Exception as e:
                logger.error(f"Book {book.id}: {e}")
                results["failed"] += 1
    
    # Write metadata (one file per language + combined)
    metadata_file = output_dir / "metadata.csv"
    with open(metadata_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["id", "title", "authors", "death_year", "birth_year", 
                        "language", "subjects", "downloaded"])
        for book in books:
            lang_dir = output_dir / book.language
            writer.writerow([
                book.id, book.title, "; ".join(book.authors),
                book.author_death_year, book.author_birth_year,
                book.language, "; ".join(book.subjects),
                (lang_dir / f"{book.id}.txt").exists()
            ])
    
    logger.info(f"\nComplete! Success: {results['success']}, "
                f"Skipped: {results['skipped']}, Failed: {results['failed']}")
    logger.info(f"Metadata: {metadata_file}")
    
    # Report by language
    if languages == ['all'] or len(languages) > 1:
        logger.info("Files by language directory:")
        for lang_dir in sorted(output_dir.iterdir()):
            if lang_dir.is_dir():
                count = len(list(lang_dir.glob("*.txt")))
                if count > 0:
                    logger.info(f"  {lang_dir.name}/: {count} files")


def main():
    parser = argparse.ArgumentParser(
        description="Collect temporally-filtered Project Gutenberg texts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # English only, pre-1900
  python gutenberg_collector.py -y 1900 -o ./corpus_en
  
  # Multiple languages (scholarly corpus)
  python gutenberg_collector.py -y 1900 -l en,fr,de,la,el -o ./corpus_scholarly
  
  # All languages pre-1900
  python gutenberg_collector.py -y 1900 -l all -o ./corpus_all
  
  # Ancient texts only (pre-500 CE)
  python gutenberg_collector.py -y 500 -l all -o ./corpus_ancient
        """)
    parser.add_argument("--cutoff-year", "-y", type=int, default=1900,
                       help="Include authors who died on/before this year")
    parser.add_argument("--language", "-l", default="en",
                       help="Language(s): 'en', 'en,fr,de', or 'all'")
    parser.add_argument("--output-dir", "-o", type=Path, default=Path("./gutenberg_corpus"))
    parser.add_argument("--concurrency", "-c", type=int, default=4)
    parser.add_argument("--limit", type=int, help="Limit books (for testing)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    
    # Parse language argument
    if args.language.lower() == 'all':
        languages = ['all']
    else:
        languages = [l.strip().lower() for l in args.language.split(',')]
    
    logger = setup_logger(args.verbose)
    logger.info("=" * 60)
    logger.info("Gutenberg Temporal Dataset Collector v3")
    logger.info(f"Cutoff: {args.cutoff_year} | Languages: {args.language}")
    logger.info("=" * 60)
    
    collect_corpus(args.cutoff_year, languages, args.output_dir,
                   args.concurrency, args.limit, logger)


if __name__ == "__main__":
    main()
