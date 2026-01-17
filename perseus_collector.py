#!/usr/bin/env python3
"""
Perseus Digital Library Collector

Downloads classical Greek and Latin texts from the Perseus CTS API.
These are scholarly editions - the actual Plato, Aristotle, Homer, Cicero, etc.
in original languages.

CTS API endpoints:
- GetCapabilities: List all available texts
- GetPassage: Fetch text content by URN

Quality: EXCELLENT - scholarly edited, not OCR
"""

import argparse
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

# Import our unified schema
try:
    from corpus_schema import TextMetadata, CorpusMetadataWriter
except ImportError:
    # Standalone mode
    TextMetadata = None
    CorpusMetadataWriter = None


CTS_BASE = "https://cts.perseids.org/api/cts"

# Namespace mapping for XML parsing
NS = {
    'cts': 'http://chs.harvard.edu/xmlns/cts',
    'ti': 'http://chs.harvard.edu/xmlns/cts',
    'tei': 'http://www.tei-c.org/ns/1.0',
}


@dataclass
class PerseusText:
    """Represents a text in the Perseus catalog."""
    urn: str
    title: str
    author: str
    language: str  # grc, lat, eng, etc.
    description: str = ""
    is_translation: bool = False
    

def fetch_url(url: str, retries: int = 3, delay: float = 1.0) -> str:
    """Fetch URL with retries and rate limiting."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={'User-Agent': 'PerseusCollector/1.0 (research)'})
            with urlopen(req, timeout=30) as response:
                return response.read().decode('utf-8')
        except (HTTPError, URLError) as e:
            if attempt < retries - 1:
                print(f"  Retry {attempt + 1}/{retries} after error: {e}")
                time.sleep(delay * (attempt + 1))
            else:
                raise
    return ""


def get_capabilities() -> list[PerseusText]:
    """Fetch the complete catalog from Perseus CTS API."""
    print("Fetching Perseus catalog...")
    url = f"{CTS_BASE}?request=GetCapabilities"
    xml_text = fetch_url(url)
    
    # Parse XML
    root = ET.fromstring(xml_text)
    texts = []
    
    # Find all textgroups
    for textgroup in root.findall('.//ti:textgroup', NS):
        group_urn = textgroup.get('urn', '')
        
        # Get author name
        groupname = textgroup.find('ti:groupname', NS)
        author = groupname.text if groupname is not None and groupname.text else "Unknown"
        
        # Find all works in this textgroup
        for work in textgroup.findall('ti:work', NS):
            work_urn = work.get('urn', '')
            work_lang = work.get('{http://www.w3.org/XML/1998/namespace}lang', 'unknown')
            
            # Get title
            title_elem = work.find('ti:title', NS)
            title = title_elem.text if title_elem is not None and title_elem.text else "Untitled"
            
            # Find editions (original language)
            for edition in work.findall('ti:edition', NS):
                edition_urn = edition.get('urn', '')
                
                label = edition.find('ti:label', NS)
                label_text = label.text if label is not None and label.text else title
                
                desc = edition.find('ti:description', NS)
                desc_text = desc.text if desc is not None and desc.text else ""
                
                texts.append(PerseusText(
                    urn=edition_urn,
                    title=label_text,
                    author=author,
                    language=work_lang,
                    description=desc_text,
                    is_translation=False,
                ))
            
            # Find translations
            for translation in work.findall('ti:translation', NS):
                trans_urn = translation.get('urn', '')
                trans_lang = translation.get('{http://www.w3.org/XML/1998/namespace}lang', 'eng')
                
                label = translation.find('ti:label', NS)
                label_text = label.text if label is not None and label.text else title
                
                desc = translation.find('ti:description', NS)
                desc_text = desc.text if desc is not None and desc.text else ""
                
                texts.append(PerseusText(
                    urn=trans_urn,
                    title=f"{label_text} (translation)",
                    author=author,
                    language=trans_lang,
                    description=desc_text,
                    is_translation=True,
                ))
    
    return texts


def get_valid_reff(urn: str) -> list[str]:
    """Get valid reference citations for a text."""
    url = f"{CTS_BASE}?request=GetValidReff&urn={urn}&level=1"
    try:
        xml_text = fetch_url(url)
        root = ET.fromstring(xml_text)
        
        refs = []
        for ref in root.findall('.//cts:urn', NS):
            if ref.text:
                refs.append(ref.text)
        
        # Fallback: try ti namespace
        if not refs:
            for ref in root.findall('.//ti:urn', NS):
                if ref.text:
                    refs.append(ref.text)
        
        return refs
    except Exception as e:
        print(f"  Warning: Could not get references for {urn}: {e}")
        return []


def get_passage(urn: str) -> str:
    """Fetch the text content for a URN."""
    url = f"{CTS_BASE}?request=GetPassage&urn={urn}"
    try:
        xml_text = fetch_url(url)
        root = ET.fromstring(xml_text)
        
        # Extract text content from TEI body
        # The structure varies, so we try multiple approaches
        text_parts = []
        
        # Method 1: Find all text nodes in the passage
        for elem in root.iter():
            if elem.text and elem.text.strip():
                text_parts.append(elem.text.strip())
            if elem.tail and elem.tail.strip():
                text_parts.append(elem.tail.strip())
        
        return ' '.join(text_parts)
    except Exception as e:
        print(f"  Warning: Could not fetch passage {urn}: {e}")
        return ""


def download_text(text: PerseusText, output_dir: Path, delay: float = 0.5) -> Optional[dict]:
    """Download a complete text and return metadata."""
    print(f"  Downloading: {text.author} - {text.title}")
    
    # Get all valid references
    refs = get_valid_reff(text.urn)
    
    if not refs:
        # Try fetching the whole text directly
        content = get_passage(text.urn)
        if not content or len(content) < 100:
            print(f"    Skipping: No content available")
            return None
        refs = [text.urn]
    
    # Fetch all passages
    all_content = []
    for ref in refs:
        time.sleep(delay)  # Rate limiting
        passage = get_passage(ref)
        if passage:
            all_content.append(passage)
    
    if not all_content:
        print(f"    Skipping: No content retrieved")
        return None
    
    # Combine content
    full_text = '\n\n'.join(all_content)
    
    # Clean up the text
    full_text = clean_text(full_text)
    
    if len(full_text) < 100:
        print(f"    Skipping: Content too short ({len(full_text)} chars)")
        return None
    
    # Create safe filename
    safe_author = re.sub(r'[^\w\-]', '_', text.author)[:50]
    safe_title = re.sub(r'[^\w\-]', '_', text.title)[:50]
    safe_urn = re.sub(r'[^\w\-]', '_', text.urn.split(':')[-1])
    filename = f"{safe_author}_{safe_title}_{safe_urn}.txt"
    
    # Save text
    filepath = output_dir / filename
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f"# {text.title}\n")
        f.write(f"# Author: {text.author}\n")
        f.write(f"# URN: {text.urn}\n")
        f.write(f"# Language: {text.language}\n")
        f.write(f"# Source: Perseus Digital Library\n")
        f.write(f"# Description: {text.description}\n")
        f.write("\n" + "="*60 + "\n\n")
        f.write(full_text)
    
    word_count = len(full_text.split())
    print(f"    Saved: {filepath.name} ({word_count:,} words)")
    
    return {
        'urn': text.urn,
        'title': text.title,
        'author': text.author,
        'language': text.language,
        'description': text.description,
        'is_translation': text.is_translation,
        'filepath': str(filepath.relative_to(output_dir)),
        'file_size': filepath.stat().st_size,
        'word_count': word_count,
    }


def clean_text(text: str) -> str:
    """Clean up Perseus text content."""
    # Remove XML artifacts
    text = re.sub(r'<[^>]+>', ' ', text)
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r' +', ' ', text)
    
    # Fix common issues
    text = text.replace('  ', ' ')
    
    # Restore paragraph breaks at sentence ends followed by caps
    text = re.sub(r'([.!?])\s+([A-Z\u0391-\u03A9])', r'\1\n\n\2', text)
    
    return text.strip()


def main():
    parser = argparse.ArgumentParser(description='Download texts from Perseus Digital Library')
    parser.add_argument('-o', '--output', default='./corpus/perseus',
                        help='Output directory')
    parser.add_argument('-l', '--languages', default='grc,lat',
                        help='Languages to download (comma-separated: grc,lat,eng)')
    parser.add_argument('--no-translations', action='store_true',
                        help='Skip translations, download originals only')
    parser.add_argument('--list-only', action='store_true',
                        help='List available texts without downloading')
    parser.add_argument('--delay', type=float, default=0.5,
                        help='Delay between requests (seconds)')
    parser.add_argument('--limit', type=int, default=0,
                        help='Limit number of texts to download (0=unlimited)')
    
    args = parser.parse_args()
    
    # Get catalog
    all_texts = get_capabilities()
    print(f"Found {len(all_texts)} texts in Perseus catalog")
    
    # Filter by language
    languages = set(args.languages.lower().split(','))
    texts = [t for t in all_texts if t.language.lower() in languages]
    print(f"Filtered to {len(texts)} texts in languages: {languages}")
    
    # Filter translations
    if args.no_translations:
        texts = [t for t in texts if not t.is_translation]
        print(f"After removing translations: {len(texts)} texts")
    
    # Language breakdown
    by_lang = {}
    for t in texts:
        by_lang[t.language] = by_lang.get(t.language, 0) + 1
    print(f"By language: {by_lang}")
    
    if args.list_only:
        print("\n=== Available Texts ===")
        for t in texts:
            trans = " [TRANSLATION]" if t.is_translation else ""
            print(f"  [{t.language}] {t.author}: {t.title}{trans}")
            print(f"       URN: {t.urn}")
        return
    
    # Apply limit
    if args.limit > 0:
        texts = texts[:args.limit]
        print(f"Limited to first {args.limit} texts")
    
    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Download texts
    print(f"\nDownloading {len(texts)} texts to {output_dir}...")
    
    metadata_records = []
    success = 0
    failed = 0
    
    for i, text in enumerate(texts, 1):
        print(f"\n[{i}/{len(texts)}] ", end='')
        try:
            result = download_text(text, output_dir, args.delay)
            if result:
                metadata_records.append(result)
                success += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            failed += 1
    
    # Save metadata
    metadata_file = output_dir / 'metadata.json'
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump({
            'source': 'Perseus Digital Library',
            'api': CTS_BASE,
            'languages': list(languages),
            'include_translations': not args.no_translations,
            'texts': metadata_records,
            'total_downloaded': success,
            'total_failed': failed,
        }, f, indent=2, ensure_ascii=False)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"Download complete!")
    print(f"  Successful: {success}")
    print(f"  Failed: {failed}")
    print(f"  Metadata: {metadata_file}")
    
    total_words = sum(r['word_count'] for r in metadata_records)
    total_bytes = sum(r['file_size'] for r in metadata_records)
    print(f"  Total words: {total_words:,}")
    print(f"  Total size: {total_bytes / 1024 / 1024:.1f} MB")


if __name__ == '__main__':
    main()
