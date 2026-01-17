#!/usr/bin/env python3
"""
Gutenberg Catalog Analyzer

Analyzes what's available in Project Gutenberg by time period.
Useful for planning temporal dataset collection.

Usage:
    python analyze_catalog.py
    python analyze_catalog.py --language en --save-report report.txt
"""

import argparse
import csv
import io
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

import requests

CATALOG_URL = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv"


def parse_author_years(author_string: str) -> tuple:
    """Extract birth and death years from author string."""
    # Standard: 1832-1898
    match = re.search(r',\s*(\d{4})\s*-\s*(\d{4})\s*$', author_string)
    if match:
        return int(match.group(1)), int(match.group(2))
    
    # BCE: 621? BCE-565? BCE
    match = re.search(r'(\d{1,4})\??\s*BCE\s*-\s*(\d{1,4})\??\s*BCE', author_string)
    if match:
        return -int(match.group(1)), -int(match.group(2))
    
    # Single BCE: -850? BCE
    match = re.search(r'-\s*(\d{1,4})\??\s*BCE', author_string)
    if match:
        return None, -int(match.group(1))
    
    # Anywhere in string
    match = re.search(r'(\d{4})\s*-\s*(\d{4})', author_string)
    if match:
        return int(match.group(1)), int(match.group(2))
    
    return None, None


def analyze_catalog(language: str = "en"):
    """Download and analyze the Gutenberg catalog."""
    
    print(f"Downloading catalog from {CATALOG_URL}...")
    resp = requests.get(CATALOG_URL, timeout=120)
    resp.raise_for_status()
    print(f"Downloaded: {len(resp.content):,} bytes\n")
    
    reader = csv.DictReader(io.StringIO(resp.text))
    
    # Statistics
    by_era = defaultdict(int)
    by_century = defaultdict(int)
    no_date = 0
    total = 0
    authors_by_era = defaultdict(set)
    subjects_by_era = defaultdict(lambda: defaultdict(int))
    
    # Era definitions
    ERAS = [
        (-1000, "Ancient (before 1 CE)"),
        (500, "Classical (1-500 CE)"),
        (1000, "Early Medieval (500-1000)"),
        (1400, "Medieval (1000-1400)"),
        (1600, "Renaissance (1400-1600)"),
        (1700, "Enlightenment (1600-1700)"),
        (1800, "18th Century (1700-1800)"),
        (1875, "Early 19th C (1800-1875)"),
        (1900, "Late 19th C (1875-1900)"),
        (1950, "Early 20th C (1900-1950)"),
        (2000, "Late 20th C (1950-2000)"),
        (9999, "21st Century (2000+)"),
    ]
    
    for row in reader:
        if row.get("Type") != "Text":
            continue
        if language and row.get("Language", "").lower() != language.lower():
            continue
            
        total += 1
        
        authors_str = row.get("Authors", "")
        authors = [a.strip() for a in authors_str.split(";") if a.strip()]
        
        # Get latest death year among authors
        death_years = []
        for author in authors:
            _, death = parse_author_years(author)
            if death is not None:
                death_years.append(death)
        
        if not death_years:
            no_date += 1
            continue
        
        death_year = max(death_years)
        
        # Categorize by era
        for threshold, era_name in ERAS:
            if death_year <= threshold:
                by_era[era_name] += 1
                for author in authors:
                    authors_by_era[era_name].add(author.split(",")[0])
                    
                # Track subjects
                subjects = row.get("Subjects", "").split(";")
                for subj in subjects:
                    subj = subj.strip()
                    if subj:
                        subjects_by_era[era_name][subj] += 1
                break
        
        # Categorize by century
        if death_year < 0:
            century = f"{abs(death_year)//100 + 1} BCE"
        else:
            century = f"{death_year//100 + 1}th CE"
        by_century[century] += 1
    
    # Print report
    print("=" * 70)
    print(f"GUTENBERG CATALOG ANALYSIS - Language: {language.upper()}")
    print("=" * 70)
    print(f"\nTotal texts: {total:,}")
    print(f"With author dates: {total - no_date:,}")
    print(f"Without author dates: {no_date:,}")
    
    print("\n" + "-" * 70)
    print("TEXTS BY ERA (based on author death year)")
    print("-" * 70)
    
    cumulative = 0
    for threshold, era_name in ERAS:
        count = by_era[era_name]
        cumulative += count
        if count > 0:
            pct = count / (total - no_date) * 100
            authors = authors_by_era[era_name]
            sample = list(authors)[:5]
            print(f"\n{era_name}:")
            print(f"  Texts: {count:,} ({pct:.1f}%)")
            print(f"  Cumulative: {cumulative:,}")
            print(f"  Sample authors: {', '.join(sample)}")
            
            # Top subjects
            top_subjects = sorted(subjects_by_era[era_name].items(), 
                                 key=lambda x: -x[1])[:5]
            if top_subjects:
                print(f"  Top subjects: {', '.join(s[0][:30] for s in top_subjects)}")
    
    print("\n" + "-" * 70)
    print("PRACTICAL DATASET SIZES BY CUTOFF YEAR")
    print("-" * 70)
    
    cutoffs = [500, 1000, 1600, 1700, 1800, 1850, 1875, 1900, 1925, 1950]
    running_total = 0
    era_idx = 0
    
    for cutoff in cutoffs:
        # Sum all eras up to this cutoff
        count = sum(by_era[name] for thresh, name in ERAS if thresh <= cutoff)
        print(f"  Cutoff {cutoff}: ~{count:,} texts available")
    
    print("\n" + "-" * 70)
    print("HIGHLIGHTS FOR TIMECAPSULE EXPERIMENTS")
    print("-" * 70)
    
    ancient = sum(by_era[n] for t, n in ERAS if t <= 500)
    medieval = sum(by_era[n] for t, n in ERAS if 500 < t <= 1400)
    early_modern = sum(by_era[n] for t, n in ERAS if 1400 < t <= 1800)
    victorian = sum(by_era[n] for t, n in ERAS if 1800 < t <= 1900)
    
    print(f"""
  Ancient Classics (to 500 CE):     ~{ancient:,} texts
    - Greek philosophers, Roman historians
    - Homer, Plato, Aristotle, Virgil, etc.
    
  Medieval (500-1400):              ~{medieval:,} texts
    - Early church fathers, Dante, Chaucer
    
  Early Modern (1400-1800):         ~{early_modern:,} texts
    - Shakespeare, Milton, Enlightenment
    
  Victorian Era (1800-1900):        ~{victorian:,} texts
    - Dickens, Austen, the Brontes, etc.
    - This is the TimeCapsuleLLM target era
    
RECOMMENDATIONS:
  1. For Greek classics: --cutoff-year 500
  2. For pre-industrial: --cutoff-year 1800
  3. For Victorian (like TimeCapsuleLLM): --cutoff-year 1900
  4. For early 20th century: --cutoff-year 1950
""")
    
    return by_era, by_century


def main():
    parser = argparse.ArgumentParser(description="Analyze Gutenberg catalog")
    parser.add_argument("--language", "-l", default="en")
    parser.add_argument("--save-report", "-o", type=str, help="Save to file")
    args = parser.parse_args()
    
    if args.save_report:
        import contextlib
        with open(args.save_report, 'w') as f:
            with contextlib.redirect_stdout(f):
                analyze_catalog(args.language)
        print(f"Report saved to {args.save_report}")
    else:
        analyze_catalog(args.language)


if __name__ == "__main__":
    main()
