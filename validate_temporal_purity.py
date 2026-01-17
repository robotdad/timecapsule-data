#!/usr/bin/env python3
"""
Temporal Purity Validator

Scans collected texts for anachronistic content that would leak
modern knowledge into the training data.

This is a CRITICAL quality check - any contamination defeats the
entire purpose of temporal training.

Usage:
    python validate_temporal_purity.py ./corpus --cutoff-year 1900
"""

import argparse
import re
import sys
from pathlib import Path
from collections import defaultdict

# =============================================================================
# Anachronism Detection Patterns
# =============================================================================

# Terms that should NOT appear in pre-cutoff texts
# Organized by approximate introduction date

ANACHRONISMS = {
    # Post-1900 technology
    "post_1900_tech": [
        r"\b(computer|internet|website|email|software|hardware)\b",
        r"\b(television|TV|radio broadcast|airplane|aircraft)\b",
        r"\b(nuclear|atomic bomb|reactor)\b",
        r"\b(smartphone|cellphone|mobile phone)\b",
        r"\b(satellite|space station|astronaut)\b",
    ],
    
    # Post-1900 cultural references
    "post_1900_culture": [
        r"\bWorld War (I|II|One|Two)\b",
        r"\b(Nazi|Hitler|Stalin|Mussolini)\b",
        r"\b(Holocaust|genocide)\b",
        r"\bCold War\b",
        r"\b(Vietnam|Korean) War\b",
    ],
    
    # Modern publishing/digitization markers
    "modern_publishing": [
        r"Project Gutenberg",
        r"gutenberg\.org",
        r"Internet Archive",
        r"Distributed Proofreaders",
        r"Digitized by",
        r"OCR",
        r"e-?book",
        r"e-?text",
        r"ISBN",
        r"Creative Commons",
        r"public domain",  # The phrase, not the concept
        r"copyright \d{4}",
        r"\d{4}-\d{4} (by|copyright)",
    ],
    
    # Modern linguistic markers (words coined after 1900)
    "modern_vocabulary": [
        r"\brobot\b",  # Coined 1920
        r"\bsmog\b",   # Coined 1905
        r"\bradar\b",  # Coined 1940s
        r"\blaser\b",  # Coined 1960
        r"\bcomputer\b",  # Modern sense post-1940
        r"\bantibiotics?\b",  # Post-1940s usage
        r"\bpenicillin\b",  # Discovered 1928
    ],
    
    # References to modern dates
    "modern_dates": [
        r"\b(19|20)\d{2}\b",  # Years 1900-2099
        r"twenty-first century",
        r"twentieth century",
    ],
}

# Patterns that are ACCEPTABLE even in historical texts
# (to avoid false positives)
ACCEPTABLE_PATTERNS = [
    r"nineteen hundred",  # Literary way to say 1900
    r"the year \d{4}",    # Historical references are fine
]


def check_file(filepath: Path, cutoff_year: int) -> dict:
    """
    Check a single file for anachronisms.
    
    Returns dict with detected issues.
    """
    try:
        text = filepath.read_text(encoding='utf-8', errors='ignore')
    except Exception as e:
        return {"error": str(e)}
    
    issues = defaultdict(list)
    lines = text.split('\n')
    
    for line_num, line in enumerate(lines, 1):
        # Skip if matches acceptable pattern
        if any(re.search(p, line, re.IGNORECASE) for p in ACCEPTABLE_PATTERNS):
            continue
            
        for category, patterns in ANACHRONISMS.items():
            for pattern in patterns:
                matches = re.finditer(pattern, line, re.IGNORECASE)
                for match in matches:
                    # Special handling for dates
                    if category == "modern_dates":
                        # Extract year and check if truly anachronistic
                        year_match = re.search(r'\b(19|20)(\d{2})\b', match.group())
                        if year_match:
                            year = int(year_match.group(1) + year_match.group(2))
                            if year <= cutoff_year:
                                continue  # Not anachronistic
                    
                    issues[category].append({
                        "line": line_num,
                        "match": match.group(),
                        "context": line.strip()[:100],
                    })
    
    return dict(issues)


def validate_corpus(corpus_dir: Path, cutoff_year: int, verbose: bool = False):
    """Validate entire corpus for temporal purity."""
    
    files = list(corpus_dir.glob("*.txt"))
    print(f"Validating {len(files)} files for cutoff year {cutoff_year}...")
    
    contaminated_files = []
    clean_files = 0
    total_issues = defaultdict(int)
    
    for filepath in files:
        if filepath.name == "metadata.csv":
            continue
            
        issues = check_file(filepath, cutoff_year)
        
        if issues:
            if "error" in issues:
                print(f"  ERROR: {filepath.name}: {issues['error']}")
                continue
                
            contaminated_files.append((filepath.name, issues))
            for category, items in issues.items():
                total_issues[category] += len(items)
                
            if verbose:
                print(f"  CONTAMINATED: {filepath.name}")
                for category, items in issues.items():
                    for item in items[:3]:  # Show first 3
                        print(f"    [{category}] L{item['line']}: {item['match']}")
        else:
            clean_files += 1
    
    # Summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Clean files: {clean_files}")
    print(f"Contaminated files: {len(contaminated_files)}")
    print(f"\nIssues by category:")
    for category, count in sorted(total_issues.items(), key=lambda x: -x[1]):
        print(f"  {category}: {count}")
    
    if contaminated_files:
        print(f"\nWARNING: {len(contaminated_files)} files need review/cleaning!")
        print("\nMost contaminated files:")
        sorted_files = sorted(contaminated_files, 
                             key=lambda x: sum(len(v) for v in x[1].values()),
                             reverse=True)
        for name, issues in sorted_files[:10]:
            issue_count = sum(len(v) for v in issues.values())
            print(f"  {name}: {issue_count} issues")
        
        return False
    else:
        print("\nAll files pass temporal purity check!")
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Validate corpus for temporal purity")
    parser.add_argument("corpus_dir", type=Path, help="Directory with text files")
    parser.add_argument("--cutoff-year", "-y", type=int, default=1900)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    
    if not args.corpus_dir.exists():
        print(f"Error: {args.corpus_dir} does not exist")
        sys.exit(1)
    
    success = validate_corpus(args.corpus_dir, args.cutoff_year, args.verbose)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
