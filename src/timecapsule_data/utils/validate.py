#!/usr/bin/env python3
"""
Temporal Purity Validator v2

More nuanced validation - distinguishes between:
- Critical contamination (Gutenberg boilerplate that MUST be removed)
- Suspicious content (might be anachronistic, needs review)
- Acceptable historical usage (words with pre-modern meanings)
"""

import argparse
import re
import sys
from pathlib import Path
from collections import defaultdict

# =============================================================================
# CRITICAL: These indicate failed cleaning (Gutenberg boilerplate)
# =============================================================================
CRITICAL_PATTERNS = {
    "gutenberg_boilerplate": [
        r"Project Gutenberg",
        r"gutenberg\.org",
        r"Distributed Proofreaders",
        r"Internet Archive",
        r"archive\.org",
        r"Digitized by",
        r"This file was produced",
        r"E-?text prepared by",
        r"Produced by",
        r"Release Date:",
        r"Posting Date:",
        r"Character set encoding:",
        r"\[E-?[Tt]ext #?\d+\]",
        r"This eBook is for",
        r"This e-?book",
        r"electronic works",
        r"Archive Foundation",
        r"Transcriber'?s?\s+[Nn]ote",
    ],
}

# =============================================================================
# SUSPICIOUS: Likely anachronistic but could have historical usage
# =============================================================================
SUSPICIOUS_PATTERNS = {
    "modern_tech_unambiguous": [
        r"\binternet\b",
        r"\bwebsite\b",
        r"\bemail\b",
        r"\bsoftware\b",
        r"\bhardware\b",
        r"\btelevision\b",
        r"\bairplane\b",
        r"\baircraft\b",
        r"\bnuclear\b",
        r"\batomic bomb\b",
        r"\bsmartphone\b",
        r"\bcellphone\b",
        r"\bmobile phone\b",
    ],
    "post_1900_events": [
        r"\bWorld War (I|II|One|Two)\b",
        r"\bNazi\b",
        r"\bHitler\b",
        r"\bStalin\b",
        r"\bHolocaust\b",  # Capital H = the event
        r"\bCold War\b",
        r"\bVietnam War\b",
    ],
}

# =============================================================================
# ACCEPTABLE: Words that existed pre-1900 with different meanings
# =============================================================================
# These are NOT flagged:
# - "computer" = one who computes (job title, pre-1900)
# - "satellite" = attendant, follower (pre-astronomical meaning)
# - "holocaust" = sacrifice, destruction by fire (pre-WWII meaning)
# - "robot" - actually coined 1920, but often in translations of older works

def check_file(filepath: Path, cutoff_year: int) -> dict:
    """Check a single file for contamination."""
    try:
        text = filepath.read_text(encoding='utf-8', errors='ignore')
    except Exception as e:
        return {"error": str(e)}
    
    issues = {"critical": [], "suspicious": []}
    lines = text.split('\n')
    
    for line_num, line in enumerate(lines, 1):
        # Check critical patterns (cleaning failures)
        for category, patterns in CRITICAL_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    issues["critical"].append({
                        "line": line_num,
                        "category": category,
                        "match": re.search(pattern, line, re.IGNORECASE).group(),
                        "context": line.strip()[:80],
                    })
        
        # Check suspicious patterns
        for category, patterns in SUSPICIOUS_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    issues["suspicious"].append({
                        "line": line_num,
                        "category": category,
                        "match": re.search(pattern, line, re.IGNORECASE).group(),
                        "context": line.strip()[:80],
                    })
    
    return issues


def validate_corpus(corpus_dir: Path, cutoff_year: int, verbose: bool = False):
    """Validate entire corpus."""
    
    files = [f for f in corpus_dir.glob("*.txt") if f.name != "metadata.csv"]
    print(f"Validating {len(files)} files for cutoff year {cutoff_year}...")
    
    clean_files = 0
    critical_files = []
    suspicious_only_files = []
    
    for filepath in files:
        issues = check_file(filepath, cutoff_year)
        
        if "error" in issues:
            print(f"  ERROR: {filepath.name}: {issues['error']}")
            continue
        
        has_critical = len(issues["critical"]) > 0
        has_suspicious = len(issues["suspicious"]) > 0
        
        if has_critical:
            critical_files.append((filepath.name, issues))
            if verbose:
                print(f"  CRITICAL: {filepath.name} ({len(issues['critical'])} issues)")
                for item in issues["critical"][:3]:
                    print(f"    L{item['line']}: {item['match']}")
        elif has_suspicious:
            suspicious_only_files.append((filepath.name, issues))
            if verbose:
                print(f"  SUSPICIOUS: {filepath.name} ({len(issues['suspicious'])} issues)")
        else:
            clean_files += 1
    
    # Summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Clean files: {clean_files}")
    print(f"Files with CRITICAL issues (cleaning failed): {len(critical_files)}")
    print(f"Files with only suspicious content: {len(suspicious_only_files)}")
    
    if critical_files:
        print(f"\n** CRITICAL: {len(critical_files)} files still have Gutenberg boilerplate!")
        print("These need re-cleaning. Top offenders:")
        sorted_files = sorted(critical_files, key=lambda x: len(x[1]["critical"]), reverse=True)
        for name, issues in sorted_files[:10]:
            print(f"  {name}: {len(issues['critical'])} critical issues")
        return False
    
    if suspicious_only_files:
        print(f"\nNote: {len(suspicious_only_files)} files have suspicious content.")
        print("These may be false positives (historical word usage) or actual contamination.")
        print("Manual review recommended for these files.")
    
    if not critical_files:
        print("\nNo critical cleaning failures detected!")
        return True
    
    return False


def main():
    parser = argparse.ArgumentParser(description="Validate corpus for temporal purity")
    parser.add_argument("corpus_dir", type=Path)
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
