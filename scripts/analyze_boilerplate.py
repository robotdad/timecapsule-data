#!/usr/bin/env python3
"""
Analyze boilerplate patterns in OCR'd historical documents.

This script identifies and categorizes boilerplate text from various digitization sources:
- Google Books disclaimers
- Internet Archive headers
- University library stamps
- JSTOR notices
- Project Gutenberg headers/footers

Usage:
    python scripts/analyze_boilerplate.py /path/to/corpus [--sample N] [--verbose]
"""

import argparse
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class BoilerplateMatch:
    """A detected boilerplate region in a document."""

    pattern_name: str
    category: str
    start_line: int
    end_line: int
    start_char: int
    end_char: int
    matched_text: str
    location: str  # 'start', 'end', 'middle'


@dataclass
class DocumentAnalysis:
    """Analysis results for a single document."""

    path: str
    total_chars: int
    total_lines: int
    matches: list[BoilerplateMatch] = field(default_factory=list)
    boilerplate_chars: int = 0
    boilerplate_ratio: float = 0.0


# Boilerplate detection patterns
# Each pattern: (name, category, regex, flags, location_hint)
# location_hint: 'start', 'end', 'any' - where we expect to find this pattern

BOILERPLATE_PATTERNS = [
    # Google Books - long disclaimer block at start
    (
        "google_books_disclaimer",
        "google_books",
        r"This\s+is\s+a\s+digital\s+copy\s+of\s+a\s+book\s+that\s+was\s+preserved.*?"
        r"(?:at\s*\|?\s*http\s*:\s*//books\s*\.\s*(?:google|qooqle)\s*\.\s*com\s*/?\|?|"
        r"You\s+can\s+search\s+through\s+the\s+full\s+text.*?web)",
        re.IGNORECASE | re.DOTALL,
        "start",
    ),
    (
        "google_books_short",
        "google_books",
        r"'s\s+mission\s+is\s+to\s+organize\s+the\s+world's\s+information.*?Book\s+Search",
        re.IGNORECASE | re.DOTALL,
        "start",
    ),
    (
        "google_watermark_url",
        "google_books",
        r"(?:http\s*:\s*//?\s*)?books\s*\.\s*(?:google|qooqle)\s*\.\s*com\s*/?\s*",
        re.IGNORECASE,
        "any",
    ),
    # Internet Archive headers
    (
        "ia_digitized_header",
        "internet_archive",
        r"Digitized\s+by\s+(?:the\s+)?(?:Internet\s+)?Archive\s*[\n\r]+.*?(?:in\s+\d{4})?\s*[\n\r]+\s*https?://archive\.org/details/\S+",
        re.IGNORECASE | re.DOTALL,
        "start",
    ),
    (
        "ia_url",
        "internet_archive",
        r"https?://(?:www\.)?archive\.org/details/\S+",
        re.IGNORECASE,
        "start",
    ),
    (
        "ia_digitized_simple",
        "internet_archive",
        r"Digitized\s+by\s+(?:the\s+)?(?:Internet\s+)?Archive",
        re.IGNORECASE,
        "start",
    ),
    # JSTOR (often OCR'd as STOR)
    (
        "jstor_early_content",
        "jstor",
        r"(?:J?STOR|Early\s+Journal\s+Content)\s+.*?(?:public\s+domain|freely\s+available)",
        re.IGNORECASE | re.DOTALL,
        "start",
    ),
    # Project Gutenberg
    (
        "gutenberg_start",
        "gutenberg",
        r"\*{3}\s*START\s+OF\s+(?:THE\s+|THIS\s+)?PROJECT\s+GUTENBERG.*?\*{3}",
        re.IGNORECASE | re.DOTALL,
        "start",
    ),
    (
        "gutenberg_end",
        "gutenberg",
        r"\*{3}\s*END\s+OF\s+(?:THE\s+|THIS\s+)?PROJECT\s+GUTENBERG.*",
        re.IGNORECASE | re.DOTALL,
        "end",
    ),
    (
        "gutenberg_license",
        "gutenberg",
        r"Project\s+Gutenberg(?:-tm)?\s+(?:License|eBook|E-?text).*?"
        r"(?:TRADEMARK|distribute\s+copies|electronic\s+works)",
        re.IGNORECASE | re.DOTALL,
        "end",
    ),
    # University library stamps (usually at end, from scanned pages)
    (
        "university_library_stamp",
        "library_stamp",
        r"(?:THE\s+)?UNIVERSITY\s+OF\s+\w+\s*[\n\r]+\s*(?:GRADUATE\s+)?LIBRARY",
        re.IGNORECASE,
        "end",
    ),
    (
        "library_date_due",
        "library_stamp",
        r"DATE\s+DUE\s*[\n\r]+(?:\s*\w+\s*\d*\s*[\n\r]+)*",
        re.IGNORECASE,
        "end",
    ),
    (
        "library_circulate_card",
        "library_stamp",
        r"(?:CIRCULATE|IITILATE)\s+CAR[DK]",
        re.IGNORECASE,
        "end",
    ),
    (
        "library_barcode",
        "library_stamp",
        r"\d\s+\d{4}\s+\d{3}\s+\d+\s+\d+",  # University barcode pattern like "3 9015 030 7 4133"
        re.IGNORECASE,
        "end",
    ),
    # Yale/Harvard/etc specific patterns
    (
        "yale_library",
        "library_stamp",
        r"YALE\s+(?:MEDICAL\s+)?LIBRARY.*?(?:HISTORICAL\s+LIBRARY|Bequest\s+of)",
        re.IGNORECASE | re.DOTALL,
        "start",
    ),
    # HathiTrust
    (
        "hathitrust",
        "hathitrust",
        r"(?:Generated|Digitized)\s+(?:by|for)\s+HathiTrust.*?(?:www\.hathitrust\.org|public\s+domain)",
        re.IGNORECASE | re.DOTALL,
        "start",
    ),
    # Generic digitization notices
    (
        "generic_digitized",
        "generic",
        r"(?:This\s+book\s+was\s+)?[Dd]igitized\s+(?:by|from|at)\s+.*?(?:Library|Archive|University)",
        re.IGNORECASE,
        "start",
    ),
]


def find_line_number(text: str, char_pos: int) -> int:
    """Find the line number for a character position."""
    return text[:char_pos].count("\n") + 1


def classify_location(text: str, start: int, end: int) -> str:
    """Classify where in the document the match occurs."""
    total_len = len(text)
    if total_len == 0:
        return "unknown"

    start_pct = start / total_len
    end_pct = end / total_len

    if end_pct <= 0.15:  # First 15% of document
        return "start"
    elif start_pct >= 0.85:  # Last 15% of document
        return "end"
    else:
        return "middle"


def analyze_document(file_path: Path, verbose: bool = False) -> DocumentAnalysis:
    """Analyze a single document for boilerplate patterns."""
    try:
        text = file_path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        if verbose:
            print(f"Error reading {file_path}: {e}", file=sys.stderr)
        return DocumentAnalysis(path=str(file_path), total_chars=0, total_lines=0)

    total_chars = len(text)
    total_lines = text.count("\n") + 1

    analysis = DocumentAnalysis(
        path=str(file_path), total_chars=total_chars, total_lines=total_lines
    )

    # Track matched regions to avoid double-counting
    matched_regions: list[tuple[int, int]] = []

    for pattern_name, category, pattern, flags, location_hint in BOILERPLATE_PATTERNS:
        try:
            regex = re.compile(pattern, flags)
        except re.error as e:
            if verbose:
                print(f"Invalid regex for {pattern_name}: {e}", file=sys.stderr)
            continue

        # For location-hinted patterns, only search in relevant portion
        if location_hint == "start":
            search_text = text[: min(len(text), 10000)]  # First 10KB
            offset = 0
        elif location_hint == "end":
            offset = max(0, len(text) - 10000)
            search_text = text[offset:]
        else:
            search_text = text
            offset = 0

        for match in regex.finditer(search_text):
            start = match.start() + offset
            end = match.end() + offset

            # Check if this region overlaps with already matched regions
            overlaps = False
            for existing_start, existing_end in matched_regions:
                if start < existing_end and end > existing_start:
                    overlaps = True
                    break

            if overlaps:
                continue

            matched_regions.append((start, end))

            location = classify_location(text, start, end)
            matched_text = text[start:end]

            # Truncate matched text for display
            if len(matched_text) > 200:
                matched_text = matched_text[:100] + " [...] " + matched_text[-100:]

            analysis.matches.append(
                BoilerplateMatch(
                    pattern_name=pattern_name,
                    category=category,
                    start_line=find_line_number(text, start),
                    end_line=find_line_number(text, end),
                    start_char=start,
                    end_char=end,
                    matched_text=matched_text,
                    location=location,
                )
            )

    # Calculate total boilerplate chars (avoiding double-counting)
    analysis.boilerplate_chars = sum(end - start for start, end in matched_regions)
    if total_chars > 0:
        analysis.boilerplate_ratio = analysis.boilerplate_chars / total_chars

    return analysis


def analyze_corpus(
    corpus_path: Path, sample_size: Optional[int] = None, verbose: bool = False
) -> list[DocumentAnalysis]:
    """Analyze all documents in a corpus directory."""
    files = list(corpus_path.glob("*.txt"))

    if sample_size and sample_size < len(files):
        import random

        files = random.sample(files, sample_size)

    results = []
    for i, file_path in enumerate(files):
        if verbose and (i + 1) % 100 == 0:
            print(f"Processed {i + 1}/{len(files)} files...", file=sys.stderr)

        analysis = analyze_document(file_path, verbose)
        results.append(analysis)

    return results


def print_summary(results: list[DocumentAnalysis]) -> dict:
    """Print summary statistics and return as dict."""
    total_files = len(results)
    files_with_boilerplate = sum(1 for r in results if r.matches)

    # Count by category
    category_counts: Counter[str] = Counter()
    pattern_counts: Counter[str] = Counter()
    location_counts: Counter[str] = Counter()
    category_chars: Counter[str] = Counter()

    for result in results:
        seen_categories = set()
        for match in result.matches:
            pattern_counts[match.pattern_name] += 1
            location_counts[match.location] += 1
            category_chars[match.category] += match.end_char - match.start_char
            if match.category not in seen_categories:
                category_counts[match.category] += 1
                seen_categories.add(match.category)

    # Calculate boilerplate stats
    total_chars = sum(r.total_chars for r in results)
    total_boilerplate = sum(r.boilerplate_chars for r in results)

    print("\n" + "=" * 70)
    print("BOILERPLATE ANALYSIS SUMMARY")
    print("=" * 70)

    print(f"\nFiles analyzed: {total_files}")
    print(
        f"Files with boilerplate: {files_with_boilerplate} ({100 * files_with_boilerplate / total_files:.1f}%)"
    )
    print(f"Total characters: {total_chars:,}")
    print(
        f"Total boilerplate characters: {total_boilerplate:,} ({100 * total_boilerplate / total_chars:.2f}%)"
    )

    print("\n" + "-" * 40)
    print("BY CATEGORY (files containing):")
    print("-" * 40)
    for category, count in category_counts.most_common():
        pct = 100 * count / total_files
        chars = category_chars[category]
        print(f"  {category:25s} {count:5d} files ({pct:5.1f}%)  {chars:10,} chars")

    print("\n" + "-" * 40)
    print("BY PATTERN (occurrences):")
    print("-" * 40)
    for pattern, count in pattern_counts.most_common():
        print(f"  {pattern:35s} {count:5d}")

    print("\n" + "-" * 40)
    print("BY LOCATION:")
    print("-" * 40)
    for location, count in location_counts.most_common():
        print(f"  {location:10s} {count:5d}")

    # Files with highest boilerplate ratio
    print("\n" + "-" * 40)
    print("TOP 10 FILES BY BOILERPLATE RATIO:")
    print("-" * 40)
    sorted_by_ratio = sorted(results, key=lambda r: r.boilerplate_ratio, reverse=True)
    for result in sorted_by_ratio[:10]:
        if result.boilerplate_ratio > 0:
            fname = Path(result.path).name
            print(
                f"  {fname[:40]:40s} {100 * result.boilerplate_ratio:5.1f}% ({result.boilerplate_chars:,} chars)"
            )

    return {
        "total_files": total_files,
        "files_with_boilerplate": files_with_boilerplate,
        "total_chars": total_chars,
        "total_boilerplate_chars": total_boilerplate,
        "boilerplate_ratio": total_boilerplate / total_chars if total_chars else 0,
        "by_category": dict(category_counts),
        "by_pattern": dict(pattern_counts),
        "by_location": dict(location_counts),
    }


def print_examples(results: list[DocumentAnalysis], category: Optional[str] = None, n: int = 3):
    """Print example matches for inspection."""
    print("\n" + "=" * 70)
    print("EXAMPLE MATCHES" + (f" (category: {category})" if category else ""))
    print("=" * 70)

    examples_shown = 0
    for result in results:
        for match in result.matches:
            if category and match.category != category:
                continue

            print(f"\n--- {Path(result.path).name} ---")
            print(f"Pattern: {match.pattern_name}")
            print(f"Category: {match.category}")
            print(f"Location: {match.location} (lines {match.start_line}-{match.end_line})")
            print(f"Text:\n{match.matched_text}")
            print()

            examples_shown += 1
            if examples_shown >= n:
                return


def main():
    parser = argparse.ArgumentParser(description="Analyze boilerplate in OCR documents")
    parser.add_argument("corpus_path", type=Path, help="Path to corpus directory")
    parser.add_argument("--sample", "-n", type=int, help="Sample N files instead of all")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--examples", "-e", type=int, default=0, help="Show N example matches")
    parser.add_argument("--category", "-c", help="Filter examples by category")
    parser.add_argument("--json", "-j", type=Path, help="Output results to JSON file")

    args = parser.parse_args()

    if not args.corpus_path.is_dir():
        print(f"Error: {args.corpus_path} is not a directory", file=sys.stderr)
        sys.exit(1)

    print(f"Analyzing corpus: {args.corpus_path}")
    results = analyze_corpus(args.corpus_path, args.sample, args.verbose)

    summary = print_summary(results)

    if args.examples > 0:
        print_examples(results, args.category, args.examples)

    if args.json:
        # Serialize results
        output = {
            "summary": summary,
            "files": [
                {
                    "path": r.path,
                    "total_chars": r.total_chars,
                    "boilerplate_chars": r.boilerplate_chars,
                    "boilerplate_ratio": r.boilerplate_ratio,
                    "matches": [
                        {
                            "pattern": m.pattern_name,
                            "category": m.category,
                            "location": m.location,
                            "start_line": m.start_line,
                            "end_line": m.end_line,
                        }
                        for m in r.matches
                    ],
                }
                for r in results
            ],
        }
        args.json.write_text(json.dumps(output, indent=2))
        print(f"\nResults written to {args.json}")


if __name__ == "__main__":
    main()
