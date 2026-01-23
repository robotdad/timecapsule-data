#!/usr/bin/env python3
"""
Analyze vocab candidates to identify patterns and prioritize review.

Usage:
    python scripts/analyze_vocab.py /mnt/e/datasets/timecapsule-prewwi/cleaned/_vocab_candidates.txt
    python scripts/analyze_vocab.py /mnt/e/datasets/timecapsule-prewwi/cleaned/_vocab_candidates.txt --top 100
    python scripts/analyze_vocab.py /mnt/e/datasets/timecapsule-prewwi/cleaned/_vocab_candidates.txt --pattern "ſ"
    python scripts/analyze_vocab.py /mnt/e/datasets/timecapsule-prewwi/cleaned/_vocab_candidates.txt --foreign
    python scripts/analyze_vocab.py /mnt/e/datasets/timecapsule-prewwi/cleaned/_vocab_candidates.txt --flags "?"
"""

import argparse
import re
import sys
from collections import Counter
from pathlib import Path


def parse_vocab_line(line: str) -> dict | None:
    """Parse a vocab candidate line.

    Supports two formats:
    1. Simple: 'count word'
    2. Rich: 'count | FLAGS | word | context'
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    # Try rich format first: FREQ | FLAGS | WORD | CONTEXT
    if " | " in line:
        parts = line.split(" | ", 3)
        if len(parts) >= 3:
            try:
                count = int(parts[0].strip())
                flags = parts[1].strip()
                word = parts[2].strip()
                context = parts[3].strip() if len(parts) > 3 else ""
                return {
                    "word": word,
                    "count": count,
                    "flags": flags,
                    "context": context,
                }
            except ValueError:
                pass

    # Fall back to simple format: count word
    parts = line.split(None, 1)
    if len(parts) != 2:
        return None

    try:
        count = int(parts[0])
        word = parts[1]
        return {"word": word, "count": count, "flags": "", "context": ""}
    except ValueError:
        return None


def load_vocab(path: Path, limit: int | None = None) -> list[dict]:
    """Load vocab candidates from file."""
    results = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            parsed = parse_vocab_line(line)
            if parsed:
                results.append(parsed)
                if limit and len(results) >= limit:
                    break
    return results


def analyze_patterns(vocab: list[dict]) -> dict:
    """Analyze common patterns in vocab candidates."""
    patterns = {
        "long_s": re.compile(r"[ſ]"),  # Contains long-s
        "ligatures": re.compile(r"[ﬁﬂﬀﬃﬄ]"),  # Unicode ligatures
        "german_chars": re.compile(r"[äöüßÄÖÜ]"),  # German characters
        "accented": re.compile(r"[àáâãäåæçèéêëìíîïñòóôõöøùúûüý]", re.I),
        "all_caps": re.compile(r"^[A-Z]{3,}$"),  # All caps words
        "mixed_case": re.compile(r"[a-z][A-Z]|[A-Z][a-z][A-Z]"),  # Weird casing
        "numbers_mixed": re.compile(r"[a-zA-Z]\d|\d[a-zA-Z]"),  # Letters + numbers
        "repeated_chars": re.compile(r"(.)\1{2,}"),  # 3+ repeated chars
        "ocr_artifacts": re.compile(r"[|!l1]{3,}|[_]{2,}"),  # Common OCR noise
        "short": re.compile(r"^.{1,2}$"),  # Very short words
        "very_long": re.compile(r"^.{25,}$"),  # Very long words
    }

    results = {name: [] for name in patterns}
    results["clean"] = []  # Words matching no patterns

    for item in vocab:
        word = item["word"]
        matched = False
        for name, pattern in patterns.items():
            if pattern.search(word):
                results[name].append(item)
                matched = True
        if not matched:
            results["clean"].append(item)

    return results


def analyze_by_flags(vocab: list[dict]) -> dict:
    """Analyze vocab by flag types."""
    flag_groups = {
        "suspicious": [],  # Contains ?
        "capitalized": [],  # Contains C
        "unknown": [],  # Contains U
        "no_flags": [],  # No flags
    }

    for item in vocab:
        flags = item.get("flags", "")
        if "?" in flags:
            flag_groups["suspicious"].append(item)
        if "C" in flags:
            flag_groups["capitalized"].append(item)
        if "U" in flags:
            flag_groups["unknown"].append(item)
        if not flags:
            flag_groups["no_flags"].append(item)

    return flag_groups


def detect_foreign_words(vocab: list[dict]) -> dict:
    """Detect likely foreign words by pattern."""
    foreign_patterns = {
        "german": [
            re.compile(r"lich$|ung$|heit$|keit$|schaft$|chen$"),  # German suffixes
            re.compile(r"^(ge|be|ver|zer|ent|er)[a-z]+"),  # German prefixes
            re.compile(r"[äöüß]"),  # German chars
        ],
        "latin": [
            re.compile(r"(us|um|ae|orum|arum|ibus|is)$"),  # Latin endings
            re.compile(r"^(ex|ab|ad|per|pro|sub|super)[a-z]+"),  # Latin prefixes
        ],
        "french": [
            re.compile(r"(eux|aux|eau|tion|ment|oire)$"),  # French endings
            re.compile(r"[éèêëàâùûîïôœæç]"),  # French chars
        ],
    }

    results = {lang: [] for lang in foreign_patterns}

    for item in vocab:
        word = item["word"]
        for lang, patterns in foreign_patterns.items():
            if any(p.search(word.lower()) for p in patterns):
                results[lang].append(item)
                break  # Only categorize once

    return results


def print_summary(vocab: list[dict]) -> None:
    """Print overall summary."""
    print("=" * 70)
    print("VOCAB CANDIDATES SUMMARY")
    print("=" * 70)

    total_words = len(vocab)
    total_occurrences = sum(item["count"] for item in vocab)

    print(f"\nUnique candidates: {total_words:,}")
    print(f"Total occurrences: {total_occurrences:,}")

    if vocab:
        counts = [item["count"] for item in vocab]
        print("\nOccurrence distribution:")
        print(f"  Min:  {min(counts):,}")
        print(f"  Max:  {max(counts):,}")
        print(f"  Mean: {sum(counts) / len(counts):,.1f}")

    # Flag distribution
    flag_counts = Counter()
    for item in vocab:
        flags = item.get("flags", "")
        if "?" in flags:
            flag_counts["suspicious (?)"] += 1
        if "C" in flags:
            flag_counts["capitalized (C)"] += 1
        if "U" in flags:
            flag_counts["unknown (U)"] += 1

    if flag_counts:
        print("\nFlag distribution:")
        for flag, count in flag_counts.most_common():
            print(f"  {flag}: {count:,}")


def print_top_words(vocab: list[dict], n: int = 50) -> None:
    """Print top N words by occurrence."""
    print("\n" + "=" * 70)
    print(f"TOP {n} CANDIDATES BY OCCURRENCE")
    print("=" * 70)

    # Sort by count descending
    sorted_vocab = sorted(vocab, key=lambda x: -x["count"])

    for i, item in enumerate(sorted_vocab[:n], 1):
        word = item["word"]
        count = item["count"]
        flags = item.get("flags", "")
        # Escape/display special characters
        display_word = repr(word)[1:-1] if any(ord(c) > 127 for c in word) else word
        print(f"  {i:4}. {count:>10,}  [{flags:4}]  {display_word}")


def print_pattern_analysis(patterns: dict) -> None:
    """Print pattern analysis results."""
    print("\n" + "=" * 70)
    print("PATTERN ANALYSIS")
    print("=" * 70)

    for name, items in sorted(patterns.items(), key=lambda x: -len(x[1])):
        total_occ = sum(item["count"] for item in items)
        print(f"\n{name}: {len(items):,} words ({total_occ:,} occurrences)")

        if items and name != "clean":
            # Show top 5 examples
            top = sorted(items, key=lambda x: -x["count"])[:5]
            for item in top:
                word = item["word"]
                display = repr(word)[1:-1] if any(ord(c) > 127 for c in word) else word
                print(f"    {item['count']:>8,}  {display}")


def print_foreign_analysis(foreign: dict) -> None:
    """Print foreign word analysis."""
    print("\n" + "=" * 70)
    print("POTENTIAL FOREIGN WORDS")
    print("=" * 70)

    for lang, items in sorted(foreign.items(), key=lambda x: -len(x[1])):
        total_occ = sum(item["count"] for item in items)
        print(f"\n{lang.upper()}: {len(items):,} words ({total_occ:,} occurrences)")

        if items:
            # Show top 10 examples
            top = sorted(items, key=lambda x: -x["count"])[:10]
            for item in top:
                print(f"    {item['count']:>8,}  {item['word']}")


def search_pattern(vocab: list[dict], pattern: str) -> list[dict]:
    """Search for words matching a pattern."""
    regex = re.compile(pattern, re.IGNORECASE)
    return [item for item in vocab if regex.search(item["word"])]


def filter_by_flags(vocab: list[dict], flag_filter: str) -> list[dict]:
    """Filter vocab by flag characters."""
    return [item for item in vocab if flag_filter in item.get("flags", "")]


def export_words(items: list[dict], output_path: Path) -> None:
    """Export words to file (just the words, one per line)."""
    with open(output_path, "w", encoding="utf-8") as f:
        for item in sorted(items, key=lambda x: -x["count"]):
            f.write(item["word"] + "\n")


def main():
    parser = argparse.ArgumentParser(description="Analyze vocab candidates")
    parser.add_argument("vocab_path", type=Path, help="Path to _vocab_candidates.txt")
    parser.add_argument("--top", type=int, default=50, help="Show top N words")
    parser.add_argument("--pattern", type=str, help="Search for words matching regex pattern")
    parser.add_argument("--patterns", action="store_true", help="Show pattern analysis")
    parser.add_argument("--foreign", action="store_true", help="Show foreign word analysis")
    parser.add_argument("--flags", type=str, help="Filter by flag (e.g., '?' for suspicious)")
    parser.add_argument("--export", type=str, help="Export matching words to file")
    parser.add_argument("--limit", type=int, help="Limit words to load (for testing)")
    parser.add_argument("--min-count", type=int, default=5, help="Minimum occurrence count")

    args = parser.parse_args()

    if not args.vocab_path.exists():
        print(f"Error: File not found: {args.vocab_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading: {args.vocab_path}")
    vocab = load_vocab(args.vocab_path, args.limit)

    # Filter by min count
    vocab = [item for item in vocab if item["count"] >= args.min_count]
    print(f"Loaded {len(vocab):,} candidates (>= {args.min_count} occurrences)")

    # Flag filter mode
    if args.flags:
        vocab = filter_by_flags(vocab, args.flags)
        print(f"Filtered to {len(vocab):,} with flag '{args.flags}'")

    # Pattern search mode
    if args.pattern:
        matches = search_pattern(vocab, args.pattern)
        print(f"\nMatches for pattern '{args.pattern}': {len(matches):,}")

        if args.export:
            export_words(matches, Path(args.export))
            print(f"Exported to: {args.export}")
        else:
            for item in sorted(matches, key=lambda x: -x["count"])[:100]:
                word = item["word"]
                display = repr(word)[1:-1] if any(ord(c) > 127 for c in word) else word
                print(f"  {item['count']:>10,}  [{item.get('flags', ''):4}]  {display}")
        return

    print_summary(vocab)
    print_top_words(vocab, args.top)

    if args.patterns:
        patterns = analyze_patterns(vocab)
        print_pattern_analysis(patterns)

    if args.foreign:
        foreign = detect_foreign_words(vocab)
        print_foreign_analysis(foreign)


if __name__ == "__main__":
    main()
