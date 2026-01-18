#!/usr/bin/env python3
"""
OCR Cleanup Module

Repairs common OCR errors in historical texts. This goes beyond filtering -
it actually attempts to fix recognizable error patterns.

Common OCR errors in 19th century texts:
- 'tbe' -> 'the'
- 'arid' -> 'and'
- 'wbich' -> 'which'
- 'tlie' -> 'the'
- 'li' -> 'h' (very common: tliis->this, wliich->which, liim->him)
- Long s (ſ) misread as 'f'
- Ligatures (fi, fl, ff) broken apart
- 'rn' misread as 'm' and vice versa

Usage:
    # Clean a single file
    tc-ocr-clean clean input.txt -o output.txt

    # Clean entire corpus directory
    tc-ocr-clean batch ./corpus_raw -o ./corpus_clean

    # Analyze without changing (show what would be fixed)
    tc-ocr-clean analyze ./corpus_raw --report fixes.json
"""

import argparse
import json
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# Common OCR substitution errors
# Format: (error_pattern, correction, context_required)
# context_required: None = always apply, or regex that must match around the word
OCR_SUBSTITUTIONS = [
    # ==========================================================================
    # 'li' -> 'h' confusion (VERY COMMON in newspaper OCR)
    # This is one of the most frequent OCR errors - 'h' gets misread as 'li'
    # ==========================================================================
    # 'the' variants (most common English word, most common OCR errors)
    (r"\btbe\b", "the", None),
    (r"\btlie\b", "the", None),
    (r"\btiie\b", "the", None),
    (r"\btbc\b", "the", None),
    (r"\bihe\b", "the", None),
    (r"\btne\b", "the", None),
    (r"\bthc\b", "the", None),
    # Additional 'the' variants from vocab analysis (13k+ occurrences each)
    (r"\bllie\b", "the", None),
    (r"\bllic\b", "the", None),
    (r"\bllio\b", "the", None),
    # 'this' variants (5850+ occurrences of tliis)
    (r"\btbis\b", "this", None),
    (r"\bthia\b", "this", None),
    (r"\btliis\b", "this", None),
    # 'that' variants
    (r"\btbat\b", "that", None),
    (r"\btliat\b", "that", None),
    (r"\btlmt\b", "that", None),
    (r"\bthnt\b", "that", None),
    # 'which' variants (4497+ occurrences of wliich)
    (r"\bwbich\b", "which", None),
    (r"\bwhicb\b", "which", None),
    (r"\bwliich\b", "which", None),
    (r"\bwliicli\b", "which", None),
    # 'what' variants
    (r"\bwliat\b", "what", None),
    (r"\bwlmt\b", "what", None),
    # 'when' variants
    (r"\bwlien\b", "when", None),
    (r"\bwben\b", "when", None),
    # 'where' variants
    (r"\bwliere\b", "where", None),
    (r"\bwbere\b", "where", None),
    # 'while' variants
    (r"\bwliile\b", "while", None),
    (r"\bwbile\b", "while", None),
    # 'who' variants
    (r"\bwlio\b", "who", None),
    # 'whose' variants
    (r"\bwliose\b", "whose", None),
    # 'him' variants (2863 occurrences of liim)
    (r"\bliim\b", "him", None),
    (r"\bhirn\b", "him", None),
    # 'his' variants (9347 occurrences of liis)
    (r"\bliis\b", "his", None),
    (r"\bhia\b", "his", None),
    # 'her' variants
    (r"\blier\b", "her", None),
    # 'he' - needs context since 'lie' is a real word
    (r"\blie\b", "he", r"\b(and|but|that|when|if|as|so|because)\s+lie\b"),
    # 'she' variants
    (r"\bslie\b", "she", None),
    # 'they' variants
    (r"\btliey\b", "they", None),
    (r"\btbey\b", "they", None),
    # 'their' variants
    (r"\btbeir\b", "their", None),
    (r"\btlieir\b", "their", None),
    # 'them' variants
    (r"\btbem\b", "them", None),
    (r"\btliem\b", "them", None),
    # 'then' variants
    (r"\btben\b", "then", None),
    (r"\btlien\b", "then", None),
    # 'there' variants
    (r"\btbere\b", "there", None),
    (r"\btliere\b", "there", None),
    # 'these' variants
    (r"\btbese\b", "these", None),
    (r"\btliese\b", "these", None),
    # 'those' variants
    (r"\btbose\b", "those", None),
    (r"\btliose\b", "those", None),
    # 'other' variants
    (r"\botber\b", "other", None),
    (r"\botlier\b", "other", None),
    # ==========================================================================
    # Other common OCR substitution errors
    # ==========================================================================
    # 'and' variants
    (r"\barid\b", "and", None),
    (r"\baud\b", "and", None),
    (r"\bnnd\b", "and", None),
    (r"\baiid\b", "and", None),
    # 'with' variants
    (r"\bwitb\b", "with", None),
    (r"\bwitli\b", "with", None),
    # 'have' variants
    (r"\bhavo\b", "have", None),
    (r"\bbave\b", "have", None),
    (r"\bliave\b", "have", None),
    # 'been' variants
    (r"\bboen\b", "been", None),
    # 'from' variants
    (r"\bfrorn\b", "from", None),
    # 'were' variants
    (r"\bwero\b", "were", None),
    # 'would' variants
    (r"\bwonld\b", "would", None),
    (r"\bwouid\b", "would", None),
    # 'could' variants
    (r"\bconld\b", "could", None),
    (r"\bcouid\b", "could", None),
    # 'should' variants
    (r"\bsbould\b", "should", None),
    (r"\bshouid\b", "should", None),
    # 'being' variants
    (r"\bbeiug\b", "being", None),
    # 'made' variants
    (r"\bmado\b", "made", None),
    # 'upon' variants
    (r"\bnpon\b", "upon", None),
    # 'such' variants
    (r"\bsucb\b", "such", None),
    (r"\bsucli\b", "such", None),
    # 'some' variants
    (r"\bsomo\b", "some", None),
    # 'very' variants
    (r"\bverv\b", "very", None),
    # 'first' variants (2490 occurrences of llrst)
    (r"\bllrst\b", "first", None),
    (r"\bfirst\b", "first", None),
    # 'still' variants (3097 occurrences - long s confusion)
    (r"\bftill\b", "still", None),
    # ==========================================================================
    # Long s (ſ) -> s (common in pre-1800 texts)
    # ==========================================================================
    (r"ſ", "s", None),
    # ==========================================================================
    # Common 'rn' <-> 'm' confusion
    # ==========================================================================
    (r"\brnay\b", "may", None),
    (r"\brnuch\b", "much", None),
    (r"\brnore\b", "more", None),
    (r"\bsarne\b", "same", None),
    (r"\btirne\b", "time", None),
    (r"\bnarne\b", "name", None),
    (r"\bcorne\b", "come", None),
    (r"\bhorne\b", "home", None),
    # ==========================================================================
    # 'ii' -> 'n' confusion
    # ==========================================================================
    (r"\bkiiow\b", "know", None),
    (r"\bkiiown\b", "known", None),
    # ==========================================================================
    # Common 'cl' -> 'd' confusion
    # ==========================================================================
    (r"\bclo\b", "do", r"\b(to|not|can|will|shall|would|could)\s+clo\b"),
    # ==========================================================================
    # Fix broken ligatures (fi, fl, ff, ffi, ffl)
    # ==========================================================================
    (r"ﬁ", "fi", None),
    (r"ﬂ", "fl", None),
    (r"ﬀ", "ff", None),
    (r"ﬃ", "ffi", None),
    (r"ﬄ", "ffl", None),
    # ==========================================================================
    # Google digitization watermark artifacts (anachronistic, safe to remove)
    # These appear in Google Books scans and are never legitimate pre-WWI content
    # ==========================================================================
    (r"VjOOQIC", "", None),
    (r"VjOOQLC", "", None),
    (r"LjOOQIC", "", None),
    (r"LiOOQLC", "", None),
    (r"CjOOQIC", "", None),
    (r"CjOOQlC", "", None),
    (r"byVjOOQlC", "", None),
    (r"byVrrOOQlC", "", None),
    (r"byCjOOQlC", "", None),
    (r"hyGoogIc", "", None),
    (r"GoOglc", "", None),
    (r"GoogXt", "", None),
    (r"DigiLizedbyGoOglc", "", None),
    (r"Digitized\s+by\s+[VLC]j?OOQ(?:IC|LC|lC)", "", None),
    # ==========================================================================
    # Repeated letter OCR artifacts (never legitimate words)
    # Examples: EEE, OOO, NNN, WWW, AAA, BBB, DDD, FFF (3+ same letter)
    # Excludes I, X, C, M, L, V which are Roman numeral components
    # ==========================================================================
    (r"\b([ABENODFGHJKPQRSTUWYZ])\1{2,}\b", "", None),
    # ==========================================================================
    # Clear 2-3 letter OCR noise (high frequency, never words)
    # Only the most obvious patterns with 10k+ occurrences
    # ==========================================================================
    (r"[I1]A", "", None),  # 94,675 occurrences in newspaper corpus
    (r"[I1]H", "", None),  # 65,786 occurrences in newspaper corpus
]

# Patterns that indicate garbage OCR (not fixable, flag for review)
GARBAGE_PATTERNS = [
    r"[^\x00-\x7F]{10,}",  # Long runs of non-ASCII
    r"[bcdfghjklmnpqrstvwxz]{6,}",  # Long consonant runs
    r"\d{2,}[a-z]+\d{2,}",  # Numbers mixed into words oddly
    r"[|l1I]{5,}",  # Pipe/l/1/I confusion runs
]


@dataclass
class CleanupStats:
    """Track cleanup statistics."""

    total_files: int = 0
    files_modified: int = 0
    files_flagged: int = 0
    total_substitutions: int = 0
    substitution_counts: Counter = field(default_factory=Counter)
    flagged_files: list = field(default_factory=list)

    def to_dict(self):
        return {
            "total_files": self.total_files,
            "files_modified": self.files_modified,
            "files_flagged": self.files_flagged,
            "total_substitutions": self.total_substitutions,
            "top_substitutions": self.substitution_counts.most_common(50),
            "flagged_files": self.flagged_files[:100],
        }


def check_garbage(text: str) -> list[tuple[str, int]]:
    """Check for unfixable garbage patterns. Returns list of (pattern, count)."""
    issues = []
    for pattern in GARBAGE_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if len(matches) > 5:
            issues.append((pattern, len(matches)))
    return issues


def clean_text(text: str, stats: Optional[CleanupStats] = None) -> tuple[str, int]:
    """
    Apply OCR cleanup substitutions to text.

    Returns: (cleaned_text, substitution_count)
    """
    total_subs = 0

    for pattern, replacement, context in OCR_SUBSTITUTIONS:
        if context:

            def contextual_replace(match):
                nonlocal total_subs
                result = re.sub(pattern, replacement, match.group(0), flags=re.IGNORECASE)
                if result != match.group(0):
                    total_subs += 1
                    if stats:
                        stats.substitution_counts[f"{pattern} -> {replacement}"] += 1
                return result

            text = re.sub(context, contextual_replace, text, flags=re.IGNORECASE)
        else:
            count_before = len(re.findall(pattern, text, re.IGNORECASE))
            if count_before > 0:
                text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
                total_subs += count_before
                if stats:
                    stats.substitution_counts[f"{pattern} -> {replacement}"] += count_before

    return text, total_subs


def clean_file(
    input_path: Path,
    output_path: Optional[Path] = None,
    stats: Optional[CleanupStats] = None,
) -> tuple[bool, int, list]:
    """
    Clean a single file.

    Returns: (was_modified, substitution_count, garbage_issues)
    """
    try:
        with open(input_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except Exception as e:
        print(f"  Error reading {input_path}: {e}")
        return False, 0, []

    garbage_issues = check_garbage(content)
    cleaned, sub_count = clean_text(content, stats)
    was_modified = sub_count > 0

    if output_path and was_modified:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(cleaned)
    elif output_path and not was_modified:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)

    return was_modified, sub_count, garbage_issues


def clean_batch(
    input_dir: Path,
    output_dir: Optional[Path] = None,
    file_pattern: str = "*.txt",
) -> CleanupStats:
    """Clean all text files in a directory."""
    stats = CleanupStats()

    input_files = list(input_dir.glob(f"**/{file_pattern}"))
    stats.total_files = len(input_files)

    print(f"Processing {stats.total_files} files...")

    for i, input_path in enumerate(input_files, 1):
        if i % 100 == 0:
            print(f"  Progress: {i}/{stats.total_files}")

        if output_dir:
            relative = input_path.relative_to(input_dir)
            output_path = output_dir / relative
        else:
            output_path = None

        was_modified, sub_count, garbage = clean_file(input_path, output_path, stats)

        if was_modified:
            stats.files_modified += 1
            stats.total_substitutions += sub_count

        if garbage:
            stats.files_flagged += 1
            stats.flagged_files.append(
                {
                    "file": str(input_path),
                    "issues": garbage,
                }
            )

    return stats


def analyze_corpus(corpus_dir: Path, sample_size: int = 1000) -> dict:
    """
    Analyze a corpus for OCR error patterns without modifying.

    Returns analysis report.
    """
    files = list(corpus_dir.glob("**/*.txt"))
    if len(files) > sample_size:
        import random

        files = random.sample(files, sample_size)

    error_counts = Counter()
    garbage_files = []
    total_words = 0

    print(f"Analyzing {len(files)} files...")

    for filepath in files:
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
        except Exception:
            continue

        words = content.split()
        total_words += len(words)

        for pattern, replacement, _ in OCR_SUBSTITUTIONS:
            matches = re.findall(pattern, content, re.IGNORECASE)
            if matches:
                error_counts[f"{pattern} -> {replacement}"] += len(matches)

        garbage = check_garbage(content)
        if garbage:
            garbage_files.append(str(filepath))

    return {
        "files_analyzed": len(files),
        "total_words": total_words,
        "potential_errors": error_counts.most_common(50),
        "garbage_files": garbage_files[:50],
        "estimated_error_rate": sum(error_counts.values()) / total_words if total_words > 0 else 0,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Clean OCR errors in historical texts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  clean    Clean a single file
  batch    Clean all files in a directory
  analyze  Analyze corpus for errors without modifying

Examples:
  tc-ocr-clean clean input.txt -o output.txt
  tc-ocr-clean batch ./corpus_raw -o ./corpus_clean
  tc-ocr-clean analyze ./corpus --report analysis.json
        """,
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Clean single file
    clean_parser = subparsers.add_parser("clean", help="Clean a single file")
    clean_parser.add_argument("input", type=Path, help="Input file")
    clean_parser.add_argument("-o", "--output", type=Path, help="Output file (default: stdout)")

    # Batch clean
    batch_parser = subparsers.add_parser("batch", help="Clean all files in directory")
    batch_parser.add_argument("input_dir", type=Path, help="Input directory")
    batch_parser.add_argument("-o", "--output-dir", type=Path, help="Output directory")
    batch_parser.add_argument("--pattern", default="*.txt", help="File pattern (default: *.txt)")
    batch_parser.add_argument("--report", type=Path, help="Save stats report to JSON")

    # Analyze
    analyze_parser = subparsers.add_parser("analyze", help="Analyze corpus for OCR errors")
    analyze_parser.add_argument("corpus_dir", type=Path, help="Corpus directory")
    analyze_parser.add_argument("--sample", type=int, default=1000, help="Sample size")
    analyze_parser.add_argument("--report", type=Path, help="Save report to JSON")

    args = parser.parse_args()

    if args.command == "clean":
        was_modified, sub_count, garbage = clean_file(args.input, args.output)

        if args.output:
            print(f"Cleaned {args.input} -> {args.output}")
            print(f"  Substitutions: {sub_count}")
            if garbage:
                print(f"  Warning: {len(garbage)} garbage patterns detected")
        else:
            with open(args.input, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
            cleaned, _ = clean_text(content)
            print(cleaned)

    elif args.command == "batch":
        stats = clean_batch(args.input_dir, args.output_dir, args.pattern)

        print(f"\n{'=' * 60}")
        print("Batch cleanup complete")
        print(f"{'=' * 60}")
        print(f"  Total files: {stats.total_files}")
        print(f"  Files modified: {stats.files_modified}")
        print(f"  Files flagged (garbage): {stats.files_flagged}")
        print(f"  Total substitutions: {stats.total_substitutions}")

        if stats.substitution_counts:
            print("\nTop substitutions:")
            for pattern, count in stats.substitution_counts.most_common(10):
                print(f"  {pattern}: {count}")

        if args.report:
            with open(args.report, "w") as f:
                json.dump(stats.to_dict(), f, indent=2)
            print(f"\nReport saved to {args.report}")

    elif args.command == "analyze":
        report = analyze_corpus(args.corpus_dir, args.sample)

        print(f"\n{'=' * 60}")
        print("Corpus Analysis")
        print(f"{'=' * 60}")
        print(f"  Files analyzed: {report['files_analyzed']}")
        print(f"  Total words: {report['total_words']:,}")
        print(f"  Estimated error rate: {report['estimated_error_rate']:.4%}")
        print(f"  Files with garbage patterns: {len(report['garbage_files'])}")

        if report["potential_errors"]:
            print("\nTop potential errors:")
            for pattern, count in report["potential_errors"][:15]:
                print(f"  {pattern}: {count}")

        if args.report:
            with open(args.report, "w") as f:
                json.dump(report, f, indent=2)
            print(f"\nReport saved to {args.report}")


if __name__ == "__main__":
    main()
