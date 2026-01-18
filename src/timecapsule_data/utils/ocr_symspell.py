#!/usr/bin/env python3
"""
SymSpell-based OCR Cleanup

Uses SymSpell for fast, dictionary-based spell correction of OCR errors.
Much more comprehensive than the pattern-based tc-ocr-clean.

SymSpell uses symmetric delete algorithm - very fast (1M+ corrections/sec)
but limited to words within edit distance of dictionary entries.

Usage:
    # Clean a single file
    tc-ocr-symspell clean input.txt -o output.txt

    # Clean entire corpus
    tc-ocr-symspell batch ./corpus_raw -o ./corpus_clean

    # Analyze what would be corrected (dry run)
    tc-ocr-symspell analyze input.txt
"""

import argparse
import importlib.resources
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from symspellpy import SymSpell, Verbosity

# =============================================================================
# SymSpell Setup
# =============================================================================


def create_symspell(max_edit_distance: int = 2) -> SymSpell:
    """
    Create and load SymSpell with frequency dictionary.
    """
    sym_spell = SymSpell(max_dictionary_edit_distance=max_edit_distance)

    dictionary_path = str(
        importlib.resources.files("symspellpy").joinpath("frequency_dictionary_en_82_765.txt")
    )
    sym_spell.load_dictionary(dictionary_path, term_index=0, count_index=1)

    return sym_spell


# =============================================================================
# Text Processing
# =============================================================================

WORD_PATTERN = re.compile(r"\b([a-zA-Z]+(?:'[a-zA-Z]+)?)\b")

# Words/patterns to never correct
SKIP_WORDS = {
    # Single letters
    "i",
    "a",
    "o",
    # Common abbreviations
    "mr",
    "mrs",
    "ms",
    "dr",
    "jr",
    "sr",
    "st",
    "nd",
    "rd",
    "th",
    "vs",
    "etc",
    "ie",
    "eg",
    "cf",
    "al",
    "et",
    "esq",
    "hon",
    "rev",
    "messrs",
    "mesdames",
    # Roman numerals
    "ii",
    "iii",
    "iv",
    "vi",
    "vii",
    "viii",
    "ix",
    "xi",
    "xii",
    "xx",
    # Word fragments (OCR splits) - don't "fix" these to wrong words
    "ing",
    "tion",
    "sion",
    "ment",
    "ness",
    "ful",
    "less",
    "able",
    "ible",
    "ence",
    "ance",
    "ous",
    "ive",
    "tive",
    "ary",
    "ory",
    "ity",
    "ty",
    "pre",
    "pro",
    "anti",
    "dis",
    "mis",
    "non",
    "sub",
    "super",
    "un",
    "ly",
    "er",
    "est",
    "ed",
    "es",
    "en",
    "al",
    "ic",
    "ical",
    # American spellings (don't convert to British)
    "labor",
    "color",
    "honor",
    "favor",
    "neighbor",
    "behavior",
    "center",
    "theater",
    "fiber",
    "meter",
    "liter",
    "colored",
    "honored",
    "favored",
    "labored",
}

MAX_WORD_LENGTH = 25


@dataclass
class CorrectionStats:
    """Statistics from a correction run."""

    total_words: int = 0
    corrected_words: int = 0
    skipped_words: int = 0
    corrections: Counter = field(default_factory=Counter)

    def to_dict(self) -> dict:
        return {
            "total_words": self.total_words,
            "corrected_words": self.corrected_words,
            "skipped_words": self.skipped_words,
            "correction_rate": self.corrected_words / self.total_words
            if self.total_words > 0
            else 0,
            "top_corrections": self.corrections.most_common(50),
        }


def should_skip_word(word: str) -> bool:
    """Check if word should be skipped."""
    word_lower = word.lower()

    if word_lower in SKIP_WORDS:
        return True
    if len(word) == 1 and word_lower not in ("a", "i", "o"):
        return True
    if len(word) > MAX_WORD_LENGTH:
        return True
    if any(c.isdigit() for c in word):
        return True
    # Skip short all-caps (acronyms)
    if word.isupper() and len(word) <= 5:
        return True
    # Skip words that look like proper nouns (capitalized, not start of sentence)
    # This is imperfect but helps avoid mangling names

    return False


def preserve_case(original: str, corrected: str) -> str:
    """Preserve case pattern of original in correction."""
    if original.isupper():
        return corrected.upper()
    elif original.istitle():
        return corrected.title()
    elif original.islower():
        return corrected.lower()
    else:
        return corrected.lower()


def correct_word(
    word: str,
    sym_spell: SymSpell,
    stats: Optional[CorrectionStats] = None,
    max_edit_distance: int = 2,
    min_word_length: int = 4,
) -> str:
    """
    Attempt to correct a single word using SymSpell.
    """
    if stats:
        stats.total_words += 1

    # More conservative: require min length of 4
    if should_skip_word(word) or len(word) < min_word_length:
        if stats:
            stats.skipped_words += 1
        return word

    suggestions = sym_spell.lookup(
        word.lower(),
        Verbosity.CLOSEST,
        max_edit_distance=max_edit_distance,
    )

    if not suggestions:
        return word

    best = suggestions[0]

    # No change needed
    if best.term.lower() == word.lower():
        return word

    # Conservative: for words <= 5 chars, only accept distance 1
    if len(word) <= 5 and best.distance > 1:
        return word

    # Require high frequency suggestion
    if best.count < 1000:
        return word

    # Don't correct if it would just change American->British spelling
    if best.term.lower().replace("ou", "o") == word.lower():
        return word
    if best.term.lower().replace("re", "er") == word.lower():
        return word

    corrected = preserve_case(word, best.term)

    if stats:
        stats.corrected_words += 1
        stats.corrections[f"{word} -> {corrected}"] += 1

    return corrected


def correct_text(
    text: str,
    sym_spell: SymSpell,
    stats: Optional[CorrectionStats] = None,
    max_edit_distance: int = 2,
) -> str:
    """Correct all words in text using SymSpell."""
    result = []
    last_end = 0

    for match in WORD_PATTERN.finditer(text):
        result.append(text[last_end : match.start()])
        word = match.group(1)
        corrected = correct_word(word, sym_spell, stats, max_edit_distance)
        result.append(corrected)
        last_end = match.end()

    result.append(text[last_end:])
    return "".join(result)


def correct_file(
    input_path: Path,
    output_path: Optional[Path],
    sym_spell: SymSpell,
    stats: Optional[CorrectionStats] = None,
    max_edit_distance: int = 2,
) -> bool:
    """Correct a single file. Returns True if modified."""
    try:
        content = input_path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        print(f"  Error reading {input_path}: {e}", file=sys.stderr)
        return False

    corrected = correct_text(content, sym_spell, stats, max_edit_distance)
    was_modified = corrected != content

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(corrected, encoding="utf-8")

    return was_modified


# =============================================================================
# CLI Commands
# =============================================================================


def cmd_clean(args):
    """Clean a single file."""
    print("Loading SymSpell dictionary...", file=sys.stderr)
    sym_spell = create_symspell(args.max_edit_distance)
    print(f"Dictionary loaded: {len(sym_spell.words):,} words", file=sys.stderr)

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else None

    stats = CorrectionStats()
    correct_file(input_path, output_path, sym_spell, stats, args.max_edit_distance)

    print(f"\nFile: {input_path}")
    print(f"Total words: {stats.total_words:,}")
    print(
        f"Corrected: {stats.corrected_words:,} ({stats.corrected_words / stats.total_words * 100:.1f}%)"
    )
    print(f"Skipped: {stats.skipped_words:,}")

    if stats.corrections:
        print("\nTop corrections:")
        for corr, count in stats.corrections.most_common(15):
            print(f"  {corr} ({count}x)")

    if output_path:
        print(f"\nOutput written to: {output_path}")


def cmd_batch(args):
    """Clean all files in a directory."""
    print("Loading SymSpell dictionary...", file=sys.stderr)
    sym_spell = create_symspell(args.max_edit_distance)
    print(f"Dictionary loaded: {len(sym_spell.words):,} words", file=sys.stderr)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir) if args.output_dir else None

    files = list(input_dir.rglob(args.pattern))
    total = len(files)

    print(f"\nProcessing {total} files...")

    stats = CorrectionStats()
    modified_count = 0

    for i, file_path in enumerate(files, 1):
        if i % 100 == 0 or i == total:
            print(f"  Progress: {i}/{total} ({i / total * 100:.0f}%)")

        if output_dir:
            relative = file_path.relative_to(input_dir)
            out_path = output_dir / relative
        else:
            out_path = None

        if correct_file(file_path, out_path, sym_spell, stats, args.max_edit_distance):
            modified_count += 1

    print(f"\n{'=' * 60}")
    print("BATCH CORRECTION COMPLETE")
    print(f"{'=' * 60}")
    print(f"Total files: {total:,}")
    print(f"Files modified: {modified_count:,}")
    print(f"Total words processed: {stats.total_words:,}")
    print(
        f"Words corrected: {stats.corrected_words:,} ({stats.corrected_words / stats.total_words * 100:.2f}%)"
    )

    if stats.corrections:
        print("\nTop corrections:")
        for corr, count in stats.corrections.most_common(20):
            print(f"  {corr} ({count:,}x)")

    if args.report:
        with open(args.report, "w") as f:
            json.dump(stats.to_dict(), f, indent=2)
        print(f"\nReport saved to: {args.report}")


def cmd_analyze(args):
    """Analyze what would be corrected without modifying."""
    print("Loading SymSpell dictionary...", file=sys.stderr)
    sym_spell = create_symspell(args.max_edit_distance)

    input_path = Path(args.input)
    content = input_path.read_text(encoding="utf-8", errors="replace")

    stats = CorrectionStats()
    _ = correct_text(content, sym_spell, stats, args.max_edit_distance)

    print(f"\nAnalysis of: {input_path}")
    print(f"{'=' * 60}")
    print(f"Total words: {stats.total_words:,}")
    print(
        f"Would correct: {stats.corrected_words:,} ({stats.corrected_words / stats.total_words * 100:.1f}%)"
    )
    print(f"Skipped: {stats.skipped_words:,}")

    if stats.corrections:
        print("\nProposed corrections:")
        for corr, count in stats.corrections.most_common(30):
            print(f"  {corr} ({count}x)")


def main():
    parser = argparse.ArgumentParser(
        description="SymSpell-based OCR Cleanup - Fast dictionary spell correction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
SymSpell uses symmetric delete algorithm for very fast spell correction.

Best for:
  - Common misspellings and OCR errors within edit distance 2
  - Words close to correct dictionary spellings

Limitations:
  - Cannot fix severely corrupted words (edit distance > 2)
  - May not recognize proper nouns or historical terms
  - Conservative by default to avoid false positives

Examples:
  tc-ocr-symspell clean document.txt -o cleaned.txt
  tc-ocr-symspell batch ./corpus_raw -o ./corpus_clean --report stats.json
  tc-ocr-symspell analyze document.txt
        """,
    )

    parser.add_argument(
        "--max-edit-distance",
        type=int,
        default=2,
        help="Maximum edit distance for corrections (default: 2)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    clean_parser = subparsers.add_parser("clean", help="Clean a single file")
    clean_parser.add_argument("input", type=str, help="Input file")
    clean_parser.add_argument("-o", "--output", type=str, help="Output file")

    batch_parser = subparsers.add_parser("batch", help="Clean all files in directory")
    batch_parser.add_argument("input_dir", type=str, help="Input directory")
    batch_parser.add_argument("-o", "--output-dir", type=str, help="Output directory")
    batch_parser.add_argument("--pattern", default="*.txt", help="File pattern (default: *.txt)")
    batch_parser.add_argument("--report", type=str, help="Save stats report to JSON")

    analyze_parser = subparsers.add_parser("analyze", help="Analyze without modifying")
    analyze_parser.add_argument("input", type=str, help="Input file")

    args = parser.parse_args()

    if args.command == "clean":
        cmd_clean(args)
    elif args.command == "batch":
        cmd_batch(args)
    elif args.command == "analyze":
        cmd_analyze(args)


if __name__ == "__main__":
    main()
