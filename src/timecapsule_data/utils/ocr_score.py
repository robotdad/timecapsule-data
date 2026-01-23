#!/usr/bin/env python3
"""
OCR Error Rate Scorer

Scores text files based on dictionary-based OCR error detection.
Uses word frequency and dictionary lookups to estimate the proportion
of corrupted/unrecognizable words.

This is the triage step - identifies which files need more intensive cleanup.

Usage:
    # Score a single file
    tc-ocr-score check input.txt

    # Score entire corpus and generate report
    tc-ocr-score analyze ./corpus --report scores.json

    # Filter corpus by quality threshold
    tc-ocr-score filter ./corpus --threshold 0.15 --output-good ./good --output-bad ./bad
"""

import argparse
import json
import re
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

# =============================================================================
# Dictionary Management
# =============================================================================


class Dictionary:
    """
    Multi-source dictionary for OCR error detection.

    Uses Rust multi-language dictionaries (en, de, fr, la) via rust_ocr_clean module.
    """

    def __init__(self):
        self.words: set[str] = set()
        self._rust_dict_loaded = False
        self._load_dictionary()

    def _load_dictionary(self):
        """Load dictionary using Rust module (required - no fallback)."""
        from pathlib import Path

        import rust_ocr_clean  # type: ignore[import-not-found]

        dict_dir = Path(__file__).parent.parent.parent.parent / "rust-ocr-clean" / "dictionaries"
        if dict_dir.exists():
            rust_ocr_clean.init_dictionaries(str(dict_dir))
            self._rust_dict_loaded = rust_ocr_clean.dictionaries_loaded()

        # Still load supplementary word sets for historical/domain terms
        self._add_common_words()
        self._add_common_inflections()
        self._add_historical_vocabulary()
        self._add_common_names()

    def _add_common_words(self):
        """Add most common English words."""
        common = {
            "the",
            "be",
            "to",
            "of",
            "and",
            "a",
            "in",
            "that",
            "have",
            "i",
            "it",
            "for",
            "not",
            "on",
            "with",
            "he",
            "as",
            "you",
            "do",
            "at",
            "this",
            "but",
            "his",
            "by",
            "from",
            "they",
            "we",
            "say",
            "her",
            "she",
            "or",
            "an",
            "will",
            "my",
            "one",
            "all",
            "would",
            "there",
            "their",
            "what",
            "so",
            "up",
            "out",
            "if",
            "about",
            "who",
            "get",
            "which",
            "go",
            "me",
            "when",
            "make",
            "can",
            "like",
            "time",
            "no",
            "just",
            "him",
            "know",
            "take",
            "people",
            "into",
            "year",
            "your",
            "good",
            "some",
            "could",
            "them",
            "see",
            "other",
            "than",
            "then",
            "now",
            "look",
            "only",
            "come",
            "its",
            "over",
            "think",
            "also",
            "back",
            "after",
            "use",
            "two",
            "how",
            "our",
            "work",
            "first",
            "well",
            "way",
            "even",
            "new",
            "want",
            "because",
            "any",
            "these",
            "give",
            "day",
            "most",
            "us",
            "very",
            "has",
            "had",
            "was",
            "were",
            "been",
            "being",
            "is",
            "are",
            "am",
        }
        self.words.update(common)

    def _add_common_inflections(self):
        """Add common inflected forms that NLTK misses."""
        inflections = {
            # Plurals
            "years",
            "days",
            "times",
            "ways",
            "things",
            "men",
            "women",
            "children",
            "people",
            "words",
            "students",
            "members",
            "others",
            "hands",
            "eyes",
            "friends",
            "books",
            "letters",
            "pages",
            "lines",
            "places",
            "states",
            "parts",
            "points",
            "facts",
            "cases",
            "questions",
            "matters",
            "rights",
            # Past tense
            "said",
            "made",
            "found",
            "gave",
            "told",
            "asked",
            "used",
            "tried",
            "called",
            "seemed",
            "left",
            "felt",
            "became",
            "got",
            "kept",
            "let",
            "began",
            "brought",
            "heard",
            "played",
            "moved",
            "lived",
            "believed",
            "held",
            "stood",
            "showed",
            "followed",
            "turned",
            "reached",
            "issued",
            # Present tense third person
            "says",
            "makes",
            "finds",
            "gives",
            "tells",
            "asks",
            "uses",
            "tries",
            "calls",
            "seems",
            "feels",
            "becomes",
            "gets",
            "keeps",
            "lets",
            "begins",
            "brings",
            "shows",
            "follows",
            "turns",
            "means",
            "needs",
            "wants",
            # -ing forms
            "being",
            "having",
            "doing",
            "going",
            "coming",
            "making",
            "taking",
            "getting",
            "saying",
            "looking",
            "thinking",
            "working",
            "trying",
            "using",
            "finding",
            "giving",
            "telling",
            "asking",
            "leaving",
            # Contractions
            "don't",
            "won't",
            "can't",
            "didn't",
            "wouldn't",
            "couldn't",
            "shouldn't",
            "isn't",
            "aren't",
            "wasn't",
            "weren't",
            "hasn't",
            "haven't",
            "hadn't",
            "i'm",
            "you're",
            "we're",
            "they're",
            "he's",
            "she's",
            "it's",
            "that's",
            "i've",
            "you've",
            "we've",
            "they've",
            "i'd",
            "you'd",
            "we'd",
            "they'd",
            "i'll",
            "you'll",
            "we'll",
            "they'll",
            "he'll",
            "she'll",
        }
        self.words.update(inflections)

    def _add_historical_vocabulary(self):
        """Add vocabulary common in historical texts but rare today."""
        historical = {
            # Archaic pronouns/words
            "thee",
            "thou",
            "thy",
            "thine",
            "ye",
            "hath",
            "doth",
            "dost",
            "hast",
            "wherefore",
            "whence",
            "thence",
            "hence",
            "whilst",
            "amongst",
            "towards",
            "betwixt",
            "forsooth",
            "perchance",
            "mayhap",
            "methinks",
            "prithee",
            # British spellings
            "parlour",
            "honour",
            "favour",
            "colour",
            "labour",
            "behaviour",
            "neighbour",
            "centre",
            "theatre",
            "fibre",
            "metre",
            "litre",
            "calibre",
            "lustre",
            "connexion",
            "reflexion",
            "inflexion",
            "despatch",
            "gaol",
            "kerb",
            "tyre",
            "plough",
            "cheque",
            "storey",
            "waggon",
            "grey",
            "judgement",
            "acknowledgement",
            # Period-specific technology/terms
            "telegraph",
            "railway",
            "steamship",
            "gaslight",
            "omnibus",
            "typewriter",
            "phonograph",
            "daguerreotype",
            # Titles and forms of address
            "esq",
            "messrs",
            "mesdames",
            "reverend",
            "honourable",
            "excellency",
            "dr",
            "mr",
            "mrs",
            "ms",
            "prof",
            "rev",
            "hon",
            "jr",
            "sr",
        }
        self.words.update(historical)

    def _add_common_names(self):
        """Add common proper nouns that appear frequently."""
        names = {
            # Countries/places
            "england",
            "britain",
            "america",
            "france",
            "germany",
            "russia",
            "london",
            "paris",
            "berlin",
            "rome",
            "vienna",
            "washington",
            "europe",
            "asia",
            "africa",
            "atlantic",
            "pacific",
            "philadelphia",
            "boston",
            "chicago",
            "york",
            # Common given names
            "john",
            "james",
            "william",
            "henry",
            "george",
            "charles",
            "thomas",
            "mary",
            "elizabeth",
            "ann",
            "sarah",
            "jane",
            "margaret",
            "alice",
            # Months, days
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
            "jan",
            "feb",
            "mar",
            "apr",
            "jun",
            "jul",
            "aug",
            "sep",
            "sept",
            "oct",
            "nov",
            "dec",
        }
        self.words.update(names)

    def add_corpus_vocabulary(self, vocab_file: Path):
        """Add vocabulary extracted from a corpus."""
        if vocab_file.exists():
            with open(vocab_file) as f:
                for line in f:
                    word = line.strip().lower()
                    if word and len(word) > 1:
                        self.words.add(word)

    def is_word(self, word: str) -> bool:
        """Check if a word is in the dictionary."""
        import rust_ocr_clean  # type: ignore[import-not-found]

        word_lower = word.lower()

        # Fast check against known word set first
        if word_lower in self.words:
            return True

        # Use Rust dictionaries for comprehensive check (en, de, fr, la)
        if self._rust_dict_loaded:
            return rust_ocr_clean.is_known_word(word)

        return False

    def __contains__(self, word: str) -> bool:
        return self.is_word(word)

    def __len__(self) -> int:
        if self._rust_dict_loaded:
            return len(self.words) + 500000  # Approximate multi-lang dict size
        return len(self.words)


# =============================================================================
# Text Analysis
# =============================================================================

# Regex for extracting words (letters only, handles contractions)
WORD_PATTERN = re.compile(r"[a-zA-Z]+(?:'[a-zA-Z]+)?")

# Patterns that indicate OCR garbage (not fixable words)
GARBAGE_PATTERNS = [
    re.compile(r"[bcdfghjklmnpqrstvwxz]{5,}", re.I),  # Long consonant runs
    re.compile(r"[aeiou]{4,}", re.I),  # Long vowel runs
    re.compile(r"(.)\1{3,}"),  # 4+ repeated characters
    re.compile(r"[^a-zA-Z\s]{3,}"),  # 3+ consecutive symbols
]


def extract_words(text: str) -> list[str]:
    """Extract words from text, filtering very short tokens."""
    words = WORD_PATTERN.findall(text)
    # Filter single letters except 'a', 'i', 'o' (valid words)
    return [w for w in words if len(w) > 1 or w.lower() in ("a", "i", "o")]


def is_garbage(word: str) -> bool:
    """Check if a word matches garbage patterns (unfixable OCR noise)."""
    for pattern in GARBAGE_PATTERNS:
        if pattern.search(word):
            return True
    return False


def is_number_like(word: str) -> bool:
    """Check if word is a number or number-like (dates, roman numerals)."""
    # Roman numerals
    if re.match(r"^[ivxlcdm]+$", word, re.I):
        return True
    # Ordinals
    if re.match(r"^\d+(st|nd|rd|th)$", word, re.I):
        return True
    return False


@dataclass
class ScoreResult:
    """Result of OCR quality scoring."""

    file_path: str
    total_words: int
    unknown_words: int
    garbage_words: int
    error_rate: float  # unknown / total
    garbage_rate: float  # garbage / total
    combined_score: float  # weighted combination
    quality_tier: str  # 'good', 'moderate', 'poor', 'garbage'
    sample_unknown: list[str]  # Sample of unknown words for debugging

    def to_dict(self) -> dict:
        return asdict(self)


def score_text(text: str, dictionary: Dictionary, file_path: str = "") -> ScoreResult:
    """
    Score text for OCR quality.

    Returns a ScoreResult with error rates and quality tier.
    """
    words = extract_words(text)
    total = len(words)

    if total == 0:
        return ScoreResult(
            file_path=file_path,
            total_words=0,
            unknown_words=0,
            garbage_words=0,
            error_rate=1.0,
            garbage_rate=0.0,
            combined_score=1.0,
            quality_tier="garbage",
            sample_unknown=[],
        )

    unknown = []
    garbage = []

    for word in words:
        # Skip numbers and number-like tokens
        if is_number_like(word):
            continue

        # Check for garbage patterns
        if is_garbage(word):
            garbage.append(word)
            continue

        # Check dictionary
        if not dictionary.is_word(word):
            unknown.append(word)

    unknown_count = len(unknown)
    garbage_count = len(garbage)

    error_rate = unknown_count / total
    garbage_rate = garbage_count / total

    # Combined score: garbage is worse than unknown words
    combined_score = error_rate + (garbage_rate * 2)
    combined_score = min(1.0, combined_score)  # Cap at 1.0

    # Determine quality tier
    if combined_score < 0.05:
        tier = "good"
    elif combined_score < 0.10:
        tier = "moderate"
    elif combined_score < 0.20:
        tier = "poor"
    else:
        tier = "garbage"

    # Get sample of unknown words (most common)
    unknown_counter = Counter(unknown)
    sample_unknown = [w for w, _ in unknown_counter.most_common(20)]

    return ScoreResult(
        file_path=file_path,
        total_words=total,
        unknown_words=unknown_count,
        garbage_words=garbage_count,
        error_rate=round(error_rate, 4),
        garbage_rate=round(garbage_rate, 4),
        combined_score=round(combined_score, 4),
        quality_tier=tier,
        sample_unknown=sample_unknown,
    )


def score_file(file_path: Path, dictionary: Dictionary) -> ScoreResult:
    """Score a single file."""
    try:
        text = file_path.read_text(encoding="utf-8", errors="replace")
        return score_text(text, dictionary, str(file_path))
    except Exception as e:
        return ScoreResult(
            file_path=str(file_path),
            total_words=0,
            unknown_words=0,
            garbage_words=0,
            error_rate=1.0,
            garbage_rate=0.0,
            combined_score=1.0,
            quality_tier="error",
            sample_unknown=[f"Error: {e}"],
        )


# =============================================================================
# Corpus Analysis
# =============================================================================


def analyze_corpus(corpus_dir: Path, dictionary: Dictionary, limit: Optional[int] = None) -> dict:
    """
    Analyze all files in a corpus directory.

    Returns statistics and per-file scores.
    """
    files = list(corpus_dir.rglob("*.txt"))
    if limit:
        files = files[:limit]

    total = len(files)
    print(f"Scoring {total} files...")

    results = []
    tier_counts = Counter()

    for i, file_path in enumerate(files, 1):
        if i % 500 == 0:
            print(f"  Progress: {i}/{total} ({i / total * 100:.0f}%)")

        result = score_file(file_path, dictionary)
        results.append(result.to_dict())
        tier_counts[result.quality_tier] += 1

    # Sort by combined score (worst first)
    results.sort(key=lambda x: x["combined_score"], reverse=True)

    # Calculate summary statistics
    scores = [r["combined_score"] for r in results]

    summary = {
        "total_files": total,
        "tier_distribution": dict(tier_counts),
        "score_percentiles": {
            "p10": sorted(scores)[int(len(scores) * 0.1)] if scores else 0,
            "p25": sorted(scores)[int(len(scores) * 0.25)] if scores else 0,
            "p50": sorted(scores)[int(len(scores) * 0.5)] if scores else 0,
            "p75": sorted(scores)[int(len(scores) * 0.75)] if scores else 0,
            "p90": sorted(scores)[int(len(scores) * 0.9)] if scores else 0,
        },
        "mean_score": sum(scores) / len(scores) if scores else 0,
        "files": results,
    }

    return summary


# =============================================================================
# CLI
# =============================================================================


def cmd_check(args, dictionary: Dictionary):
    """Check a single file."""
    result = score_file(Path(args.file), dictionary)

    print(f"File: {result.file_path}")
    print(f"Total words: {result.total_words:,}")
    print(f"Unknown words: {result.unknown_words:,} ({result.error_rate:.1%})")
    print(f"Garbage words: {result.garbage_words:,} ({result.garbage_rate:.1%})")
    print(f"Combined score: {result.combined_score:.3f}")
    print(f"Quality tier: {result.quality_tier.upper()}")

    if result.sample_unknown:
        print("\nSample unknown words:")
        for word in result.sample_unknown[:15]:
            print(f"  - {word}")


def cmd_analyze(args, dictionary: Dictionary):
    """Analyze entire corpus."""
    corpus_dir = Path(args.corpus_dir)

    if not corpus_dir.exists():
        print(f"Error: Directory not found: {corpus_dir}", file=sys.stderr)
        sys.exit(1)

    summary = analyze_corpus(corpus_dir, dictionary, limit=args.limit)

    print(f"\n{'=' * 60}")
    print("CORPUS OCR QUALITY ANALYSIS")
    print(f"{'=' * 60}")
    print(f"Total files: {summary['total_files']:,}")
    print(f"Mean score: {summary['mean_score']:.3f}")
    print()
    print("Quality distribution:")
    for tier in ["good", "moderate", "poor", "garbage"]:
        count = summary["tier_distribution"].get(tier, 0)
        pct = count / summary["total_files"] * 100 if summary["total_files"] > 0 else 0
        print(f"  {tier.upper():10s}: {count:6,} ({pct:5.1f}%)")
    print()
    print("Score percentiles:")
    for p, v in summary["score_percentiles"].items():
        print(f"  {p}: {v:.3f}")

    if args.report:
        with open(args.report, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"\nFull report saved to: {args.report}")

    # Show worst files
    print(f"\nWorst {min(10, len(summary['files']))} files:")
    for result in summary["files"][:10]:
        print(
            f"  {result['combined_score']:.3f} [{result['quality_tier']:8s}] {Path(result['file_path']).name}"
        )


def cmd_filter(args, dictionary: Dictionary):
    """Filter corpus by quality threshold."""
    corpus_dir = Path(args.corpus_dir)
    threshold = args.threshold

    output_good = Path(args.output_good) if args.output_good else None
    output_bad = Path(args.output_bad) if args.output_bad else None

    if output_good:
        output_good.mkdir(parents=True, exist_ok=True)
    if output_bad:
        output_bad.mkdir(parents=True, exist_ok=True)

    files = list(corpus_dir.rglob("*.txt"))
    total = len(files)
    good_count = 0
    bad_count = 0

    print(f"Filtering {total} files with threshold {threshold}...")

    for i, file_path in enumerate(files, 1):
        if i % 500 == 0:
            print(f"  Progress: {i}/{total}")

        result = score_file(file_path, dictionary)

        if result.combined_score < threshold:
            good_count += 1
            if output_good:
                dest = output_good / file_path.name
                dest.write_text(file_path.read_text(encoding="utf-8", errors="replace"))
        else:
            bad_count += 1
            if output_bad:
                dest = output_bad / file_path.name
                dest.write_text(file_path.read_text(encoding="utf-8", errors="replace"))

    print("\nResults:")
    print(f"  Good (score < {threshold}): {good_count:,} ({good_count / total * 100:.1f}%)")
    print(f"  Bad (score >= {threshold}): {bad_count:,} ({bad_count / total * 100:.1f}%)")

    if output_good:
        print(f"  Good files copied to: {output_good}")
    if output_bad:
        print(f"  Bad files copied to: {output_bad}")


def main():
    parser = argparse.ArgumentParser(
        description="OCR Error Rate Scorer - Triage files by OCR quality",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Quality Tiers:
  GOOD      score < 0.05   High quality, minimal cleanup needed
  MODERATE  score < 0.10   Some errors, basic cleanup recommended
  POOR      score < 0.20   Many errors, SymSpell cleanup recommended
  GARBAGE   score >= 0.20  Severe corruption, may need LLM or discard

Examples:
  tc-ocr-score check document.txt
  tc-ocr-score analyze ./corpus --report quality_report.json
  tc-ocr-score filter ./corpus --threshold 0.10 --output-bad ./needs_cleanup
        """,
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Check single file
    check_parser = subparsers.add_parser("check", help="Score a single file")
    check_parser.add_argument("file", type=str, help="File to check")

    # Analyze corpus
    analyze_parser = subparsers.add_parser("analyze", help="Analyze entire corpus")
    analyze_parser.add_argument("corpus_dir", type=str, help="Corpus directory")
    analyze_parser.add_argument("--report", type=str, help="Save JSON report to file")
    analyze_parser.add_argument("--limit", type=int, help="Limit number of files to analyze")

    # Filter corpus
    filter_parser = subparsers.add_parser("filter", help="Filter corpus by quality")
    filter_parser.add_argument("corpus_dir", type=str, help="Corpus directory")
    filter_parser.add_argument(
        "--threshold", type=float, default=0.10, help="Score threshold (default: 0.10)"
    )
    filter_parser.add_argument("--output-good", type=str, help="Directory for good files")
    filter_parser.add_argument("--output-bad", type=str, help="Directory for bad files")

    # Common options
    parser.add_argument("--vocab", type=str, help="Additional vocabulary file")

    args = parser.parse_args()

    # Initialize dictionary
    print("Loading dictionary...", file=sys.stderr)
    dictionary = Dictionary()
    print(f"Dictionary loaded: ~{len(dictionary):,} words", file=sys.stderr)

    if hasattr(args, "vocab") and args.vocab:
        dictionary.add_corpus_vocabulary(Path(args.vocab))
        print(f"Added vocabulary from {args.vocab}", file=sys.stderr)

    # Dispatch command
    if args.command == "check":
        cmd_check(args, dictionary)
    elif args.command == "analyze":
        cmd_analyze(args, dictionary)
    elif args.command == "filter":
        cmd_filter(args, dictionary)


if __name__ == "__main__":
    main()
