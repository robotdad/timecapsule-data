#!/usr/bin/env python3
"""
Corpus Vocabulary Extractor for OCR Cleanup

Extracts vocabulary candidates from a corpus for human/AI review before
running SymSpell correction. This prevents false positives on proper nouns,
historical terms, and corpus-specific vocabulary.

Workflow:
    1. Run tc-ocr-clean first (light cleanup: ligatures, basic patterns)
    2. Run tc-ocr-vocab to extract candidates
    3. Human/AI review the candidates
    4. Use approved vocab with tc-ocr-symspell --vocab

Usage:
    tc-ocr-vocab extract ./corpus -o vocab_candidates.txt
    tc-ocr-vocab extract ./corpus --min-freq 5 --context-chars 50
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path


def get_unique_path(path: Path) -> Path:
    """Return a unique path by adding numeric suffix if file exists.

    Examples:
        vocab_candidates.txt -> vocab_candidates_1.txt -> vocab_candidates_2.txt
    """
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    parent = path.parent

    counter = 1
    while True:
        new_path = parent / f"{stem}_{counter}{suffix}"
        if not new_path.exists():
            return new_path
        counter += 1


# =============================================================================
# Known Vocabulary Whitelist
# Words in this set are skipped during extraction (known good words)
# =============================================================================
KNOWN_VOCAB_FILE = Path(__file__).parent.parent / "data" / "known_vocab.txt"


def load_known_vocab(filepath: Path | None = None) -> set[str]:
    """
    Load known vocabulary whitelist from file.

    Words in this list are considered "known good" and will be skipped
    during vocab extraction to reduce noise.
    """
    if filepath is None:
        filepath = KNOWN_VOCAB_FILE

    if not filepath.exists():
        return set()

    words = set()
    try:
        with open(filepath, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith("#"):
                    continue
                # Add lowercase version
                words.add(line.lower())
    except Exception as e:
        print(f"Warning: Could not load known vocab from {filepath}: {e}", file=sys.stderr)
        return set()

    return words


# Load known vocab at module import time
KNOWN_VOCAB: set[str] = load_known_vocab()

# Dictionary lookup is now handled in Rust (rust_ocr_clean module)
# The Rust module provides multi-language dictionary support (en, de, fr, la)
# via the is_known_word() function. No Python dictionary needed.


# =============================================================================
# Patterns
# =============================================================================

WORD_PATTERN = re.compile(r"\b([a-zA-Z][a-zA-Z']*[a-zA-Z]|[a-zA-Z])\b")

# NOTE: Suspicious pattern checking is now done exclusively in Rust (rust_ocr_clean module).
# The Rust implementation handles patterns for: camelCase, triple repeats, consonant runs,
# confusable chars (requiring actual digits/pipes), and rn/m confusion.

# =============================================================================
# Pattern-based whitelist (skip these patterns during extraction)
# These are legitimate patterns that would otherwise be flagged as unknown
# =============================================================================
SKIP_PATTERNS = [
    # Roman numerals (valid sequences only: I, V, X, L, C, D, M)
    # Matches: III, VIII, XIII, XLVIII, MCMXCIX, etc.
    # Does NOT match corrupted ones like IIL, IIII, VX (invalid sequences)
    re.compile(r"^M{0,3}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$"),
    # Scottish/Irish surname prefixes (Mc/Mac + capital + lowercase)
    # Matches: McDonald, McLean, MacArthur, MacNeil, etc.
    re.compile(r"^M[ac][A-Z][a-z]+$"),
    re.compile(r"^Mac[A-Z][a-z]+$"),
    # Common -ville place name suffix (American towns)
    # Matches: Nashville, Louisville, Taylorsville, etc.
    re.compile(r"^[A-Z][a-z]+ville$"),
]


def matches_skip_pattern(word: str) -> bool:
    """Check if word matches any skip pattern (known legitimate patterns)."""
    for pattern in SKIP_PATTERNS:
        if pattern.match(word):
            return True
    return False


# Common words to skip (too common to be interesting)
# Global interrupt flag for signal handler access from nested functions
_interrupted = False


SKIP_WORDS = {
    "a",
    "an",
    "the",
    "and",
    "or",
    "but",
    "in",
    "on",
    "at",
    "to",
    "for",
    "of",
    "with",
    "by",
    "from",
    "as",
    "is",
    "was",
    "are",
    "were",
    "been",
    "be",
    "have",
    "has",
    "had",
    "do",
    "does",
    "did",
    "will",
    "would",
    "could",
    "should",
    "may",
    "might",
    "must",
    "shall",
    "can",
    "need",
    "this",
    "that",
    "these",
    "those",
    "it",
    "its",
    "he",
    "she",
    "they",
    "him",
    "her",
    "them",
    "his",
    "their",
    "my",
    "your",
    "our",
    "who",
    "which",
    "what",
    "where",
    "when",
    "why",
    "how",
    "all",
    "each",
    "every",
    "both",
    "few",
    "more",
    "most",
    "other",
    "some",
    "such",
    "no",
    "not",
    "only",
    "same",
    "so",
    "than",
    "too",
    "very",
    "just",
    "also",
    "now",
    "i",
    "you",
    "we",
    "me",
    "us",
}


@dataclass
class VocabCandidate:
    """A vocabulary candidate with metadata."""

    word: str
    frequency: int = 0
    contexts: list = field(default_factory=list)
    is_capitalized: bool = False
    is_unknown: bool = False
    is_suspicious: bool = False
    suspicious_reason: str = ""

    def add_context(self, context: str, max_contexts: int = 3):
        """Add a context snippet if we don't have too many."""
        if len(self.contexts) < max_contexts:
            # Clean up context
            context = " ".join(context.split())
            if context not in self.contexts:
                self.contexts.append(context)


def is_known_word(word: str) -> bool:
    """Check if word is in the dictionary (uses Rust multi-language dictionaries)."""
    import rust_ocr_clean  # type: ignore[import-not-found]

    return rust_ocr_clean.is_known_word(word)


def get_word_languages(word: str) -> list[str]:
    """Get list of languages that recognize this word."""
    import rust_ocr_clean  # type: ignore[import-not-found]

    return rust_ocr_clean.word_languages(word)


def extract_context(text: str, match_start: int, match_end: int, context_chars: int = 40) -> str:
    """Extract context around a match."""
    start = max(0, match_start - context_chars)
    end = min(len(text), match_end + context_chars)

    # Try to extend to word boundaries
    while start > 0 and text[start - 1].isalnum():
        start -= 1
    while end < len(text) and text[end].isalnum():
        end += 1

    context = text[start:end]

    # Add ellipsis if truncated
    if start > 0:
        context = "..." + context
    if end < len(text):
        context = context + "..."

    return context


def format_output(
    candidates: dict[str, VocabCandidate],
    min_freq: int,
    output_format: str,
    show_known: bool = False,
) -> str:
    """Format candidates for output."""
    global _interrupted

    # Filter candidates - check interrupt periodically for large datasets
    filtered = []
    check_interval = 100_000  # Check every 100k items
    for i, (key, c) in enumerate(candidates.items()):
        if i % check_interval == 0 and _interrupted:
            break
        if c.frequency < min_freq:
            continue
        # By default, only show unknown words (not in dictionary)
        if not show_known and not c.is_unknown:
            continue
        filtered.append(c)

    if _interrupted:
        return ""  # Early exit on interrupt

    # Sort: suspicious first, then by frequency descending
    filtered.sort(key=lambda c: (not c.is_suspicious, -c.frequency))

    if output_format == "json":
        return json.dumps(
            [
                {
                    "word": c.word,
                    "frequency": c.frequency,
                    "capitalized": c.is_capitalized,
                    "unknown": c.is_unknown,
                    "suspicious": c.is_suspicious,
                    "suspicious_reason": c.suspicious_reason,
                    "contexts": c.contexts,
                }
                for c in filtered
            ],
            indent=2,
        )

    # Text format for human review
    lines = []
    lines.append("# Vocabulary Candidates for Review")
    lines.append("#")
    lines.append("# Format: FREQ | FLAGS | WORD | SAMPLE CONTEXT")
    lines.append("# Flags: C=Capitalized, U=Unknown, ?=Suspicious (review carefully)")
    lines.append("#")
    lines.append("# Instructions:")
    lines.append("#   1. Review each candidate")
    lines.append("#   2. Delete lines that are OCR errors (don't protect them)")
    lines.append("#   3. Keep lines that are legitimate words/names")
    lines.append("#   4. Save as approved_vocab.txt (just the words, one per line)")
    lines.append("#")
    lines.append(f"# Total candidates: {len(filtered)}")
    lines.append("#" + "=" * 78)
    lines.append("")

    # Separate suspicious from normal
    suspicious = [c for c in filtered if c.is_suspicious]
    normal = [c for c in filtered if not c.is_suspicious]

    if suspicious:
        lines.append("# ⚠️  SUSPICIOUS - Review carefully (likely OCR errors)")
        lines.append(
            "# Category codes: M=mixed_case, R=repeated, G=garbage, C=confusable, X=modern, F=fragment"
        )
        lines.append("#" + "-" * 78)
        for c in suspicious:
            flags = ""
            flags += "C" if c.is_capitalized else " "
            flags += "U" if c.is_unknown else " "
            flags += "?"
            # Extract category code from suspicious_reason (e.g., "M:mixed_case" -> "M")
            cat = c.suspicious_reason.split(":")[0] if c.suspicious_reason else "-"
            context = c.contexts[0] if c.contexts else ""
            lines.append(f"{c.frequency:6d} | {flags} | {cat:2s} | {c.word:20s} | {context}")
        lines.append("")

    if normal:
        lines.append("# Capitalized words (likely proper nouns)")
        lines.append("#" + "-" * 78)
        capitalized = [c for c in normal if c.is_capitalized]
        for c in capitalized:
            flags = "C"
            flags += "U" if c.is_unknown else " "
            flags += " "
            context = c.contexts[0] if c.contexts else ""
            lines.append(f"{c.frequency:6d} | {flags} | {c.word:20s} | {context}")
        lines.append("")

        lines.append("# Other unknown words (may be historical terms, technical vocab)")
        lines.append("#" + "-" * 78)
        other = [c for c in normal if not c.is_capitalized]
        for c in other:
            flags = " "
            flags += "U" if c.is_unknown else " "
            flags += " "
            context = c.contexts[0] if c.contexts else ""
            lines.append(f"{c.frequency:6d} | {flags} | {c.word:20s} | {context}")

    return "\n".join(lines)


def cmd_extract(args):
    """Extract vocabulary candidates from corpus."""
    import fnmatch
    import os
    import signal
    import time

    global _interrupted
    input_dir = Path(args.input_dir)
    _interrupted = False

    def handle_interrupt(signum, frame):
        global _interrupted
        if _interrupted:
            print("\n\nForce quit.", file=sys.stderr)
            os._exit(1)  # Hard exit - sys.exit() doesn't work reliably in signal handlers
        _interrupted = True
        print("\n\nInterrupted! Processing collected data...", file=sys.stderr)

    old_handler = signal.signal(signal.SIGINT, handle_interrupt)

    try:
        # Rust module is required - no Python fallback
        import rust_ocr_clean  # type: ignore[import-not-found]

        rust_extract_batch_parallel = rust_ocr_clean.extract_vocab_batch_parallel

        # Initialize Rust dictionaries for multi-language word lookup (en, de, fr, la)
        # This is done once per run; dictionaries are loaded globally in the Rust module
        # Try multiple possible locations for the dictionaries
        possible_dict_dirs = [
            # Development: relative to source file
            Path(__file__).parent.parent.parent.parent / "rust-ocr-clean" / "dictionaries",
            # Development: relative to cwd
            Path.cwd() / "rust-ocr-clean" / "dictionaries",
            # Installed: next to the rust module
            Path(rust_ocr_clean.__file__).parent / "dictionaries"
            if hasattr(rust_ocr_clean, "__file__")
            else None,
        ]

        dict_dir = None
        for candidate in possible_dict_dirs:
            if candidate and candidate.exists():
                dict_dir = candidate
                break

        if dict_dir:
            rust_ocr_clean.init_dictionaries(str(dict_dir))
            if rust_ocr_clean.dictionaries_loaded():
                print(f"Dictionaries loaded from: {dict_dir}", file=sys.stderr)
            else:
                print(
                    "WARNING: Dictionary init called but dictionaries_loaded() is False!",
                    file=sys.stderr,
                )
        else:
            print("WARNING: Dictionary directory not found! Tried:", file=sys.stderr)
            for p in possible_dict_dirs:
                if p:
                    print(f"  - {p}", file=sys.stderr)
            print("Capitalized English words will NOT be filtered from output.", file=sys.stderr)

        # Load known vocab whitelist
        if hasattr(args, "no_whitelist") and args.no_whitelist:
            known_vocab: set[str] = set()
            print("Known vocabulary whitelist disabled", file=sys.stderr)
        elif hasattr(args, "known_vocab") and args.known_vocab:
            known_vocab = load_known_vocab(Path(args.known_vocab))
            print(f"Loaded {len(known_vocab):,} words from custom whitelist", file=sys.stderr)
        else:
            known_vocab = KNOWN_VOCAB
            if known_vocab:
                print(f"Using {len(known_vocab):,} words from built-in whitelist", file=sys.stderr)

        # Initialize Rust whitelist (for skipping during extraction)
        if known_vocab:
            rust_ocr_clean.init_whitelist(list(known_vocab))

        # Fast file discovery
        print(f"Scanning {input_dir} for {args.pattern} files...", end="", flush=True)

        def fast_find_files(directory: Path, pattern: str) -> list[Path]:
            results = []
            dirs_to_scan = [directory]
            scanned = 0
            while dirs_to_scan:
                current_dir = dirs_to_scan.pop()
                try:
                    with os.scandir(current_dir) as it:
                        for entry in it:
                            if entry.is_dir(follow_symlinks=False):
                                dirs_to_scan.append(Path(entry.path))
                            elif entry.is_file() and fnmatch.fnmatch(entry.name, pattern):
                                results.append(Path(entry.path))
                except PermissionError:
                    continue
                scanned += 1
                if scanned % 500 == 0:
                    print(".", end="", flush=True)
            return results

        files = fast_find_files(input_dir, args.pattern)
        total_files = len(files)
        print(f" found {total_files:,} files", file=sys.stderr)

        if total_files == 0:
            print(f"No files matching '{args.pattern}' found in {input_dir}", file=sys.stderr)
            sys.exit(1)

        # Determine thread count (default 24, can be overridden)
        num_threads = getattr(args, "threads", 24) or 24

        print(f"\n{'=' * 60}", file=sys.stderr)
        print(
            f"Vocabulary Extraction - Rust engine (parallel, {num_threads} threads)",
            file=sys.stderr,
        )
        print(f"{'=' * 60}", file=sys.stderr)
        print(f"  Files to process: {total_files:,}", file=sys.stderr)
        print(f"{'=' * 60}\n", file=sys.stderr)

        start_time = time.time()

        # Single parallel call processes all files
        all_paths = [str(f) for f in files]
        stats, batch_results = rust_extract_batch_parallel(
            all_paths, args.context_chars, num_threads
        )

        elapsed = time.time() - start_time

        # Convert results to VocabCandidate objects
        candidates: dict[str, VocabCandidate] = {}
        for word_lower, (
            word,
            count,
            is_cap,
            is_susp,
            reason,
            context,
        ) in batch_results.items():
            candidates[word_lower] = VocabCandidate(
                word=word,
                frequency=count,
                contexts=[context] if context else [],
                is_capitalized=is_cap,
                is_unknown=True,  # Default to unknown, filter by suspicious instead
                is_suspicious=is_susp,
                suspicious_reason=reason,
            )

        # Calculate throughput
        files_per_sec = stats.files_processed / elapsed if elapsed > 0 else 0
        mb_per_sec = (stats.total_bytes / 1024 / 1024) / elapsed if elapsed > 0 else 0

        print(f"\n{'=' * 60}", file=sys.stderr)
        print("COMPLETE", file=sys.stderr)
        print(f"{'=' * 60}", file=sys.stderr)
        print(f"  Time elapsed: {elapsed:.1f}s", file=sys.stderr)
        print(f"  Throughput: {files_per_sec:.1f} files/s, {mb_per_sec:.1f} MB/s", file=sys.stderr)
        print(f"  Total words processed: {stats.total_words:,}", file=sys.stderr)
        print(f"  Unique candidates: {stats.unique_candidates:,}", file=sys.stderr)

        above_threshold = sum(1 for c in candidates.values() if c.frequency >= args.min_freq)
        print(f"  Candidates >= {args.min_freq} occurrences: {above_threshold:,}", file=sys.stderr)
        print(f"{'=' * 60}", file=sys.stderr)

        # Post-process: dictionary check for suspicious words
        # NOTE: Rust now does dictionary checks during extraction, but we do a final pass
        # here to catch any edge cases with the is_known_word function
        if not _interrupted and rust_ocr_clean.dictionaries_loaded():
            suspicious_to_check = [
                c for c in candidates.values() if c.is_suspicious and c.frequency >= args.min_freq
            ]
            if suspicious_to_check:
                print(
                    f"\nDictionary check for {len(suspicious_to_check):,} suspicious candidates...",
                    file=sys.stderr,
                )
                cleared = 0
                lang_counts: dict[str, int] = {}
                for c in suspicious_to_check:
                    if is_known_word(c.word):
                        c.is_suspicious = False
                        c.is_unknown = False
                        cleared += 1
                        # Track which languages recognized this word
                        for lang in get_word_languages(c.word):
                            lang_counts[lang] = lang_counts.get(lang, 0) + 1
                print(f"  Cleared {cleared:,} as known dictionary words", file=sys.stderr)
                if lang_counts:
                    # Sort by count descending
                    sorted_langs = sorted(lang_counts.items(), key=lambda x: -x[1])
                    lang_summary = ", ".join(f"{lang}: {count}" for lang, count in sorted_langs)
                    print(f"  By language: {lang_summary}", file=sys.stderr)

        # Generate output (skip if interrupted)
        if not _interrupted:
            output = format_output(
                candidates,
                min_freq=args.min_freq,
                output_format=args.format,
                show_known=args.show_known,
            )

            # Default output to parent of input dir
            input_path = Path(args.input_dir)
            if args.output:
                output_path = Path(args.output)
            else:
                output_path = input_path.parent / "_vocab_candidates.txt"

            # Don't overwrite existing files - add numeric suffix
            output_path = get_unique_path(output_path)

            output_path.write_text(output, encoding="utf-8")
            print(f"\nOutput written to: {output_path}", file=sys.stderr)
        else:
            print("\nSkipped output generation due to interrupt.", file=sys.stderr)

    finally:
        signal.signal(signal.SIGINT, old_handler)


def cmd_simplify(args):
    """Simplify reviewed candidates to just a word list."""
    input_path = Path(args.input)
    content = input_path.read_text(encoding="utf-8")

    words = []
    for line in content.splitlines():
        line = line.strip()
        # Skip comments and empty lines
        if not line or line.startswith("#"):
            continue

        # Parse the format: FREQ | FLAGS | WORD | CONTEXT
        parts = line.split("|")
        if len(parts) >= 3:
            word = parts[2].strip()
            if word:
                words.append(word.lower())

    # Deduplicate and sort
    words = sorted(set(words))

    output = "\n".join(words)

    if args.output:
        Path(args.output).write_text(output, encoding="utf-8")
        print(f"Extracted {len(words)} words to: {args.output}", file=sys.stderr)
    else:
        print(output)


def main():
    parser = argparse.ArgumentParser(
        description="Corpus Vocabulary Extractor for OCR Cleanup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Workflow:
  1. Run tc-ocr-clean on corpus (light cleanup first)
  2. Run tc-ocr-vocab extract to get candidates
  3. Review and edit the candidates file (remove OCR errors)
  4. Run tc-ocr-vocab simplify to get word list
  5. Use word list with tc-ocr-symspell --vocab

Examples:
  tc-ocr-vocab extract ./corpus -o candidates.txt --min-freq 5
  tc-ocr-vocab simplify candidates_reviewed.txt -o approved_vocab.txt
        """,
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Extract command
    extract_parser = subparsers.add_parser("extract", help="Extract vocabulary candidates")
    extract_parser.add_argument("input_dir", help="Input directory to scan")
    extract_parser.add_argument(
        "-o",
        "--output",
        help="Output file (default: {input_parent}/_vocab_candidates.txt)",
    )
    extract_parser.add_argument(
        "--min-freq",
        type=int,
        default=5,
        help="Minimum frequency to include (default: 5)",
    )
    extract_parser.add_argument(
        "--pattern",
        default="*.txt",
        help="File pattern to match (default: *.txt)",
    )
    extract_parser.add_argument(
        "--context-chars",
        type=int,
        default=40,
        help="Context characters around word (default: 40)",
    )
    extract_parser.add_argument(
        "--max-contexts",
        type=int,
        default=3,
        help="Max context examples per word (default: 3)",
    )
    extract_parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    extract_parser.add_argument(
        "--show-known",
        action="store_true",
        help="Include known dictionary words",
    )
    extract_parser.add_argument(
        "--known-vocab",
        type=str,
        help="Custom known vocabulary whitelist file (words to skip)",
    )
    extract_parser.add_argument(
        "--no-whitelist",
        action="store_true",
        help="Disable the built-in known vocabulary whitelist",
    )

    # Simplify command
    simplify_parser = subparsers.add_parser(
        "simplify",
        help="Convert reviewed candidates to word list",
    )
    simplify_parser.add_argument("input", help="Reviewed candidates file")
    simplify_parser.add_argument("-o", "--output", help="Output word list file")

    args = parser.parse_args()

    if args.command == "extract":
        cmd_extract(args)
    elif args.command == "simplify":
        cmd_simplify(args)


if __name__ == "__main__":
    main()
