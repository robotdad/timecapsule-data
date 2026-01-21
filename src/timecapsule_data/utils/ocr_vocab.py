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

# Try to import dictionary for unknown word detection (optional)
try:
    import enchant  # type: ignore[import-not-found]

    DICT = enchant.Dict("en_US")
    HAS_ENCHANT = True
except ImportError:
    HAS_ENCHANT = False
    DICT = None


# =============================================================================
# Patterns
# =============================================================================

WORD_PATTERN = re.compile(r"\b([a-zA-Z][a-zA-Z']*[a-zA-Z]|[a-zA-Z])\b")

# Patterns that suggest OCR errors (suspicious words)
SUSPICIOUS_PATTERNS = [
    re.compile(r"[a-z][A-Z]"),  # camelCase in middle (OCR mixing)
    re.compile(r"(.)\1{2,}"),  # Triple+ repeated chars
    re.compile(r"[^aeiouAEIOU]{5,}"),  # 5+ consonants in a row
    re.compile(r"^[bcdfghjklmnpqrstvwxz]{4,}$", re.I),  # All consonants, 4+ chars
    re.compile(r"[il1|]{3,}"),  # Multiple confusable chars (l/1/|/i)
    re.compile(r"[rnm]{4,}"),  # Multiple similar chars (rn looks like m)
]

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


def is_suspicious(word: str) -> tuple[bool, str]:
    """Check if a word looks like an OCR error."""
    for pattern in SUSPICIOUS_PATTERNS:
        if pattern.search(word):
            return True, pattern.pattern
    return False, ""


def is_known_word(word: str) -> bool:
    """Check if word is in the dictionary."""
    if not HAS_ENCHANT or DICT is None:
        return False
    try:
        return DICT.check(word) or DICT.check(word.lower())
    except Exception:
        return False


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


def process_file(
    file_path: Path,
    candidates: dict[str, VocabCandidate],
    context_chars: int = 40,
    max_contexts: int = 3,
    known_vocab: set[str] | None = None,
) -> int:
    """Process a single file and update candidates dict."""
    if known_vocab is None:
        known_vocab = KNOWN_VOCAB

    try:
        text = file_path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        print(f"  Error reading {file_path}: {e}", file=sys.stderr)
        return 0

    word_count = 0
    for match in WORD_PATTERN.finditer(text):
        word = match.group(1)
        word_lower = word.lower()

        # Skip common words
        if word_lower in SKIP_WORDS:
            continue

        # Skip known vocabulary (British spellings, Latin terms, etc.)
        if word_lower in known_vocab:
            continue

        # Skip very short words
        if len(word) < 2:
            continue

        word_count += 1

        # Get or create candidate
        key = word_lower
        if key not in candidates:
            candidate = VocabCandidate(word=word)
            candidate.is_capitalized = word[0].isupper()
            # Skip dictionary lookup - too slow, filter by suspicious instead
            candidate.is_unknown = True
            suspicious, reason = is_suspicious(word)
            candidate.is_suspicious = suspicious
            candidate.suspicious_reason = reason
            candidates[key] = candidate
        else:
            candidate = candidates[key]
            # Update capitalization if we see a capitalized version
            if word[0].isupper():
                candidate.is_capitalized = True
                # Prefer capitalized form for display
                if not candidate.word[0].isupper():
                    candidate.word = word

        candidate.frequency += 1
        candidate.add_context(
            extract_context(text, match.start(), match.end(), context_chars),
            max_contexts,
        )

    return word_count


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
        lines.append("#" + "-" * 78)
        for c in suspicious:
            flags = ""
            flags += "C" if c.is_capitalized else " "
            flags += "U" if c.is_unknown else " "
            flags += "?"
            context = c.contexts[0] if c.contexts else ""
            lines.append(f"{c.frequency:6d} | {flags} | {c.word:20s} | {context}")
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
        # Try to use Rust for speed
        rust_available = False
        rust_extract_batch = None
        try:
            import rust_ocr_clean  # type: ignore[import-not-found]

            rust_extract_batch = rust_ocr_clean.extract_vocab_batch
            rust_available = True
        except ImportError:
            print("Note: Rust module not available, using Python (slower)", file=sys.stderr)

        if not HAS_ENCHANT:
            print(
                "Warning: pyenchant not available, all words will be marked as unknown",
                file=sys.stderr,
            )

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

        print(f"\n{'=' * 60}", file=sys.stderr)
        print(
            f"Vocabulary Extraction - {'Rust' if rust_available else 'Python'} engine",
            file=sys.stderr,
        )
        print(f"{'=' * 60}", file=sys.stderr)
        print(f"  Files to process: {total_files:,}", file=sys.stderr)
        print(f"{'=' * 60}\n", file=sys.stderr)

        candidates: dict[str, VocabCandidate] = {}
        total_words = 0
        start_time = time.time()

        if rust_available and rust_extract_batch is not None:
            # Process in batches with Rust
            batch_size = 500
            for batch_start in range(0, total_files, batch_size):
                if _interrupted:
                    break

                batch_end = min(batch_start + batch_size, total_files)
                batch_files = [str(f) for f in files[batch_start:batch_end]]

                batch_words, batch_results = rust_extract_batch(batch_files, args.context_chars)
                total_words += batch_words

                # Merge batch results into candidates
                for word_lower, (
                    word,
                    count,
                    is_cap,
                    is_susp,
                    reason,
                    context,
                ) in batch_results.items():
                    if word_lower in candidates:
                        c = candidates[word_lower]
                        c.frequency += count
                        if is_cap:
                            c.is_capitalized = True
                            if not c.word[0].isupper():
                                c.word = word
                    else:
                        # Skip dictionary lookup in hot path - too slow
                        # is_unknown will be set in post-processing if needed
                        candidates[word_lower] = VocabCandidate(
                            word=word,
                            frequency=count,
                            contexts=[context] if context else [],
                            is_capitalized=is_cap,
                            is_unknown=True,  # Default to unknown, filter by suspicious instead
                            is_suspicious=is_susp,
                            suspicious_reason=reason,
                        )

                # Progress update
                elapsed = time.time() - start_time
                files_done = batch_end
                files_per_sec = files_done / elapsed if elapsed > 0 else 0
                pct = (files_done / total_files) * 100
                remaining = (total_files - files_done) / files_per_sec if files_per_sec > 0 else 0

                if remaining >= 3600:
                    eta = f"{remaining / 3600:.1f}h"
                elif remaining >= 60:
                    eta = f"{remaining / 60:.1f}m"
                else:
                    eta = f"{remaining:.0f}s"

                # Count candidates meeting frequency threshold
                above_threshold = sum(
                    1 for c in candidates.values() if c.frequency >= args.min_freq
                )

                print(
                    f"  [{pct:5.1f}%] {files_done:,}/{total_files:,} files | "
                    f"{files_per_sec:.1f} files/s | ETA: {eta} | "
                    f"vocab: {above_threshold:,}",
                    file=sys.stderr,
                )
        else:
            # Fall back to Python
            for i, file_path in enumerate(files, 1):
                if _interrupted:
                    break

                if i % 500 == 0 or i == total_files:
                    elapsed = time.time() - start_time
                    files_per_sec = i / elapsed if elapsed > 0 else 0
                    pct = (i / total_files) * 100
                    print(
                        f"  [{pct:5.1f}%] {i:,}/{total_files:,} files | "
                        f"{files_per_sec:.1f} files/s | unique: {len(candidates):,}",
                        file=sys.stderr,
                    )

                total_words += process_file(
                    file_path,
                    candidates,
                    context_chars=args.context_chars,
                    max_contexts=args.max_contexts,
                )

        elapsed = time.time() - start_time
        print(f"\n{'=' * 60}", file=sys.stderr)
        if _interrupted:
            print("INTERRUPTED - showing partial results", file=sys.stderr)
        else:
            print("COMPLETE", file=sys.stderr)
        print(f"{'=' * 60}", file=sys.stderr)
        print(f"  Time elapsed: {elapsed:.1f}s", file=sys.stderr)
        print(f"  Total words processed: {total_words:,}", file=sys.stderr)
        print(f"  Unique candidates: {len(candidates):,}", file=sys.stderr)

        # Filter by frequency (skip if interrupted - this is slow with 200M+ candidates)
        if not _interrupted:
            above_threshold = sum(1 for c in candidates.values() if c.frequency >= args.min_freq)
            print(
                f"  Candidates >= {args.min_freq} occurrences: {above_threshold:,}", file=sys.stderr
            )
        print(f"{'=' * 60}", file=sys.stderr)

        # Generate output (skip if interrupted)
        if not _interrupted:
            output = format_output(
                candidates,
                min_freq=args.min_freq,
                output_format=args.format,
                show_known=args.show_known,
            )

            if args.output:
                Path(args.output).write_text(output, encoding="utf-8")
                print(f"\nOutput written to: {args.output}", file=sys.stderr)
            else:
                print(output)
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
    extract_parser.add_argument("-o", "--output", help="Output file (default: stdout)")
    extract_parser.add_argument(
        "--min-freq",
        type=int,
        default=3,
        help="Minimum frequency to include (default: 3)",
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
