#!/usr/bin/env python3
"""
OCR Cleanup Module

Repairs common OCR errors in historical texts. This goes beyond filtering -
it actually attempts to fix recognizable error patterns.

Pipeline order:
1. Language detection (skip non-English documents)
2. Whitespace normalization (strip trailing, collapse multiples)
3. Hyphen rejoining (fix line-break hyphenation)
4. Mid-word uppercase normalization (sVo -> svo)
5. OCR substitutions (pattern-based fixes)

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
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import rust_ocr_clean


def get_unique_path(path: Path) -> Path:
    """Return a unique path by adding numeric suffix if file exists.

    Examples:
        _cleanup_report.json -> _cleanup_report_1.json -> _cleanup_report_2.json
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
# Preprocessing Functions (applied before OCR substitutions)
# Uses Rust implementations for performance at scale (2M+ docs)
# =============================================================================


def detect_language(text: str, confidence_threshold: float = 0.5) -> tuple[bool, float]:
    """
    Detect if text is primarily English using Rust whatlang.

    Args:
        text: The text to analyze
        confidence_threshold: Minimum confidence to accept detection (default 0.5)

    Returns:
        (is_english, confidence) - is_english is True if detected as English with sufficient confidence
    """
    result = rust_ocr_clean.detect_language(text, confidence_threshold)
    return result.is_english, result.confidence


def fix_unicode(text: str) -> str:
    """
    Fix Unicode issues using Rust implementation.

    Fixes:
    - Mojibake (encoding errors like "Ã©" → "é")
    - Broken HTML entities
    - Unicode whitespace normalization
    - NFC normalization

    Should run BEFORE pattern matching.
    """
    return rust_ocr_clean.fix_unicode(text)


def normalize_whitespace(text: str) -> tuple[str, int]:
    """
    Normalize whitespace in text. Run BEFORE hyphen rejoining.

    - Strip trailing whitespace from lines (important for hyphen detection)
    - Collapse multiple spaces to single space
    - Normalize line endings to \n
    - Remove spaces around hyphens at line ends

    Returns:
        (normalized_text, count_of_changes)
    """
    changes = 0

    # Normalize line endings first
    if "\r\n" in text:
        text = text.replace("\r\n", "\n")
        changes += 1
    if "\r" in text:
        text = text.replace("\r", "\n")
        changes += 1

    # Strip trailing whitespace from each line (critical for hyphen detection)
    lines = text.split("\n")
    stripped_lines = []
    for line in lines:
        stripped = line.rstrip()
        if stripped != line:
            changes += 1
        stripped_lines.append(stripped)
    text = "\n".join(stripped_lines)

    # Collapse multiple spaces to single (but not at line start - preserve indentation)
    original = text
    text = re.sub(r"([^ \n]) {2,}", r"\1 ", text)
    if text != original:
        changes += text.count("  ")  # Rough count

    return text, changes


def rejoin_hyphenated(text: str) -> tuple[str, int]:
    """
    Rejoin words split by end-of-line hyphenation.

    Pattern: word-fragment + hyphen + newline + lowercase continuation
    Example: "de-\npendance" -> "dependance"

    Must run AFTER normalize_whitespace (to handle "word- \n" patterns).

    Returns:
        (rejoined_text, count_of_rejoins)
    """
    # Pattern: letters, hyphen, newline, optional whitespace, lowercase letters
    # Only rejoin if continuation starts lowercase (indicates word continuation)
    pattern = r"([a-zA-Z]{2,})-\n\s*([a-z]{2,})\b"

    count = len(re.findall(pattern, text))
    if count > 0:
        text = re.sub(pattern, r"\1\2", text)

    return text, count


def normalize_midword_caps(text: str) -> tuple[str, int]:
    """
    Fix OCR errors where a letter is incorrectly uppercase mid-word.

    Pattern: lowercase-UPPERCASE-lowercase in middle of word
    Examples: sVo -> svo, tRe -> tre, lVs -> lvs

    These are never intentional in normal text.

    Returns:
        (normalized_text, count_of_fixes)
    """
    # Pattern: lowercase letter followed by uppercase followed by lowercase
    # This catches mid-word caps that are clearly OCR errors
    pattern = r"(?<=[a-z])([A-Z])(?=[a-z])"

    count = len(re.findall(pattern, text))
    if count > 0:
        text = re.sub(pattern, lambda m: m.group(1).lower(), text)

    return text, count


# =============================================================================
# Long-s (ſ) Detection and Fixing
# =============================================================================
# In pre-1800 texts, the "long s" (ſ) was commonly used and OCR misreads it as 'f'.
# Instead of enumerating all variants, we:
# 1. Detect documents with pervasive long-s using marker words
# 2. Apply broad pattern-based fixes only to those documents

# Marker words where ſ→f is unmistakable (these patterns don't occur in normal English)
# Long-s patterns are now consolidated in Rust (rust-ocr-clean/src/lib.rs)
# The Rust module handles all long-s detection and fixing via clean_text()

# Common OCR substitution errors
# Format: (error_pattern, correction, context_required)
# context_required: None = always apply, or regex that must match around the word
# Patterns that indicate garbage OCR (not fixable, flag for review)
GARBAGE_PATTERNS = [
    r"[^\x00-\x7F]{10,}",  # Long runs of non-ASCII
    r"[bcdfghjklmnpqrstvwxz]{6,}",  # Long consonant runs
    r"\d{2,}[a-z]+\d{2,}",  # Numbers mixed into words oddly
    r"[|l1I]{5,}",  # Pipe/l/1/I confusion runs
]


# Threshold for flagging high-substitution documents (substitutions per 1000 chars)
HIGH_SUBSTITUTION_THRESHOLD = 10.0


@dataclass
class CleanupStats:
    """Track cleanup statistics."""

    total_files: int = 0
    files_modified: int = 0
    files_flagged: int = 0
    files_skipped_language: int = 0  # Non-English documents skipped
    total_substitutions: int = 0
    whitespace_fixes: int = 0
    hyphen_rejoins: int = 0
    midword_caps_fixes: int = 0
    long_s_fixes: int = 0  # Track long-s fixes separately
    substitution_counts: Counter = field(default_factory=Counter)
    flagged_files: list = field(default_factory=list)
    skipped_files: list = field(default_factory=list)  # Non-English files
    # Per-document tracking (only interesting docs, not all 1M+)
    high_substitution_docs: list = field(default_factory=list)  # Docs above threshold
    long_s_documents: list = field(default_factory=list)  # Docs with long-s patterns
    long_s_document_count: int = 0  # Total count (list is capped)
    high_sub_document_count: int = 0  # Total count (list is capped)
    # Triage stats
    triage_passed: int = 0
    triage_quarantined: int = 0
    triage_rejected: int = 0
    triage_results: list = field(default_factory=list)  # Full triage results for JSONL export
    elapsed_seconds: float = 0.0  # Total processing time
    # Boilerplate stripping stats
    files_with_boilerplate: int = 0
    total_boilerplate_chars: int = 0
    boilerplate_by_category: Counter = field(default_factory=Counter)  # category -> count

    def track_document(
        self,
        filename: str,
        char_count: int,
        total_subs: int,
        long_s_fixes: int,
        whitespace_fixes: int,
        hyphen_fixes: int,
        midword_caps_fixes: int,
        has_long_s: bool,
    ):
        """Track per-document stats - only stores interesting documents to avoid memory bloat."""
        # Calculate substitution rate
        sub_rate = (total_subs / char_count) * 1000 if char_count > 0 else 0

        # Only store high-substitution docs (cap at 1000 to avoid memory issues)
        if sub_rate >= HIGH_SUBSTITUTION_THRESHOLD:
            self.high_sub_document_count += 1
            if len(self.high_substitution_docs) < 1000:
                ocr_pattern_fixes = (
                    total_subs - long_s_fixes - whitespace_fixes - hyphen_fixes - midword_caps_fixes
                )
                self.high_substitution_docs.append(
                    {
                        "filename": filename,
                        "char_count": char_count,
                        "total_substitutions": total_subs,
                        "substitution_rate": round(sub_rate, 2),
                        "categories": {
                            "long_s": long_s_fixes,
                            "whitespace": whitespace_fixes,
                            "hyphens": hyphen_fixes,
                            "midword_caps": midword_caps_fixes,
                            "ocr_patterns": ocr_pattern_fixes,
                        },
                    }
                )

        # Only store long-s docs (cap at 1000)
        if has_long_s:
            self.long_s_document_count += 1
            if len(self.long_s_documents) < 1000:
                self.long_s_documents.append(
                    {
                        "filename": filename,
                        "long_s_fixes": long_s_fixes,
                    }
                )

    def to_dict(self):
        return {
            "total_files": self.total_files,
            "files_modified": self.files_modified,
            "files_flagged": self.files_flagged,
            "files_skipped_language": self.files_skipped_language,
            "total_substitutions": self.total_substitutions,
            "substitution_breakdown": {
                "whitespace": self.whitespace_fixes,
                "hyphens": self.hyphen_rejoins,
                "midword_caps": self.midword_caps_fixes,
                "long_s": self.long_s_fixes,
                "ocr_patterns": self.total_substitutions
                - self.whitespace_fixes
                - self.hyphen_rejoins
                - self.midword_caps_fixes
                - self.long_s_fixes,
            },
            "top_substitutions": self.substitution_counts.most_common(50),
            "flagged_files": self.flagged_files[:100],
            "skipped_files": self.skipped_files[:100],
            # Per-document analysis (only interesting docs stored, not all 1M+)
            "long_s_documents": {
                "total_count": self.long_s_document_count,
                "sample_files": self.long_s_documents[:100],  # First 100 of up to 1000 stored
            },
            "high_substitution_documents": {
                "total_count": self.high_sub_document_count,
                "threshold_per_1000_chars": HIGH_SUBSTITUTION_THRESHOLD,
                "sample_files": sorted(
                    self.high_substitution_docs, key=lambda x: x["substitution_rate"], reverse=True
                )[:100],
            },
            # Triage stats
            "triage_passed": self.triage_passed,
            "triage_quarantined": self.triage_quarantined,
            "triage_rejected": self.triage_rejected,
            "triage_skipped_files": [r for r in self.triage_results if r["action"] != "pass"][
                :500
            ],  # Cap at 500 to avoid huge reports
            # Boilerplate stripping stats
            "boilerplate": {
                "files_with_boilerplate": self.files_with_boilerplate,
                "total_chars_stripped": self.total_boilerplate_chars,
                "by_category": dict(self.boilerplate_by_category),
            },
        }


def check_garbage(text: str) -> list[tuple[str, int]]:
    """Check for unfixable garbage patterns. Returns list of (pattern, count)."""
    issues = []
    for pattern in GARBAGE_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if len(matches) > 5:
            issues.append((pattern, len(matches)))
    return issues


def clean_file(
    input_path: Path,
    output_path: Optional[Path] = None,
    stats: Optional[CleanupStats] = None,
    skip_language_check: bool = False,
) -> tuple[bool, int, list, bool]:
    """
    Clean a single file.

    Args:
        input_path: Path to input file
        output_path: Path to output file (None = don't write)
        stats: CleanupStats object to update
        skip_language_check: If True, skip language detection

    Returns: (was_modified, substitution_count, garbage_issues, was_skipped)
        was_skipped is True if file was skipped due to non-English content
    """
    try:
        with open(input_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except Exception as e:
        print(f"  Error reading {input_path}: {e}")
        return False, 0, [], False

    # Language detection - skip non-English documents
    if not skip_language_check:
        is_english, confidence = detect_language(content)
        if not is_english:
            if stats:
                stats.files_skipped_language += 1
                stats.skipped_files.append(
                    {
                        "file": str(input_path),
                        "reason": "non-english",
                        "confidence": confidence,
                    }
                )
            # Don't process, don't copy to output
            return False, 0, [], True

    garbage_issues = check_garbage(content)

    # Use Rust for all OCR cleanup (no Python fallback)
    result = rust_ocr_clean.clean_text_with_categories(content)
    cleaned = result.text
    sub_count = result.total_substitutions
    categories = result.substitutions_by_category
    was_modified = sub_count > 0

    # Update stats from Rust results
    if stats:
        stats.total_substitutions += sub_count
        stats.long_s_fixes += categories.get("long_s", 0)

    # Track per-document stats (only stores interesting docs - high sub rate)
    if stats and sub_count > 0:
        stats.track_document(
            filename=input_path.name,
            char_count=len(content),
            total_subs=sub_count,
            long_s_fixes=categories.get("long_s", 0),
            whitespace_fixes=0,  # Preprocessing not done in single-file mode
            hyphen_fixes=0,
            midword_caps_fixes=0,
            has_long_s=categories.get("long_s", 0) > 0,
        )

    if output_path and was_modified:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(cleaned)
    elif output_path and not was_modified:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)

    return was_modified, sub_count, garbage_issues, False


def clean_batch(
    input_dir: Path,
    output_dir: Optional[Path] = None,
    file_pattern: str = "*.txt",
    use_rust: bool = True,
    skip_triage: bool = False,
    triage_output: Optional[Path] = None,
    boilerplate_log: Optional[Path] = None,
    parallel: bool = True,
    num_threads: int = 24,
) -> CleanupStats:
    """
    Clean all text files in a directory.

    Uses Rust for file I/O when available (much faster).
    With parallel=True (default), uses Rayon for multi-threaded processing.

    Args:
        input_dir: Directory containing input files
        output_dir: Directory for output files (None = in-place)
        file_pattern: Glob pattern for files to process
        use_rust: Use Rust engine for speed
        skip_triage: If True, skip document triage and process all files
        triage_output: If set, write triage results to this JSONL file
        boilerplate_log: If set, write boilerplate audit log to this JSONL file
        parallel: If True, use multi-threaded Rayon processing (default: True)
        num_threads: Number of threads for parallel processing (default: 24)
    """
    import signal
    import sys
    import time

    stats = CleanupStats()
    interrupted = False

    def handle_interrupt(signum, frame):
        nonlocal interrupted
        if interrupted:
            # Second Ctrl+C - force exit
            print("\n\nForce quit.", file=sys.stderr)
            sys.exit(1)
        interrupted = True
        print("\n\nInterrupted! Finishing current file, then stopping...", file=sys.stderr)

    # Set up clean interrupt handling
    old_handler = signal.signal(signal.SIGINT, handle_interrupt)

    try:
        # Import Rust module (required - no Python fallback)
        import rust_ocr_clean  # type: ignore[import-not-found]

        rust_clean_file = rust_ocr_clean.clean_file_to_file

        # Discover files using os.scandir (faster than glob)
        print(f"Scanning {input_dir} for {file_pattern} files...", end="", flush=True)

        import fnmatch
        import os

        def fast_find_files(directory: Path, pattern: str) -> list[Path]:
            """Fast recursive file discovery using os.scandir."""
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

        input_files = fast_find_files(input_dir, file_pattern)
        stats.total_files = len(input_files)
        print(f" found {stats.total_files:,} files")

        if stats.total_files == 0:
            print("No files found.")
            return stats

        # Document triage - filter out problematic content before OCR cleanup
        files_to_process = input_files
        if not skip_triage:
            print(f"Running document triage (parallel, {num_threads} threads)...")
            try:
                import rust_ocr_clean  # type: ignore[import-not-found]

                total_files = len(input_files)
                paths = [str(f) for f in input_files]
                triage_start = time.time()

                # Single parallel call does structural triage + language detection
                triage_results, triage_stats = rust_ocr_clean.triage_batch_parallel(
                    paths,
                    num_threads,
                    0.5,  # lang confidence threshold
                )

                triage_elapsed = time.time() - triage_start
                triage_rate = total_files / triage_elapsed if triage_elapsed > 0 else 0

                # Process results
                pass_files = []
                language_counts: dict[str, int] = {}

                for r in triage_results:
                    triage_record = {
                        "path": r.path,
                        "action": r.action,
                        "problems": list(r.problems),
                        "signals": {
                            "alpha_ratio": round(r.alpha_ratio, 4),
                            "line_length_cv": round(r.line_length_cv, 4),
                            "mean_words_per_line": round(r.mean_words_per_line, 2),
                            "fragment_ratio": round(r.fragment_ratio, 4),
                        },
                    }

                    # Add language info if detected
                    if r.detected_lang:
                        triage_record["language"] = {
                            "detected": r.detected_lang,
                            "confidence": round(r.lang_confidence, 4),
                            "is_english": r.is_english,
                        }
                        if not r.is_english:
                            language_counts[r.detected_lang] = (
                                language_counts.get(r.detected_lang, 0) + 1
                            )

                    stats.triage_results.append(triage_record)

                    if r.action == "pass":
                        pass_files.append(Path(r.path))

                # Update stats from Rust
                stats.triage_passed = triage_stats.passed
                stats.triage_quarantined = triage_stats.quarantined
                stats.triage_rejected = triage_stats.rejected

                files_to_process = pass_files

                # Print summary
                print(
                    f"  Triage complete: {total_files:,} files in {triage_elapsed:.1f}s "
                    f"({triage_rate:.0f} files/s)"
                )
                print(
                    f"  Results: pass={triage_stats.passed:,}, "
                    f"quarantine={triage_stats.quarantined:,}, "
                    f"reject={triage_stats.rejected:,}"
                )

                # Show language stats
                if triage_stats.non_english > 0:
                    print(f"\n  Non-English detected: {triage_stats.non_english:,} files")
                    sorted_langs = sorted(language_counts.items(), key=lambda x: -x[1])[:10]
                    for lang, count in sorted_langs:
                        print(f"    {lang}: {count:,}")
                    if len(language_counts) > 10:
                        print(f"    ... and {len(language_counts) - 10} more languages")

                # Write triage results to JSONL if requested
                if triage_output:
                    with open(triage_output, "w") as f:
                        for record in stats.triage_results:
                            f.write(json.dumps(record) + "\n")
                    print(f"  Triage results written to: {triage_output}")

            except ImportError:
                print("  Skipped (Rust module not available)")

        # Skip upfront size calculation - we'll track as we go
        print("(Size will be calculated during processing)")

        print(f"\n{'=' * 60}")
        print(f"OCR Cleanup - Rust engine {'(parallel)' if parallel else '(sequential)'}")
        print(f"{'=' * 60}")
        print(f"  Files to process: {len(files_to_process):,}")
        if not skip_triage:
            print(f"  (Skipped by triage: {stats.triage_quarantined + stats.triage_rejected:,})")
        print(f"  Output: {output_dir or 'in-place'}")
        if parallel:
            print(f"  Threads: {num_threads}")
        print(f"{'=' * 60}\n")

        start_time = time.time()
        bytes_processed = 0
        last_update = start_time
        i = 0  # Track progress even if loop is empty or interrupted
        boilerplate_log_file = None

        # Open boilerplate audit log if path provided
        if boilerplate_log:
            boilerplate_log_file = open(boilerplate_log, "w")

        # Build file pairs list (needed for both parallel and sequential)
        file_pairs = []
        for input_path in files_to_process:
            if output_dir:
                relative = input_path.relative_to(input_dir)
                output_path = output_dir / relative
            else:
                output_path = input_path  # in-place
            file_pairs.append((str(input_path), str(output_path)))

        if parallel and not interrupted:
            # ===== PARALLEL PROCESSING WITH RAYON =====
            total_to_process = len(file_pairs)
            BATCH_SIZE = 1000  # Process in batches for progress reporting

            for batch_start in range(0, total_to_process, BATCH_SIZE):
                if interrupted:
                    break

                batch_end = min(batch_start + BATCH_SIZE, total_to_process)
                batch = file_pairs[batch_start:batch_end]

                # Process batch in parallel using Rust/Rayon
                batch_stats = rust_ocr_clean.clean_batch_parallel(batch, num_threads)

                # Aggregate stats
                i = batch_end
                stats.files_modified += batch_stats.files_modified
                stats.total_substitutions += batch_stats.total_substitutions
                stats.long_s_fixes += batch_stats.long_s_fixes
                stats.files_with_boilerplate += batch_stats.boilerplate_files
                stats.total_boilerplate_chars += batch_stats.boilerplate_chars
                bytes_processed += batch_stats.total_bytes

                # Progress update
                now = time.time()
                elapsed = now - start_time
                files_per_sec = i / elapsed if elapsed > 0 else 0
                mb_per_sec = (bytes_processed / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                remaining = (total_to_process - i) / files_per_sec if files_per_sec > 0 else 0

                # Format remaining time
                if remaining >= 3600:
                    eta = f"{remaining / 3600:.1f}h"
                elif remaining >= 60:
                    eta = f"{remaining / 60:.1f}m"
                else:
                    eta = f"{remaining:.0f}s"

                pct = (i / total_to_process) * 100 if total_to_process > 0 else 100
                print(
                    f"  [{pct:5.1f}%] {i:,}/{total_to_process:,} files | "
                    f"{files_per_sec:.1f} files/s | {mb_per_sec:.1f} MB/s | "
                    f"ETA: {eta} | subs: {stats.total_substitutions:,}"
                )

        else:
            # ===== SEQUENTIAL PROCESSING (for Ctrl+C support or when parallel=False) =====
            for i, (input_path_str, output_path_str) in enumerate(file_pairs, 1):
                if interrupted:
                    break

                input_path = Path(input_path_str)

                try:
                    # Use Rust for all file I/O (pipeline: strip boilerplate -> OCR cleanup)
                    was_modified, sub_count, file_bytes, categories, boilerplate_regions = (
                        rust_clean_file(input_path_str, output_path_str)
                    )
                    bytes_processed += file_bytes
                    # Aggregate category counts from Rust
                    stats.long_s_fixes += categories.get("long_s", 0)

                    if was_modified:
                        stats.files_modified += 1
                        stats.total_substitutions += sub_count

                    # Track boilerplate stripping
                    if boilerplate_regions:
                        stats.files_with_boilerplate += 1
                        for cat, pattern, start_line, end_line, char_count in boilerplate_regions:
                            stats.total_boilerplate_chars += char_count
                            stats.boilerplate_by_category[cat] += 1

                        # Write to boilerplate audit log if enabled
                        if boilerplate_log_file:
                            relative = input_path.relative_to(input_dir)
                            log_entry = {
                                "file": str(relative),
                                "stripped": [
                                    {
                                        "category": cat,
                                        "pattern": pattern,
                                        "lines": [start_line, end_line],
                                        "chars": char_count,
                                    }
                                    for cat, pattern, start_line, end_line, char_count in boilerplate_regions
                                ],
                            }
                            boilerplate_log_file.write(json.dumps(log_entry) + "\n")

                except Exception as e:
                    print(f"\n  Error processing {input_path}: {e}", file=sys.stderr)
                    continue

                # Progress update every 2 seconds or every 500 files
                now = time.time()
                total_to_process = len(file_pairs)
                if now - last_update >= 2.0 or i % 500 == 0:
                    elapsed = now - start_time
                    files_per_sec = i / elapsed if elapsed > 0 else 0
                    mb_per_sec = (bytes_processed / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                    remaining = (total_to_process - i) / files_per_sec if files_per_sec > 0 else 0

                    # Format remaining time
                    if remaining >= 3600:
                        eta = f"{remaining / 3600:.1f}h"
                    elif remaining >= 60:
                        eta = f"{remaining / 60:.1f}m"
                    else:
                        eta = f"{remaining:.0f}s"

                    pct = (i / total_to_process) * 100 if total_to_process > 0 else 100
                    print(
                        f"  [{pct:5.1f}%] {i:,}/{total_to_process:,} files | "
                        f"{files_per_sec:.1f} files/s | {mb_per_sec:.1f} MB/s | "
                        f"ETA: {eta} | subs: {stats.total_substitutions:,}"
                    )
                    last_update = now

        # Final stats
        elapsed = time.time() - start_time
        stats.elapsed_seconds = elapsed  # Store for final report
        total_to_process = len(files_to_process)
        files_per_sec = total_to_process / elapsed if elapsed > 0 else 0
        mb_per_sec = (bytes_processed / (1024 * 1024)) / elapsed if elapsed > 0 else 0

        print(f"\n{'=' * 60}")
        if interrupted:
            print(f"INTERRUPTED after {i:,} of {total_to_process:,} files")
        else:
            print("COMPLETE")
        print(f"{'=' * 60}")
        print(f"  Files processed: {i:,}")
        print(f"  Files modified:  {stats.files_modified:,}")
        print(f"  Substitutions:   {stats.total_substitutions:,}")
        if not skip_triage:
            print(
                f"  Triage skipped:  {stats.triage_quarantined + stats.triage_rejected:,} "
                f"({stats.triage_quarantined:,} quarantine, {stats.triage_rejected:,} reject)"
            )
        print(f"  Time elapsed:    {elapsed:.1f}s")
        print(f"  Throughput:      {files_per_sec:.1f} files/s, {mb_per_sec:.1f} MB/s")
        if stats.files_flagged > 0:
            print(f"  Files flagged:   {stats.files_flagged:,} (garbage patterns)")
        if stats.files_with_boilerplate > 0:
            print(
                f"  Boilerplate:     {stats.files_with_boilerplate:,} files, "
                f"{stats.total_boilerplate_chars:,} chars stripped"
            )
        print(f"{'=' * 60}")

        # Close boilerplate log file
        if boilerplate_log_file:
            boilerplate_log_file.close()

    finally:
        # Restore original signal handler
        signal.signal(signal.SIGINT, old_handler)

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

        # Use Rust to count potential errors by category
        result = rust_ocr_clean.clean_text_with_categories(content)
        for category, count in result.substitutions_by_category.items():
            error_counts[category] += count

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
    batch_parser.add_argument(
        "--report",
        type=Path,
        help="Override stats report location (default: {output_parent}/_cleanup_report.json)",
    )
    batch_parser.add_argument(
        "--triage-output",
        type=Path,
        help="Override triage results location (default: {output_parent}/_triage_results.jsonl)",
    )
    batch_parser.add_argument(
        "--no-report",
        action="store_true",
        help="Disable automatic report generation",
    )
    batch_parser.add_argument(
        "--skip-triage",
        action="store_true",
        help="Skip document triage and process all files",
    )
    batch_parser.add_argument(
        "--skip-boilerplate",
        action="store_true",
        help="Skip boilerplate stripping (Google Books, Internet Archive, etc.)",
    )
    batch_parser.add_argument(
        "--boilerplate-log",
        type=Path,
        help="Override boilerplate audit log location (default: {output_parent}/_boilerplate_stripped.jsonl)",
    )

    # Strip boilerplate only (standalone command)
    strip_parser = subparsers.add_parser(
        "strip-boilerplate", help="Strip digitization boilerplate from files"
    )
    strip_parser.add_argument("input", type=Path, help="Input file or directory")
    strip_parser.add_argument("-o", "--output", type=Path, help="Output file or directory")
    strip_parser.add_argument(
        "--pattern", default="*.txt", help="File pattern for directory mode (default: *.txt)"
    )
    strip_parser.add_argument("--log", type=Path, help="Write stripped regions to JSONL audit log")

    # Analyze
    analyze_parser = subparsers.add_parser("analyze", help="Analyze corpus for OCR errors")
    analyze_parser.add_argument("corpus_dir", type=Path, help="Corpus directory")
    analyze_parser.add_argument("--sample", type=int, default=1000, help="Sample size")
    analyze_parser.add_argument("--report", type=Path, help="Save report to JSON")

    args = parser.parse_args()

    if args.command == "clean":
        was_modified, sub_count, garbage, was_skipped = clean_file(args.input, args.output)

        if was_skipped:
            print(f"Skipped {args.input} - detected as non-English")
        elif args.output:
            print(f"Cleaned {args.input} -> {args.output}")
            print(f"  Substitutions: {sub_count}")
            if garbage:
                print(f"  Warning: {len(garbage)} garbage patterns detected")
        else:
            with open(args.input, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
            result = rust_ocr_clean.clean_text_with_categories(content)
            print(result.text)

    elif args.command == "batch":
        from datetime import datetime

        run_start = datetime.now()

        # Determine report paths (default to parent of output dir)
        output_parent = args.output_dir.parent if args.output_dir else args.input_dir
        report_path = args.report if args.report else output_parent / "_cleanup_report.json"
        triage_path = (
            args.triage_output if args.triage_output else output_parent / "_triage_results.jsonl"
        )
        boilerplate_path = (
            args.boilerplate_log
            if args.boilerplate_log
            else output_parent / "_boilerplate_stripped.jsonl"
        )

        # Don't overwrite existing files - add numeric suffix
        if report_path:
            report_path = get_unique_path(report_path)
        if triage_path:
            triage_path = get_unique_path(triage_path)
        if boilerplate_path:
            boilerplate_path = get_unique_path(boilerplate_path)

        # Disable reports if requested
        if args.no_report:
            report_path = None
            triage_path = None
            boilerplate_path = None

        # Disable boilerplate stripping if requested
        if args.skip_boilerplate:
            boilerplate_path = None

        stats = clean_batch(
            args.input_dir,
            args.output_dir,
            args.pattern,
            skip_triage=args.skip_triage,
            triage_output=triage_path,
            boilerplate_log=boilerplate_path,
        )

        run_end = datetime.now()

        print(f"\n{'=' * 60}")
        print("Batch cleanup complete")
        print(f"{'=' * 60}")

        # Format duration in human terms
        duration = stats.elapsed_seconds if hasattr(stats, "elapsed_seconds") else 0
        if duration >= 3600:
            hours = int(duration // 3600)
            mins = int((duration % 3600) // 60)
            secs = int(duration % 60)
            duration_str = f"{hours}h {mins}m {secs}s"
        elif duration >= 60:
            mins = int(duration // 60)
            secs = int(duration % 60)
            duration_str = f"{mins}m {secs}s"
        else:
            duration_str = f"{duration:.1f}s"

        print(f"  Duration: {duration_str}")
        print(f"  Total files scanned: {stats.total_files:,}")

        # Triage summary
        if stats.triage_passed > 0 or stats.triage_quarantined > 0 or stats.triage_rejected > 0:
            print("\n  Triage results:")
            print(f"    Passed (processed):  {stats.triage_passed:,}")
            print(f"    Quarantined:         {stats.triage_quarantined:,}")
            print(f"    Rejected:            {stats.triage_rejected:,}")

        # OCR cleanup results
        print("\n  OCR cleanup:")
        print(f"    Files modified:      {stats.files_modified:,}")
        print(f"    Total substitutions: {stats.total_substitutions:,}")
        if stats.files_flagged > 0:
            print(f"    Flagged (post-OCR garbage): {stats.files_flagged:,}")

        if stats.substitution_counts:
            print("\n  Top substitutions:")
            for pattern, count in stats.substitution_counts.most_common(10):
                print(f"    {pattern}: {count:,}")

        # Write reports with metadata
        print("\n  Reports:")
        print(f"    Output directory: {args.output_dir}")

        if report_path:
            report_data = {
                "metadata": {
                    "input_dir": str(args.input_dir.resolve()),
                    "output_dir": str(args.output_dir.resolve()) if args.output_dir else None,
                    "run_started": run_start.isoformat(),
                    "run_completed": run_end.isoformat(),
                    "duration_seconds": duration,
                    "duration_human": duration_str,
                    "pattern": args.pattern,
                    "skip_triage": args.skip_triage,
                },
                "stats": stats.to_dict(),
            }
            with open(report_path, "w") as f:
                json.dump(report_data, f, indent=2)
            print(f"    Stats report: {report_path}")

        if triage_path and triage_path.exists():
            print(f"    Triage results: {triage_path}")

        if boilerplate_path and boilerplate_path.exists():
            print(f"    Boilerplate log: {boilerplate_path}")
        elif args.no_report:
            print("    (Reports disabled with --no-report)")

        print(f"{'=' * 60}")

    elif args.command == "strip-boilerplate":
        import fnmatch
        import os

        input_path = args.input
        output_path = args.output
        log_path = args.log

        if input_path.is_file():
            # Single file mode
            result = rust_ocr_clean.strip_boilerplate_file(
                str(input_path), str(output_path) if output_path else None
            )

            if result.stripped_regions:
                print(f"Stripped {len(result.stripped_regions)} region(s) from {input_path}")
                for region in result.stripped_regions:
                    print(
                        f"  - {region.category}/{region.pattern_name}: "
                        f"lines {region.start_line}-{region.end_line} ({region.char_count} chars)"
                    )
                if output_path:
                    print(f"Output written to: {output_path}")
            else:
                print(f"No boilerplate found in {input_path}")

            # Write audit log if requested
            if log_path:
                log_entry = {
                    "file": str(input_path),
                    "stripped": [
                        {
                            "category": r.category,
                            "pattern": r.pattern_name,
                            "lines": [r.start_line, r.end_line],
                            "chars": r.char_count,
                        }
                        for r in result.stripped_regions
                    ],
                }
                with open(log_path, "w") as f:
                    f.write(json.dumps(log_entry) + "\n")
                print(f"Audit log written to: {log_path}")

        elif input_path.is_dir():
            # Directory mode
            if not output_path:
                print("Error: --output is required for directory mode", file=sys.stderr)
                sys.exit(1)

            output_path.mkdir(parents=True, exist_ok=True)

            # Find files
            files = []
            for root, dirs, filenames in os.walk(input_path):
                for filename in filenames:
                    if fnmatch.fnmatch(filename, args.pattern):
                        files.append(Path(root) / filename)

            print(f"Processing {len(files)} files...")

            files_with_boilerplate = 0
            total_chars_stripped = 0
            log_file = None

            if log_path:
                log_file = open(log_path, "w")

            try:
                for i, file_path in enumerate(files, 1):
                    relative = file_path.relative_to(input_path)
                    out_file = output_path / relative
                    out_file.parent.mkdir(parents=True, exist_ok=True)

                    result = rust_ocr_clean.strip_boilerplate_file(str(file_path), str(out_file))

                    if result.stripped_regions:
                        files_with_boilerplate += 1
                        total_chars_stripped += result.total_chars_stripped

                        if log_file:
                            log_entry = {
                                "file": str(relative),
                                "stripped": [
                                    {
                                        "category": r.category,
                                        "pattern": r.pattern_name,
                                        "lines": [r.start_line, r.end_line],
                                        "chars": r.char_count,
                                    }
                                    for r in result.stripped_regions
                                ],
                            }
                            log_file.write(json.dumps(log_entry) + "\n")

                    if i % 500 == 0:
                        print(f"  Processed {i}/{len(files)} files...")

            finally:
                if log_file:
                    log_file.close()

            print("\nComplete!")
            print(f"  Files processed: {len(files)}")
            print(f"  Files with boilerplate: {files_with_boilerplate}")
            print(f"  Total chars stripped: {total_chars_stripped:,}")
            if log_path:
                print(f"  Audit log: {log_path}")

        else:
            print(f"Error: {input_path} does not exist", file=sys.stderr)
            sys.exit(1)

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
