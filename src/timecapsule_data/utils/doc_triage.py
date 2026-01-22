#!/usr/bin/env python3
"""
Document Triage Module

Fast heuristic-based document classification to filter out problematic content
BEFORE running expensive OCR cleanup. Identifies:
- Low quality scans (low alpha ratio, fragmented text)
- Multicolumn content (newspapers with column mixing)
- Catalog-like content (lists, indexes)
- Non-English content (via language detection)

This runs BEFORE OCR cleanup to avoid wasting compute on garbage.

Pipeline position:
    [Raw Download] -> [TRIAGE] -> [OCR Score] -> [OCR Clean] -> [SymSpell]

Usage:
    # Triage a single file
    tc-doc-triage file input.txt

    # Triage all files in a directory (outputs JSONL)
    tc-doc-triage batch ./corpus_raw -o triage_results.jsonl

    # Triage and show summary statistics
    tc-doc-triage batch ./corpus_raw --stats

    # Filter to show only files with problems
    tc-doc-triage batch ./corpus_raw --filter quarantine
    tc-doc-triage batch ./corpus_raw --filter reject

Actions:
    pass       - File is suitable for OCR cleanup pipeline
    quarantine - File has structural issues (multicolumn, catalog-like)
                 May be recoverable with specialized processing later
    reject     - File is garbage (low alpha, fragmented scans, non-English)
                 Not worth processing

Problems detected:
    low_alpha    - Alpha character ratio < 0.45 (scan garbage, photo albums)
    fragmented   - Mean words/line < 2.5 AND fragment ratio > 0.5
    multicolumn  - High line length variance + high fragment ratio
                   Indicates newspaper column mixing
    catalog_like - High list pattern ratio (numbered/bulleted lists)
    too_short    - File has < 5 non-empty lines
    non_english  - Language detection indicates non-English content
"""

import argparse
import json
import signal
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Optional

import rust_ocr_clean


def triage_single(path: str, verbose: bool = False, check_language: bool = True) -> dict:
    """Triage a single file and return result dict."""
    result = rust_ocr_clean.triage_file(path)

    output = {
        "path": result.path,
        "action": result.action,
        "problems": list(result.problems),
        "signals": {
            "alpha_ratio": round(result.alpha_ratio, 4),
            "line_length_cv": round(result.line_length_cv, 4),
            "mean_words_per_line": round(result.mean_words_per_line, 2),
            "fragment_ratio": round(result.fragment_ratio, 4),
            "list_pattern_ratio": round(result.list_pattern_ratio, 4),
            "line_count": result.line_count,
            "char_count": result.char_count,
        },
    }

    # Language detection (only if file passes structural checks)
    if check_language and result.action == "pass":
        try:
            lang_result = rust_ocr_clean.detect_language_file(path, 0.5)
            output["language"] = {
                "detected": lang_result.detected_lang,
                "confidence": round(lang_result.confidence, 4),
                "is_english": lang_result.is_english,
            }
            if not lang_result.is_english:
                output["action"] = "reject"
                output["problems"] = list(output["problems"]) + ["non_english"]
        except Exception:
            # If language detection fails, assume English
            output["language"] = {"detected": "unknown", "confidence": 0.0, "is_english": True}

    if verbose:
        print(f"\nFile: {path}")
        print(f"  Action: {output['action']}")
        print(f"  Problems: {output['problems'] if output['problems'] else 'none'}")
        print("  Signals:")
        print(f"    alpha_ratio:         {result.alpha_ratio:.4f}")
        print(f"    line_length_cv:      {result.line_length_cv:.4f}")
        print(f"    mean_words_per_line: {result.mean_words_per_line:.2f}")
        print(f"    fragment_ratio:      {result.fragment_ratio:.4f}")
        print(f"    list_pattern_ratio:  {result.list_pattern_ratio:.4f}")
        print(f"    line_count:          {result.line_count}")
        print(f"    char_count:          {result.char_count}")
        if "language" in output:
            lang = output["language"]
            print(f"  Language: {lang['detected']} (confidence: {lang['confidence']:.2f})")

    return output


def triage_batch(
    input_dir: Path,
    output_file: Optional[Path] = None,
    filter_action: Optional[str] = None,
    show_stats: bool = False,
    verbose: bool = False,
    check_language: bool = True,
    chunk_size: int = 1000,
) -> tuple[list[dict], dict]:
    """
    Triage all .txt files in a directory.

    Returns:
        (results, stats) where stats is a summary dict
    """
    # Set up interrupt handling
    interrupted = False
    old_handler = signal.getsignal(signal.SIGINT)

    def handle_interrupt(signum, frame):
        nonlocal interrupted
        if interrupted:
            # Second interrupt - exit immediately
            print("\n\nForce quit!", file=sys.stderr)
            sys.exit(1)
        interrupted = True
        print("\n\nInterrupted! Finishing current chunk, then stopping...", file=sys.stderr)

    signal.signal(signal.SIGINT, handle_interrupt)

    try:
        # Collect all txt files
        print(f"Scanning {input_dir} for *.txt files...", end=" ", file=sys.stderr, flush=True)
        txt_files = list(input_dir.rglob("*.txt"))
        print(f"found {len(txt_files):,} files", file=sys.stderr)

        if not txt_files:
            print(f"No .txt files found in {input_dir}", file=sys.stderr)
            return [], {}

        # Process in chunks for progress reporting
        results = []
        action_counts = Counter()
        problem_counts = Counter()
        language_counts = Counter()
        total_chars = 0
        total_lines = 0
        non_english_count = 0

        start_time = time.time()
        last_update = start_time
        total_files = len(txt_files)

        print(f"\nTriaging {total_files:,} files...", file=sys.stderr)

        for chunk_start in range(0, total_files, chunk_size):
            if interrupted:
                break

            chunk_end = min(chunk_start + chunk_size, total_files)
            chunk_files = txt_files[chunk_start:chunk_end]
            paths = [str(f) for f in chunk_files]

            # Batch process structural triage with Rust
            rust_results = rust_ocr_clean.triage_batch(paths)

            # Process results and add language detection
            for r in rust_results:
                if interrupted:
                    break

                result_dict = {
                    "path": r.path,
                    "action": r.action,
                    "problems": list(r.problems),
                    "signals": {
                        "alpha_ratio": round(r.alpha_ratio, 4),
                        "line_length_cv": round(r.line_length_cv, 4),
                        "mean_words_per_line": round(r.mean_words_per_line, 2),
                        "fragment_ratio": round(r.fragment_ratio, 4),
                        "list_pattern_ratio": round(r.list_pattern_ratio, 4),
                        "line_count": r.line_count,
                        "char_count": r.char_count,
                    },
                }

                # Language detection for files that pass structural checks
                if check_language and r.action == "pass":
                    try:
                        lang_result = rust_ocr_clean.detect_language_file(r.path, 0.5)
                        result_dict["language"] = {
                            "detected": lang_result.detected_lang,
                            "confidence": round(lang_result.confidence, 4),
                            "is_english": lang_result.is_english,
                        }
                        if not lang_result.is_english:
                            result_dict["action"] = "reject"
                            result_dict["problems"] = list(result_dict["problems"]) + [
                                "non_english"
                            ]
                            non_english_count += 1
                            language_counts[lang_result.detected_lang] += 1
                    except Exception:
                        result_dict["language"] = {
                            "detected": "unknown",
                            "confidence": 0.0,
                            "is_english": True,
                        }

                # Update counters
                action_counts[result_dict["action"]] += 1
                for p in result_dict["problems"]:
                    problem_counts[p] += 1
                total_chars += r.char_count
                total_lines += r.line_count

                # Apply filter if specified
                if filter_action is None or result_dict["action"] == filter_action:
                    results.append(result_dict)

            # Progress update
            now = time.time()
            processed = chunk_end
            if now - last_update >= 1.0 or processed == total_files:
                elapsed = now - start_time
                files_per_sec = processed / elapsed if elapsed > 0 else 0
                remaining = (total_files - processed) / files_per_sec if files_per_sec > 0 else 0

                # Format ETA
                if remaining >= 3600:
                    eta = f"{remaining / 3600:.1f}h"
                elif remaining >= 60:
                    eta = f"{remaining / 60:.1f}m"
                else:
                    eta = f"{remaining:.0f}s"

                pct = (processed / total_files) * 100
                pass_count = action_counts.get("pass", 0)
                quar_count = action_counts.get("quarantine", 0)
                rej_count = action_counts.get("reject", 0)

                print(
                    f"  [{pct:5.1f}%] {processed:,}/{total_files:,} | "
                    f"{files_per_sec:.0f} files/s | ETA: {eta} | "
                    f"pass: {pass_count:,}, quarantine: {quar_count:,}, reject: {rej_count:,}",
                    file=sys.stderr,
                )
                last_update = now

        # Final stats
        elapsed = time.time() - start_time
        processed_count = sum(action_counts.values())

        stats = {
            "total_files": processed_count,
            "actions": dict(action_counts),
            "problems": dict(problem_counts),
            "languages": dict(language_counts),
            "non_english_count": non_english_count,
            "total_chars": total_chars,
            "total_lines": total_lines,
            "pass_rate": action_counts.get("pass", 0) / processed_count if processed_count else 0,
            "elapsed_seconds": elapsed,
            "interrupted": interrupted,
        }

        # Output results
        if output_file:
            with open(output_file, "w") as f:
                for r in results:
                    f.write(json.dumps(r) + "\n")
            print(f"\nWrote {len(results):,} results to {output_file}", file=sys.stderr)

        if show_stats or verbose:
            print(f"\n{'=' * 60}", file=sys.stderr)
            if interrupted:
                print(
                    f"INTERRUPTED after {processed_count:,} of {total_files:,} files",
                    file=sys.stderr,
                )
            else:
                print("Triage Statistics", file=sys.stderr)
            print(f"{'=' * 60}", file=sys.stderr)
            print(f"  Total files:  {processed_count:,}", file=sys.stderr)
            print(f"  Time elapsed: {elapsed:.1f}s", file=sys.stderr)
            print(f"  Throughput:   {processed_count / elapsed:.0f} files/s", file=sys.stderr)

            print("\nActions:", file=sys.stderr)
            for action in ["pass", "quarantine", "reject"]:
                count = action_counts.get(action, 0)
                pct = count / processed_count * 100 if processed_count else 0
                print(f"  {action:12} {count:>8,} ({pct:5.1f}%)", file=sys.stderr)

            print("\nProblems detected:", file=sys.stderr)
            for problem, count in sorted(problem_counts.items(), key=lambda x: -x[1]):
                pct = count / processed_count * 100 if processed_count else 0
                print(f"  {problem:15} {count:>8,} ({pct:5.1f}%)", file=sys.stderr)

            if language_counts:
                print("\nNon-English languages detected:", file=sys.stderr)
                for lang, count in sorted(language_counts.items(), key=lambda x: -x[1])[:15]:
                    pct = count / non_english_count * 100 if non_english_count else 0
                    print(f"  {lang:12} {count:>8,} ({pct:5.1f}%)", file=sys.stderr)
                if len(language_counts) > 15:
                    print(f"  ... and {len(language_counts) - 15} more languages", file=sys.stderr)
                print(f"\n  Total non-English: {non_english_count:,} files", file=sys.stderr)

            print("\nTotal content:", file=sys.stderr)
            print(f"  Lines: {total_lines:,}", file=sys.stderr)
            print(f"  Chars: {total_chars:,}", file=sys.stderr)
            print(f"{'=' * 60}", file=sys.stderr)

        return results, stats

    finally:
        # Restore original signal handler
        signal.signal(signal.SIGINT, old_handler)


def cmd_file(args):
    """Handle 'file' subcommand."""
    result = triage_single(args.input, verbose=True, check_language=not args.no_language)

    if args.json:
        print(json.dumps(result, indent=2))

    # Exit code based on action
    if result["action"] == "pass":
        return 0
    elif result["action"] == "quarantine":
        return 1
    else:  # reject
        return 2


def cmd_batch(args):
    """Handle 'batch' subcommand."""
    input_dir = Path(args.input)

    if not input_dir.is_dir():
        print(f"Error: {input_dir} is not a directory", file=sys.stderr)
        return 1

    output_file = Path(args.output) if args.output else None

    results, stats = triage_batch(
        input_dir,
        output_file=output_file,
        filter_action=args.filter,
        show_stats=args.stats or not output_file,
        verbose=args.verbose,
        check_language=not args.no_language,
    )

    # If no output file, print results to stdout
    if not output_file and not args.stats:
        for r in results:
            print(json.dumps(r))

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Document triage for OCR corpus filtering",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Triage a single file
    tc-doc-triage file input.txt

    # Triage directory and save results
    tc-doc-triage batch ./corpus_raw -o triage.jsonl

    # Show statistics only
    tc-doc-triage batch ./corpus_raw --stats

    # Find all files that should be quarantined
    tc-doc-triage batch ./corpus_raw --filter quarantine

    # Skip language detection (faster)
    tc-doc-triage batch ./corpus_raw --no-language

Actions:
    pass       - Suitable for OCR cleanup
    quarantine - Structural issues, may recover later
    reject     - Garbage or non-English, not worth processing
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # File subcommand
    file_parser = subparsers.add_parser("file", help="Triage a single file")
    file_parser.add_argument("input", help="Input file path")
    file_parser.add_argument("--json", action="store_true", help="Output as JSON")
    file_parser.add_argument("--no-language", action="store_true", help="Skip language detection")
    file_parser.set_defaults(func=cmd_file)

    # Batch subcommand
    batch_parser = subparsers.add_parser("batch", help="Triage all files in directory")
    batch_parser.add_argument("input", help="Input directory")
    batch_parser.add_argument("-o", "--output", help="Output JSONL file")
    batch_parser.add_argument(
        "--filter",
        choices=["pass", "quarantine", "reject"],
        help="Filter results to specific action",
    )
    batch_parser.add_argument("--stats", action="store_true", help="Show statistics summary")
    batch_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    batch_parser.add_argument("--no-language", action="store_true", help="Skip language detection")
    batch_parser.set_defaults(func=cmd_batch)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
