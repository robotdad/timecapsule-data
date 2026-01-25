#!/usr/bin/env python3
"""
Strip OCR noise words from cleaned corpus files.

Removes words flagged as G (garbage) or R (repeated) category
from the vocabulary candidates file. These are pure noise that
degrades LLM training quality.

Usage:
    tc-ocr-strip batch ./cleaned -o ./training --vocab _vocab_candidates.txt
    tc-ocr-strip file input.txt -o output.txt --vocab _vocab_candidates.txt

Workflow:
    1. Run tc-ocr-clean batch (OCR pattern fixes)
    2. Run tc-ocr-vocab extract (identify noise words)
    3. Run tc-ocr-strip batch (remove G/R noise for training)
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from pathlib import Path


def cmd_batch(args: argparse.Namespace) -> int:
    """Strip noise words from batch of files."""
    import rust_ocr_clean  # type: ignore[import-not-found]

    input_dir = Path(args.input_dir).resolve()
    output_dir = (
        Path(args.output_dir).resolve() if args.output_dir else input_dir.parent / "training"
    )
    vocab_path = Path(args.vocab).resolve()

    if not input_dir.exists():
        print(f"Error: Input directory not found: {input_dir}", file=sys.stderr)
        return 1

    if not vocab_path.exists():
        print(f"Error: Vocab file not found: {vocab_path}", file=sys.stderr)
        return 1

    # Parse categories
    categories = None
    if args.categories:
        categories = [c.strip().upper() for c in args.categories.split(",")]

    # Initialize noise word set
    print(f"Loading noise words from: {vocab_path}", file=sys.stderr)
    if categories:
        print(f"  Categories: {', '.join(categories)}", file=sys.stderr)
        noise_count = rust_ocr_clean.init_noise_words(str(vocab_path), categories)
    else:
        print("  Categories: G (garbage), R (repeated) [default]", file=sys.stderr)
        noise_count = rust_ocr_clean.init_noise_words(str(vocab_path))

    print(f"  Loaded {noise_count:,} noise words", file=sys.stderr)

    if noise_count == 0:
        print("Warning: No noise words loaded. Check vocab file format.", file=sys.stderr)
        return 1

    # Set up interrupt handling
    interrupted = False

    def handle_interrupt(signum: int, frame: object) -> None:
        nonlocal interrupted
        if interrupted:
            print("\n\nForce quit.", file=sys.stderr)
            sys.exit(1)
        interrupted = True
        print(
            "\n\nInterrupted! Finishing current batch, then stopping...",
            file=sys.stderr,
        )

    old_handler = signal.signal(signal.SIGINT, handle_interrupt)

    try:
        # Discover files
        print(f"Scanning {input_dir} for *.txt files...", end="", flush=True)

        def fast_find_files(directory: Path, pattern: str) -> list[Path]:
            """Fast recursive file discovery using os.scandir."""
            import fnmatch

            results = []
            try:
                with os.scandir(directory) as entries:
                    for entry in entries:
                        if entry.is_file() and fnmatch.fnmatch(entry.name, pattern):
                            results.append(Path(entry.path))
                        elif entry.is_dir():
                            results.extend(fast_find_files(Path(entry.path), pattern))
            except PermissionError:
                pass
            return results

        files = fast_find_files(input_dir, "*.txt")
        print(f" found {len(files):,} files", file=sys.stderr)

        if not files:
            print("No files to process.", file=sys.stderr)
            return 0

        # Build file pairs
        file_pairs = []
        for input_path in files:
            relative = input_path.relative_to(input_dir)
            output_path = output_dir / relative
            file_pairs.append((str(input_path), str(output_path)))

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        num_threads = args.threads or 24

        # Print header
        print(f"\n{'=' * 60}")
        print("OCR Noise Stripping - Rust engine (parallel)")
        print(f"{'=' * 60}")
        print(f"  Files to process: {len(file_pairs):,}")
        print(f"  Noise words: {noise_count:,}")
        print(f"  Output: {output_dir}")
        print(f"  Threads: {num_threads}")
        print(f"{'=' * 60}\n")

        # Process in batches for progress reporting
        start_time = time.time()
        total_to_process = len(file_pairs)
        BATCH_SIZE = 1000

        files_processed = 0
        files_modified = 0
        total_stripped = 0
        bytes_processed = 0

        for batch_start in range(0, total_to_process, BATCH_SIZE):
            if interrupted:
                break

            batch_end = min(batch_start + BATCH_SIZE, total_to_process)
            batch = file_pairs[batch_start:batch_end]

            # Process batch in parallel using Rust/Rayon
            batch_stats = rust_ocr_clean.strip_noise_batch_parallel(batch, num_threads)

            # Aggregate stats
            files_processed += batch_stats.files_processed
            files_modified += batch_stats.files_modified
            total_stripped += batch_stats.total_words_stripped
            bytes_processed += batch_stats.total_bytes

            # Progress update
            now = time.time()
            elapsed = now - start_time
            current_count = batch_end
            files_per_sec = current_count / elapsed if elapsed > 0 else 0
            mb_per_sec = (bytes_processed / (1024 * 1024)) / elapsed if elapsed > 0 else 0
            remaining = (
                (total_to_process - current_count) / files_per_sec if files_per_sec > 0 else 0
            )

            # Format remaining time
            if remaining >= 3600:
                eta = f"{remaining / 3600:.1f}h"
            elif remaining >= 60:
                eta = f"{remaining / 60:.1f}m"
            else:
                eta = f"{remaining:.0f}s"

            pct = (current_count / total_to_process) * 100 if total_to_process > 0 else 100
            print(
                f"  [{pct:5.1f}%] {current_count:,}/{total_to_process:,} files | "
                f"{files_per_sec:.1f} files/s | {mb_per_sec:.1f} MB/s | "
                f"ETA: {eta} | stripped: {total_stripped:,}"
            )

        # Final stats
        elapsed = time.time() - start_time
        files_per_sec = files_processed / elapsed if elapsed > 0 else 0
        mb_per_sec = (bytes_processed / (1024 * 1024)) / elapsed if elapsed > 0 else 0

        print(f"\n{'=' * 60}")
        if interrupted:
            print(f"INTERRUPTED after {files_processed:,} of {total_to_process:,} files")
        else:
            print("COMPLETE")
        print(f"{'=' * 60}")
        print(f"  Files processed: {files_processed:,}")
        print(f"  Files modified:  {files_modified:,}")
        print(f"  Words stripped:  {total_stripped:,}")
        print(f"  Time elapsed:    {elapsed:.1f}s")
        print(f"  Throughput:      {files_per_sec:.1f} files/s, {mb_per_sec:.1f} MB/s")
        print(f"{'=' * 60}")

        return 0 if not interrupted else 1

    finally:
        signal.signal(signal.SIGINT, old_handler)


def cmd_file(args: argparse.Namespace) -> int:
    """Strip noise words from a single file."""
    import rust_ocr_clean  # type: ignore[import-not-found]

    input_path = Path(args.input_file).resolve()
    output_path = (
        Path(args.output).resolve() if args.output else input_path.with_suffix(".stripped.txt")
    )
    vocab_path = Path(args.vocab).resolve()

    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        return 1

    if not vocab_path.exists():
        print(f"Error: Vocab file not found: {vocab_path}", file=sys.stderr)
        return 1

    # Parse categories
    categories = None
    if args.categories:
        categories = [c.strip().upper() for c in args.categories.split(",")]

    # Initialize noise word set
    if categories:
        noise_count = rust_ocr_clean.init_noise_words(str(vocab_path), categories)
    else:
        noise_count = rust_ocr_clean.init_noise_words(str(vocab_path))

    print(f"Loaded {noise_count:,} noise words", file=sys.stderr)

    # Process file
    was_modified, words_stripped, bytes_processed = rust_ocr_clean.strip_noise_file(
        str(input_path), str(output_path)
    )

    print(f"  Input:  {input_path}")
    print(f"  Output: {output_path}")
    print(f"  Words stripped: {words_stripped:,}")
    print(f"  Modified: {was_modified}")

    return 0


def cmd_check(args: argparse.Namespace) -> int:
    """Check how many words would be stripped (dry run)."""
    import rust_ocr_clean  # type: ignore[import-not-found]

    input_path = Path(args.input_file).resolve()
    vocab_path = Path(args.vocab).resolve()

    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        return 1

    if not vocab_path.exists():
        print(f"Error: Vocab file not found: {vocab_path}", file=sys.stderr)
        return 1

    # Parse categories
    categories = None
    if args.categories:
        categories = [c.strip().upper() for c in args.categories.split(",")]

    # Initialize noise word set
    if categories:
        noise_count = rust_ocr_clean.init_noise_words(str(vocab_path), categories)
    else:
        noise_count = rust_ocr_clean.init_noise_words(str(vocab_path))

    # Read and check
    content = input_path.read_text()
    _, words_stripped = rust_ocr_clean.strip_noise_words(content)

    print(f"File: {input_path}")
    print(f"Noise words loaded: {noise_count:,}")
    print(f"Words that would be stripped: {words_stripped:,}")

    return 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        prog="tc-ocr-strip",
        description="Strip OCR noise words (G/R categories) from corpus files",
        epilog="""
Workflow:
  1. Run tc-ocr-clean batch (OCR pattern fixes)
  2. Run tc-ocr-vocab extract (identify noise words)
  3. Run tc-ocr-strip batch (remove G/R noise for training)

Categories:
  G = garbage (consonant clusters, unpronounceable)
  R = repeated (character stuttering like MEEE)
  M = mixed_case (random capitals - NOT stripped by default)

Examples:
  tc-ocr-strip batch ./cleaned -o ./training --vocab _vocab_candidates.txt
  tc-ocr-strip batch ./cleaned --vocab vocab.txt --categories G,R,F
  tc-ocr-strip file doc.txt -o doc_clean.txt --vocab vocab.txt
  tc-ocr-strip check doc.txt --vocab vocab.txt
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Batch command
    batch_parser = subparsers.add_parser("batch", help="Process directory of files")
    batch_parser.add_argument("input_dir", help="Input directory")
    batch_parser.add_argument("-o", "--output-dir", help="Output directory (default: ../training)")
    batch_parser.add_argument("--vocab", required=True, help="Vocab candidates file")
    batch_parser.add_argument(
        "--categories",
        help="Comma-separated categories to strip (default: G,R)",
    )
    batch_parser.add_argument("--threads", type=int, default=24, help="Thread count (default: 24)")

    # File command
    file_parser = subparsers.add_parser("file", help="Process single file")
    file_parser.add_argument("input_file", help="Input file")
    file_parser.add_argument("-o", "--output", help="Output file")
    file_parser.add_argument("--vocab", required=True, help="Vocab candidates file")
    file_parser.add_argument(
        "--categories",
        help="Comma-separated categories to strip (default: G,R)",
    )

    # Check command (dry run)
    check_parser = subparsers.add_parser("check", help="Check how many words would be stripped")
    check_parser.add_argument("input_file", help="Input file")
    check_parser.add_argument("--vocab", required=True, help="Vocab candidates file")
    check_parser.add_argument(
        "--categories",
        help="Comma-separated categories to strip (default: G,R)",
    )

    args = parser.parse_args()

    if args.command == "batch":
        return cmd_batch(args)
    elif args.command == "file":
        return cmd_file(args)
    elif args.command == "check":
        return cmd_check(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
