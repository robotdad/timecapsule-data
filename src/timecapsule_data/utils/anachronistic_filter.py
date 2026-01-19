#!/usr/bin/env python3
"""
Anachronistic Content Filter

Removes content that is anachronistic for historical corpus creation.
This is separate from OCR cleanup - it filters out modern metadata,
digitization artifacts, and post-cutoff content.

For a pre-WWI corpus (cutoff ~1914), this removes:
- URLs and web references
- Digitization project metadata (PGDP, Internet Archive)
- Modern technology references
- Copyright and scanning metadata

Usage:
    # Filter a single file
    tc-anachronistic-filter clean input.txt -o output.txt --cutoff-year 1914

    # Filter entire corpus directory
    tc-anachronistic-filter batch ./corpus_clean -o ./corpus_filtered --cutoff-year 1914

    # Analyze what would be removed
    tc-anachronistic-filter analyze ./corpus --report anachronistic.json
"""

import argparse
import json
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# Patterns for anachronistic content removal
# Format: (pattern, description)
ANACHRONISTIC_PATTERNS = [
    # ==========================================================================
    # Web and Internet references (post-1990s)
    # ==========================================================================
    (r"https?://[^\s]+", "URL"),
    (r"www\.[^\s]+", "WWW address"),
    (r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b", "Email address"),
    # ==========================================================================
    # Digitization project metadata
    # ==========================================================================
    (r"Project Gutenberg[^\n]*", "Project Gutenberg metadata"),
    (r"PGDP[^\n]*", "PGDP metadata"),
    (r"Distributed Proofreading[^\n]*", "Distributed Proofreading metadata"),
    (r"Internet Archive[^\n]*", "Internet Archive metadata"),
    (r"archive\.org[^\s]*", "Archive.org reference"),
    (r"Digitized by[^\n]*", "Digitization watermark"),
    (r"Scanned by[^\n]*", "Scanning metadata"),
    # ==========================================================================
    # File format references (anachronistic for pre-WWI)
    # ==========================================================================
    (r"\.(?:html?|pdf|jpg|png|gif|xml)\b", "File extension"),
    (r"\bHTML\b", "HTML reference"),
    (r"\bPDF\b", "PDF reference"),
    (r"\bXML\b", "XML reference"),
    (r"\bASCII\b", "ASCII reference"),
    # ==========================================================================
    # Copyright and modern legal notices
    # ==========================================================================
    (r"Copyright\s+©\s+\d{4}", "Modern copyright notice"),
    (r"All rights reserved\.?", "Rights reservation"),
    (r"ISBN[:\s-]*[\d-]+", "ISBN (post-1970)"),
]

# Phrases that indicate modern frontmatter/backmatter to remove entirely
METADATA_SECTION_MARKERS = [
    "Project Gutenberg",
    "Distributed Proofreading",
    "Internet Archive",
    "End of Project Gutenberg",
    "START OF THIS PROJECT GUTENBERG",
    "END OF THIS PROJECT GUTENBERG",
    "*** START OF THE PROJECT GUTENBERG",
    "*** END OF THE PROJECT GUTENBERG",
    "Transcriber's Note",
    "Transcribed by",
    "Produced by",
]


@dataclass
class FilterStats:
    """Track filtering statistics."""

    total_files: int = 0
    files_modified: int = 0
    total_removals: int = 0
    removal_counts: Counter = field(default_factory=Counter)
    removed_sections: list = field(default_factory=list)

    def to_dict(self):
        return {
            "total_files": self.total_files,
            "files_modified": self.files_modified,
            "total_removals": self.total_removals,
            "top_removals": self.removal_counts.most_common(50),
            "removed_sections_sample": self.removed_sections[:100],
        }


def detect_metadata_sections(text: str) -> list[tuple[int, int, str]]:
    """
    Detect frontmatter/backmatter metadata sections to remove.

    Returns: list of (start_pos, end_pos, marker_type)
    """
    sections = []
    lines = text.split("\n")

    in_metadata = False
    metadata_start = 0
    metadata_type = None

    for i, line in enumerate(lines):
        # Check if this line starts a metadata section
        for marker in METADATA_SECTION_MARKERS:
            if marker.lower() in line.lower():
                if not in_metadata:
                    in_metadata = True
                    metadata_start = i
                    metadata_type = marker
                # If we see an END marker, close the section
                elif "END" in marker.upper() and in_metadata:
                    # Calculate character positions
                    start_pos = sum(len(ln) + 1 for ln in lines[:metadata_start])
                    end_pos = sum(len(ln) + 1 for ln in lines[: i + 1])
                    sections.append((start_pos, end_pos, metadata_type))
                    in_metadata = False
                break

    return sections


def filter_text(text: str, stats: Optional[FilterStats] = None) -> tuple[str, int]:
    """
    Remove anachronistic content from text.

    Returns: (filtered_text, removal_count)
    """
    total_removals = 0

    # First, remove entire metadata sections
    metadata_sections = detect_metadata_sections(text)
    if metadata_sections:
        # Remove sections in reverse order to maintain positions
        for start, end, marker_type in reversed(metadata_sections):
            removed = text[start:end]
            text = text[:start] + text[end:]
            total_removals += 1
            if stats:
                stats.removal_counts[f"Metadata section: {marker_type}"] += 1
                stats.removed_sections.append(
                    {
                        "type": marker_type,
                        "content_preview": removed[:200] + "..." if len(removed) > 200 else removed,
                    }
                )

    # Then apply pattern-based removals
    for pattern, description in ANACHRONISTIC_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            count = len(matches)
            text = re.sub(pattern, "", text, flags=re.IGNORECASE)
            total_removals += count
            if stats:
                stats.removal_counts[description] += count

    # Clean up multiple blank lines left by removals
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text, total_removals


def filter_file(
    input_path: Path,
    output_path: Optional[Path] = None,
    stats: Optional[FilterStats] = None,
) -> tuple[bool, int]:
    """
    Filter a single file.

    Returns: (was_modified, removal_count)
    """
    try:
        with open(input_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except Exception as e:
        print(f"  Error reading {input_path}: {e}")
        return False, 0

    filtered, removal_count = filter_text(content, stats)
    was_modified = removal_count > 0

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(filtered)

    return was_modified, removal_count


def filter_batch(
    input_dir: Path,
    output_dir: Optional[Path] = None,
    file_pattern: str = "*.txt",
) -> FilterStats:
    """Filter all text files in a directory."""
    stats = FilterStats()

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

        was_modified, removal_count = filter_file(input_path, output_path, stats)

        if was_modified:
            stats.files_modified += 1
            stats.total_removals += removal_count

    return stats


def analyze_corpus(corpus_dir: Path, sample_size: int = 1000) -> dict:
    """
    Analyze corpus for anachronistic content without modifying.

    Returns analysis report.
    """
    files = list(corpus_dir.glob("**/*.txt"))
    if len(files) > sample_size:
        import random

        files = random.sample(files, sample_size)

    pattern_counts = Counter()
    total_chars = 0
    files_with_anachronisms = []

    print(f"Analyzing {len(files)} files...")

    for filepath in files:
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
        except Exception:
            continue

        total_chars += len(content)
        file_has_anachronism = False

        # Check for metadata sections
        sections = detect_metadata_sections(content)
        if sections:
            file_has_anachronism = True
            pattern_counts["Metadata sections"] += len(sections)

        # Check for pattern matches
        for pattern, description in ANACHRONISTIC_PATTERNS:
            matches = re.findall(pattern, content, re.IGNORECASE)
            if matches:
                file_has_anachronism = True
                pattern_counts[description] += len(matches)

        if file_has_anachronism:
            files_with_anachronisms.append(str(filepath))

    return {
        "files_analyzed": len(files),
        "total_chars": total_chars,
        "files_with_anachronisms": len(files_with_anachronisms),
        "anachronism_counts": pattern_counts.most_common(50),
        "affected_files_sample": files_with_anachronisms[:50],
    }


def main():
    parser = argparse.ArgumentParser(
        description="Filter anachronistic content from historical texts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  clean    Filter a single file
  batch    Filter all files in a directory
  analyze  Analyze corpus for anachronisms without modifying

Examples:
  tc-anachronistic-filter clean input.txt -o output.txt
  tc-anachronistic-filter batch ./corpus_clean -o ./corpus_filtered
  tc-anachronistic-filter analyze ./corpus --report analysis.json
        """,
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Filter single file
    clean_parser = subparsers.add_parser("clean", help="Filter a single file")
    clean_parser.add_argument("input", type=Path, help="Input file")
    clean_parser.add_argument("-o", "--output", type=Path, help="Output file (default: stdout)")

    # Batch filter
    batch_parser = subparsers.add_parser("batch", help="Filter all files in directory")
    batch_parser.add_argument("input_dir", type=Path, help="Input directory")
    batch_parser.add_argument("-o", "--output-dir", type=Path, help="Output directory")
    batch_parser.add_argument("--pattern", default="*.txt", help="File pattern (default: *.txt)")
    batch_parser.add_argument("--report", type=Path, help="Save stats report to JSON")

    # Analyze
    analyze_parser = subparsers.add_parser("analyze", help="Analyze corpus for anachronisms")
    analyze_parser.add_argument("corpus_dir", type=Path, help="Corpus directory")
    analyze_parser.add_argument("--sample", type=int, default=1000, help="Sample size")
    analyze_parser.add_argument("--report", type=Path, help="Save report to JSON")

    args = parser.parse_args()

    if args.command == "clean":
        was_modified, removal_count = filter_file(args.input, args.output)

        if args.output:
            print(f"Filtered {args.input} -> {args.output}")
            print(f"  Removals: {removal_count}")
        else:
            with open(args.input, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
            filtered, _ = filter_text(content)
            print(filtered)

    elif args.command == "batch":
        stats = filter_batch(args.input_dir, args.output_dir, args.pattern)

        print(f"\n{'=' * 60}")
        print("Batch filtering complete")
        print(f"{'=' * 60}")
        print(f"  Total files: {stats.total_files}")
        print(f"  Files modified: {stats.files_modified}")
        print(f"  Total removals: {stats.total_removals}")

        if stats.removal_counts:
            print("\nTop removals:")
            for pattern, count in stats.removal_counts.most_common(10):
                print(f"  {pattern}: {count}")

        if args.report:
            with open(args.report, "w") as f:
                json.dump(stats.to_dict(), f, indent=2)
            print(f"\nReport saved to {args.report}")

    elif args.command == "analyze":
        report = analyze_corpus(args.corpus_dir, args.sample)

        print(f"\n{'=' * 60}")
        print("Corpus Analysis - Anachronistic Content")
        print(f"{'=' * 60}")
        print(f"  Files analyzed: {report['files_analyzed']}")
        print(f"  Files with anachronisms: {report['files_with_anachronisms']}")
        print(f"  Total characters: {report['total_chars']:,}")

        if report["anachronism_counts"]:
            print("\nAnachronistic content found:")
            for pattern, count in report["anachronism_counts"][:15]:
                print(f"  {pattern}: {count}")

        if args.report:
            with open(args.report, "w") as f:
                json.dump(report, f, indent=2)
            print(f"\nReport saved to {args.report}")


if __name__ == "__main__":
    main()
