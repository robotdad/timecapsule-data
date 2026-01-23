#!/usr/bin/env python3
"""
Analyze OCR cleanup report to understand substitution patterns.

Usage:
    python scripts/analyze_cleanup_report.py /mnt/e/datasets/timecapsule-prewwi/cleaned/_cleanup_report.json
    python scripts/analyze_cleanup_report.py /mnt/e/datasets/timecapsule-prewwi/cleaned/_cleanup_report.json --top 50
    python scripts/analyze_cleanup_report.py /mnt/e/datasets/timecapsule-prewwi/cleaned/_cleanup_report.json --high-sub 1000
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path


def load_report(path: Path) -> dict:
    """Load the cleanup report JSON."""
    with open(path) as f:
        return json.load(f)


def analyze_summary(report: dict) -> None:
    """Print overall summary statistics."""
    print("=" * 70)
    print("CLEANUP REPORT SUMMARY")
    print("=" * 70)

    # Handle nested structure (stats under 'stats' key) or flat structure
    stats = report.get("stats", report)
    metadata = report.get("metadata", {})

    # Print metadata if available
    if metadata:
        print(f"\nRun: {metadata.get('run_started', 'N/A')}")
        print(f"Duration: {metadata.get('duration_human', 'N/A')}")

    total_files = stats.get("total_files", stats.get("files_processed", 0))
    files_modified = stats.get("files_modified", 0)
    total_subs = stats.get("total_substitutions", 0)

    print(f"\nFiles processed:     {total_files:,}")
    print(f"Files modified:      {files_modified:,}")
    print(f"Total substitutions: {total_subs:,}")

    if total_files:
        avg_subs = total_subs / total_files
        print(f"Avg subs/file:       {avg_subs:,.1f}")

    # Triage breakdown if available
    if "triage_passed" in stats:
        print("\n" + "-" * 70)
        print("TRIAGE BREAKDOWN")
        print("-" * 70)
        print(f"  Passed:      {stats.get('triage_passed', 0):>10,}")
        print(f"  Quarantined: {stats.get('triage_quarantined', 0):>10,}")
        print(f"  Rejected:    {stats.get('triage_rejected', 0):>10,}")

    # Substitution breakdown if available
    breakdown = stats.get("substitution_breakdown", {})
    if breakdown:
        print("\n" + "-" * 70)
        print("SUBSTITUTION BREAKDOWN")
        print("-" * 70)
        total = sum(breakdown.values())
        for cat, count in sorted(breakdown.items(), key=lambda x: -x[1]):
            if count > 0:
                pct = (count / total * 100) if total else 0
                print(f"  {cat:25} {count:>12,}  ({pct:5.1f}%)")

    # Category breakdown if available (newer format)
    if "substitutions_by_category" in report:
        print("\n" + "-" * 70)
        print("SUBSTITUTIONS BY CATEGORY")
        print("-" * 70)
        cats = report["substitutions_by_category"]
        total = sum(cats.values())
        for cat, count in sorted(cats.items(), key=lambda x: -x[1]):
            pct = (count / total * 100) if total else 0
            print(f"  {cat:25} {count:>12,}  ({pct:5.1f}%)")


def analyze_per_document(report: dict, top_n: int = 20, high_sub_threshold: int = 500) -> None:
    """Analyze per-document substitution data."""
    # Handle nested structure (stats under 'stats' key) or flat structure
    stats = report.get("stats", report)

    # Check for high substitution documents (new format)
    high_sub_data = stats.get("high_substitution_documents", {})
    long_s_data = stats.get("long_s_documents", {})

    if not high_sub_data and not long_s_data:
        # Fall back to old format
        docs = report.get("documents", [])
        if not docs:
            print("\nNo per-document data available.")
            return
        _analyze_legacy_documents(docs, top_n, high_sub_threshold)
        return

    print("\n" + "=" * 70)
    print("PER-DOCUMENT ANALYSIS")
    print("=" * 70)

    # High substitution documents
    if high_sub_data:
        total_count = high_sub_data.get("total_count", 0)
        threshold = high_sub_data.get("threshold_per_1000_chars", "N/A")
        sample_files = high_sub_data.get("sample_files", [])

        print(f"\nHIGH SUBSTITUTION DOCUMENTS: {total_count:,} total")
        print(f"  (threshold: {threshold} substitutions per 1000 chars)")
        print("-" * 70)

        if sample_files:
            # Sort by substitution rate
            sorted_files = sorted(
                sample_files, key=lambda x: x.get("substitution_rate", 0), reverse=True
            )
            print(f"\nTOP {min(top_n, len(sorted_files))} BY SUBSTITUTION RATE:")
            for i, doc in enumerate(sorted_files[:top_n], 1):
                rate = doc.get("substitution_rate", 0)
                total_subs = doc.get("total_substitutions", 0)
                filename = doc.get("filename", "unknown")
                # Truncate filename for display
                if len(filename) > 45:
                    filename = "..." + filename[-42:]
                print(f"  {i:3}. {rate:>6.1f}/1k chars  {total_subs:>6,} subs  {filename}")

            # Category breakdown from sample files
            category_totals = Counter()
            for doc in sample_files:
                cats = doc.get("categories", {})
                for cat, count in cats.items():
                    category_totals[cat] += count

            if category_totals:
                print(f"\nCATEGORY BREAKDOWN (from {len(sample_files)} sampled docs):")
                print("-" * 50)
                total = sum(category_totals.values())
                for cat, count in category_totals.most_common():
                    pct = (count / total * 100) if total else 0
                    print(f"  {cat:20} {count:>10,}  ({pct:5.1f}%)")

    # Long-s documents
    if long_s_data:
        total_count = long_s_data.get("total_count", 0)
        sample_files = long_s_data.get("sample_files", [])

        print(f"\nLONG-S DOCUMENTS: {total_count:,} total")
        print("-" * 70)

        if sample_files:
            sorted_files = sorted(
                sample_files, key=lambda x: x.get("long_s_fixes", 0), reverse=True
            )
            print(f"\nTOP {min(top_n, len(sorted_files))} BY LONG-S FIXES:")
            for i, doc in enumerate(sorted_files[:top_n], 1):
                fixes = doc.get("long_s_fixes", 0)
                filename = doc.get("filename", "unknown")
                if len(filename) > 50:
                    filename = "..." + filename[-47:]
                print(f"  {i:3}. {fixes:>6,} fixes  {filename}")


def _analyze_legacy_documents(docs: list, top_n: int, high_sub_threshold: int) -> None:
    """Analyze legacy per-document format (flat list)."""
    print("\n" + "=" * 70)
    print(f"PER-DOCUMENT ANALYSIS ({len(docs):,} documents)")
    print("=" * 70)

    # Sort by substitution count
    docs_sorted = sorted(docs, key=lambda x: x.get("substitutions", 0), reverse=True)

    # Top N by substitutions
    print(f"\nTOP {top_n} FILES BY SUBSTITUTION COUNT:")
    print("-" * 70)
    for i, doc in enumerate(docs_sorted[:top_n], 1):
        subs = doc.get("substitutions", 0)
        path = doc.get("file", doc.get("path", "unknown"))
        # Truncate path for display
        if len(path) > 50:
            path = "..." + path[-47:]
        print(f"  {i:3}. {subs:>8,} subs  {path}")

    # High substitution files (potential problem documents)
    high_sub_docs = [d for d in docs if d.get("substitutions", 0) >= high_sub_threshold]
    print(f"\nFILES WITH >= {high_sub_threshold:,} SUBSTITUTIONS: {len(high_sub_docs):,}")

    # Distribution analysis
    sub_counts = [d.get("substitutions", 0) for d in docs]
    if sub_counts:
        print("\n" + "-" * 70)
        print("SUBSTITUTION DISTRIBUTION")
        print("-" * 70)
        print(f"  Min:    {min(sub_counts):,}")
        print(f"  Max:    {max(sub_counts):,}")
        print(f"  Mean:   {sum(sub_counts) / len(sub_counts):,.1f}")

        # Percentiles
        sorted_counts = sorted(sub_counts)
        for pct in [50, 75, 90, 95, 99]:
            idx = int(len(sorted_counts) * pct / 100)
            print(f"  P{pct}:    {sorted_counts[idx]:,}")

    # Category breakdown per document (if available)
    category_totals = Counter()
    docs_with_cats = 0
    for doc in docs:
        if "categories" in doc:
            docs_with_cats += 1
            for cat, count in doc["categories"].items():
                category_totals[cat] += count

    if category_totals:
        print("\n" + "-" * 70)
        print(f"CATEGORY TOTALS (from {docs_with_cats:,} documents)")
        print("-" * 70)
        total = sum(category_totals.values())
        for cat, count in category_totals.most_common():
            pct = (count / total * 100) if total else 0
            print(f"  {cat:25} {count:>12,}  ({pct:5.1f}%)")


def find_problem_documents(report: dict, threshold: int = 1000) -> list:
    """Find documents that may need manual review."""
    docs = report.get("documents", [])
    problems = []

    for doc in docs:
        subs = doc.get("substitutions", 0)
        if subs >= threshold:
            problems.append(
                {
                    "file": doc.get("file", doc.get("path", "unknown")),
                    "substitutions": subs,
                    "categories": doc.get("categories", {}),
                }
            )

    return sorted(problems, key=lambda x: -x["substitutions"])


def main():
    parser = argparse.ArgumentParser(description="Analyze OCR cleanup report")
    parser.add_argument("report_path", type=Path, help="Path to _cleanup_report.json")
    parser.add_argument("--top", type=int, default=20, help="Show top N files by substitutions")
    parser.add_argument(
        "--high-sub", type=int, default=500, help="Threshold for high substitution files"
    )
    parser.add_argument("--problems", action="store_true", help="Output problem documents as JSON")
    parser.add_argument(
        "--problems-threshold", type=int, default=1000, help="Threshold for problem documents"
    )

    args = parser.parse_args()

    if not args.report_path.exists():
        print(f"Error: File not found: {args.report_path}", file=sys.stderr)
        sys.exit(1)

    report = load_report(args.report_path)

    if args.problems:
        problems = find_problem_documents(report, args.problems_threshold)
        print(json.dumps(problems, indent=2))
    else:
        analyze_summary(report)
        analyze_per_document(report, args.top, args.high_sub)


if __name__ == "__main__":
    main()
