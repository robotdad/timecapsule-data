#!/usr/bin/env python3
"""
Analyze triage results to understand document quality distribution.

Usage:
    python scripts/analyze_triage.py /mnt/e/datasets/timecapsule-prewwi/cleaned/_triage_results.jsonl
    python scripts/analyze_triage.py /mnt/e/datasets/timecapsule-prewwi/cleaned/_triage_results.jsonl --action quarantine
    python scripts/analyze_triage.py /mnt/e/datasets/timecapsule-prewwi/cleaned/_triage_results.jsonl --reasons
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path


def stream_jsonl(path: Path):
    """Stream JSONL records without loading entire file."""
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def analyze_triage(path: Path, limit: int | None = None) -> dict:
    """Analyze triage results and return statistics."""
    stats = {
        "total": 0,
        "by_action": Counter(),
        "reasons": defaultdict(Counter),  # action -> reason -> count
        "scores": [],  # For score distribution
        "languages": Counter(),
        "sample_files": defaultdict(list),  # action -> sample file paths
    }

    for i, record in enumerate(stream_jsonl(path)):
        if limit and i >= limit:
            break

        stats["total"] += 1
        action = record.get("action", "unknown")
        stats["by_action"][action] += 1

        # Track reasons
        reason = record.get("reason", "none")
        stats["reasons"][action][reason] += 1

        # Track scores if available
        if "score" in record:
            stats["scores"].append(record["score"])

        # Track language if available
        lang_info = record.get("language", {})
        if isinstance(lang_info, dict):
            detected = lang_info.get("detected", "unknown")
            stats["languages"][detected] += 1

        # Keep sample files (first 10 per action)
        if len(stats["sample_files"][action]) < 10:
            stats["sample_files"][action].append(record.get("path", "unknown"))

    return stats


def print_summary(stats: dict) -> None:
    """Print overall summary."""
    print("=" * 70)
    print("TRIAGE RESULTS SUMMARY")
    print("=" * 70)

    print(f"\nTotal documents: {stats['total']:,}")

    print("\n" + "-" * 70)
    print("BY ACTION")
    print("-" * 70)
    for action, count in stats["by_action"].most_common():
        pct = (count / stats["total"] * 100) if stats["total"] else 0
        print(f"  {action:15} {count:>10,}  ({pct:5.1f}%)")


def print_reasons(stats: dict, action_filter: str | None = None) -> None:
    """Print reason breakdown by action."""
    print("\n" + "=" * 70)
    print("REASONS BY ACTION")
    print("=" * 70)

    for action in sorted(stats["reasons"].keys()):
        if action_filter and action != action_filter:
            continue

        reasons = stats["reasons"][action]
        total = sum(reasons.values())

        print(f"\n{action.upper()} ({total:,} documents):")
        print("-" * 50)
        for reason, count in reasons.most_common(20):
            pct = (count / total * 100) if total else 0
            # Truncate long reasons
            reason_display = reason[:40] + "..." if len(reason) > 40 else reason
            print(f"  {reason_display:45} {count:>8,}  ({pct:5.1f}%)")


def print_samples(stats: dict, action: str) -> None:
    """Print sample files for an action."""
    samples = stats["sample_files"].get(action, [])
    if not samples:
        print(f"\nNo samples found for action: {action}")
        return

    print("\n" + "=" * 70)
    print(f"SAMPLE FILES: {action.upper()}")
    print("=" * 70)
    for path in samples:
        print(f"  {path}")


def print_languages(stats: dict) -> None:
    """Print language distribution."""
    if not stats["languages"]:
        return

    print("\n" + "=" * 70)
    print("LANGUAGE DISTRIBUTION")
    print("=" * 70)

    total = sum(stats["languages"].values())
    for lang, count in stats["languages"].most_common(20):
        pct = (count / total * 100) if total else 0
        print(f"  {lang:15} {count:>10,}  ({pct:5.1f}%)")


def print_score_distribution(stats: dict) -> None:
    """Print score distribution if available."""
    scores = stats["scores"]
    if not scores:
        return

    print("\n" + "=" * 70)
    print("SCORE DISTRIBUTION")
    print("=" * 70)

    print(f"  Count:  {len(scores):,}")
    print(f"  Min:    {min(scores):.4f}")
    print(f"  Max:    {max(scores):.4f}")
    print(f"  Mean:   {sum(scores) / len(scores):.4f}")

    # Percentiles
    sorted_scores = sorted(scores)
    for pct in [25, 50, 75, 90, 95]:
        idx = int(len(sorted_scores) * pct / 100)
        print(f"  P{pct}:    {sorted_scores[idx]:.4f}")


def export_action_files(path: Path, action: str, output_path: Path) -> int:
    """Export file paths for a specific action."""
    count = 0
    with open(output_path, "w") as out:
        for record in stream_jsonl(path):
            if record.get("action") == action:
                out.write(record.get("path", "") + "\n")
                count += 1
    return count


def main():
    parser = argparse.ArgumentParser(description="Analyze triage results")
    parser.add_argument("triage_path", type=Path, help="Path to _triage_results.jsonl")
    parser.add_argument(
        "--action", type=str, help="Filter to specific action (pass/quarantine/reject)"
    )
    parser.add_argument("--reasons", action="store_true", help="Show detailed reason breakdown")
    parser.add_argument("--languages", action="store_true", help="Show language distribution")
    parser.add_argument("--scores", action="store_true", help="Show score distribution")
    parser.add_argument("--samples", type=str, help="Show sample files for action")
    parser.add_argument("--export", type=str, help="Export file paths for action to file")
    parser.add_argument("--limit", type=int, help="Limit records to process (for testing)")

    args = parser.parse_args()

    if not args.triage_path.exists():
        print(f"Error: File not found: {args.triage_path}", file=sys.stderr)
        sys.exit(1)

    # Export mode
    if args.export:
        if not args.action:
            print("Error: --export requires --action", file=sys.stderr)
            sys.exit(1)
        output_path = Path(args.export)
        count = export_action_files(args.triage_path, args.action, output_path)
        print(f"Exported {count:,} file paths to {output_path}")
        return

    print(f"Analyzing: {args.triage_path}")
    if args.limit:
        print(f"(Limited to first {args.limit:,} records)")

    stats = analyze_triage(args.triage_path, args.limit)

    print_summary(stats)

    if args.reasons:
        print_reasons(stats, args.action)

    if args.languages:
        print_languages(stats)

    if args.scores:
        print_score_distribution(stats)

    if args.samples:
        print_samples(stats, args.samples)


if __name__ == "__main__":
    main()
