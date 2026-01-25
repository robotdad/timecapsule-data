#!/usr/bin/env python3
"""
Reset triage data in the database.

Use this when triage results are invalid (e.g., wrong path assumptions)
and need to be re-run from scratch.

Usage:
    python scripts/reset_triage.py --db PATH [--dry-run]
"""

import argparse
import sqlite3
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Reset triage data in database")
    parser.add_argument("--db", required=True, type=Path, help="Path to SQLite database")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be reset without changing"
    )
    args = parser.parse_args()

    if not args.db.exists():
        print(f"ERROR: Database not found: {args.db}")
        sys.exit(1)

    conn = sqlite3.connect(str(args.db), timeout=60.0)

    # Check current triage state
    triaged = conn.execute("SELECT COUNT(*) FROM items WHERE triage_action IS NOT NULL").fetchone()[
        0
    ]
    by_action = conn.execute("""
        SELECT triage_action, COUNT(*) as count
        FROM items
        WHERE triage_action IS NOT NULL
        GROUP BY triage_action
    """).fetchall()

    print(f"{'=' * 60}")
    print("Triage Data Reset")
    print(f"{'=' * 60}")
    print(f"  Database: {args.db}")
    print(f"  Currently triaged: {triaged:,}")
    print()
    print("  Breakdown by action:")
    for row in by_action:
        print(f"    {row[0]}: {row[1]:,}")
    print()

    if triaged == 0:
        print("Nothing to reset.")
        return 0

    if args.dry_run:
        print(f"DRY RUN: Would reset {triaged:,} records")
        return 0

    # Confirm
    print(f"This will reset {triaged:,} triage records.")
    confirm = input("Type 'yes' to confirm: ")
    if confirm.lower() != "yes":
        print("Aborted.")
        return 1

    # Reset
    print("Resetting...", end="", flush=True)
    conn.execute("""
        UPDATE items SET
            triage_action = NULL,
            triage_problems = NULL,
            triage_alpha_ratio = NULL,
            triage_lang = NULL,
            triage_lang_confidence = NULL,
            triage_at = NULL
        WHERE triage_action IS NOT NULL
    """)
    conn.commit()
    print(f" done. Reset {conn.total_changes:,} records.")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
