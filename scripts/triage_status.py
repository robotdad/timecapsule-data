#!/usr/bin/env python3
"""
Show triage status and statistics from the database.

Usage:
    python scripts/triage_status.py --db PATH
"""

import argparse
import sqlite3
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Show triage status")
    parser.add_argument("--db", required=True, type=Path, help="Path to SQLite database")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"ERROR: Database not found: {args.db}")
        sys.exit(1)

    conn = sqlite3.connect(str(args.db), timeout=60.0)

    # Overall counts
    total = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    downloaded = conn.execute(
        "SELECT COUNT(*) FROM items WHERE downloaded_at IS NOT NULL"
    ).fetchone()[0]
    triaged = conn.execute("SELECT COUNT(*) FROM items WHERE triage_action IS NOT NULL").fetchone()[
        0
    ]
    pending = conn.execute(
        "SELECT COUNT(*) FROM items WHERE downloaded_at IS NOT NULL AND triage_action IS NULL"
    ).fetchone()[0]

    print(f"{'=' * 60}")
    print("Triage Status")
    print(f"{'=' * 60}")
    print(f"  Total records:    {total:,}")
    print(f"  Downloaded:       {downloaded:,}")
    print(f"  Triaged:          {triaged:,}")
    print(f"  Pending triage:   {pending:,}")

    if downloaded > 0:
        pct = (triaged / downloaded) * 100
        print(f"  Progress:         {pct:.1f}%")
    print()

    # Breakdown by action
    if triaged > 0:
        print("Triage Results:")
        print("-" * 40)
        by_action = conn.execute("""
            SELECT triage_action, COUNT(*) as count
            FROM items
            WHERE triage_action IS NOT NULL
            GROUP BY triage_action
            ORDER BY count DESC
        """).fetchall()

        for row in by_action:
            action = row[0]
            count = row[1]
            pct = (count / triaged) * 100
            print(f"  {action:12} {count:>10,} ({pct:5.1f}%)")
        print()

        # Top problems
        print("Top Problems (for quarantine/reject):")
        print("-" * 40)
        problems = conn.execute("""
            SELECT triage_problems, COUNT(*) as count
            FROM items
            WHERE triage_problems IS NOT NULL
            GROUP BY triage_problems
            ORDER BY count DESC
            LIMIT 15
        """).fetchall()

        for row in problems:
            print(f"  {row[1]:>8,}  {row[0]}")
        print()

        # Language distribution (for passed files)
        print("Language Distribution (passed files):")
        print("-" * 40)
        langs = conn.execute("""
            SELECT triage_lang, COUNT(*) as count
            FROM items
            WHERE triage_action = 'pass' AND triage_lang IS NOT NULL
            GROUP BY triage_lang
            ORDER BY count DESC
            LIMIT 10
        """).fetchall()

        for row in langs:
            print(f"  {row[0] or 'unknown':12} {row[1]:>10,}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
