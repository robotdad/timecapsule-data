#!/usr/bin/env python3
"""
Validate assumptions about the database and file structure.

Run this BEFORE running triage to catch mismatches early.

Usage:
    python scripts/validate_db_files.py --db PATH --raw-dir PATH [--sample N]
"""

import argparse
import sqlite3
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Validate DB and file structure assumptions")
    parser.add_argument("--db", required=True, type=Path, help="Path to SQLite database")
    parser.add_argument(
        "--raw-dir", required=True, type=Path, help="Directory containing raw files"
    )
    parser.add_argument(
        "--sample", type=int, default=100, help="Sample size for validation (default: 100)"
    )
    parser.add_argument("--fix-report", action="store_true", help="Generate SQL to fix mismatches")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"ERROR: Database not found: {args.db}")
        sys.exit(1)

    if not args.raw_dir.exists():
        print(f"ERROR: Raw directory not found: {args.raw_dir}")
        sys.exit(1)

    print(f"{'=' * 60}")
    print("Database & File Structure Validation")
    print(f"{'=' * 60}")
    print(f"  Database: {args.db}")
    print(f"  Raw dir:  {args.raw_dir}")
    print(f"  Sample:   {args.sample}")
    print(f"{'=' * 60}\n")

    conn = sqlite3.connect(str(args.db), timeout=60.0)
    conn.row_factory = sqlite3.Row

    # 1. Check what columns exist
    print("1. DATABASE SCHEMA CHECK")
    print("-" * 40)
    cursor = conn.execute("PRAGMA table_info(items)")
    columns = {row[1]: row[2] for row in cursor.fetchall()}

    required_cols = ["identifier", "text_filename", "downloaded_at"]
    triage_cols = [
        "triage_action",
        "triage_problems",
        "triage_alpha_ratio",
        "triage_lang",
        "triage_lang_confidence",
        "triage_at",
    ]

    missing_required = [c for c in required_cols if c not in columns]
    missing_triage = [c for c in triage_cols if c not in columns]

    if missing_required:
        print(f"  ❌ MISSING required columns: {missing_required}")
    else:
        print("  ✓ All required columns present")

    if missing_triage:
        print(f"  ⚠ MISSING triage columns: {missing_triage}")
        print("    Run: ALTER TABLE items ADD COLUMN <col> <type>")
    else:
        print("  ✓ All triage columns present")
    print()

    # 2. Check record counts
    print("2. RECORD COUNTS")
    print("-" * 40)
    total = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    downloaded = conn.execute(
        "SELECT COUNT(*) FROM items WHERE downloaded_at IS NOT NULL"
    ).fetchone()[0]
    with_filename = conn.execute(
        "SELECT COUNT(*) FROM items WHERE text_filename IS NOT NULL"
    ).fetchone()[0]
    downloaded_with_filename = conn.execute(
        "SELECT COUNT(*) FROM items WHERE downloaded_at IS NOT NULL AND text_filename IS NOT NULL"
    ).fetchone()[0]

    print(f"  Total records:           {total:,}")
    print(f"  Downloaded:              {downloaded:,}")
    print(f"  With text_filename:      {with_filename:,}")
    print(f"  Downloaded + filename:   {downloaded_with_filename:,}")

    if downloaded != downloaded_with_filename:
        print(
            f"  ⚠ {downloaded - downloaded_with_filename:,} downloaded records missing text_filename"
        )
    print()

    # 3. Check file naming patterns in DB
    print("3. FILE NAMING PATTERNS (DB)")
    print("-" * 40)
    patterns = conn.execute("""
        SELECT
            CASE
                WHEN text_filename LIKE '%_djvu.txt' THEN '_djvu.txt'
                WHEN text_filename LIKE '%.txt' THEN '.txt (other)'
                ELSE 'other'
            END as pattern,
            COUNT(*) as count
        FROM items
        WHERE text_filename IS NOT NULL
        GROUP BY pattern
    """).fetchall()

    for row in patterns:
        print(f"  {row['pattern']}: {row['count']:,}")
    print()

    # 4. Check actual file structure on disk
    print("4. FILE STRUCTURE ON DISK (sample)")
    print("-" * 40)

    # Sample some files from disk
    try:
        disk_files = list(args.raw_dir.iterdir())[: args.sample]
    except Exception as e:
        print(f"  ❌ Error reading directory: {e}")
        disk_files = []

    if disk_files:
        # Check if files are in subdirectories or flat
        flat_files = [f for f in disk_files if f.is_file()]
        subdirs = [f for f in disk_files if f.is_dir()]

        print(f"  Flat files: {len(flat_files)}")
        print(f"  Subdirectories: {len(subdirs)}")

        if flat_files:
            # Check naming pattern
            sample_names = [f.name for f in flat_files[:5]]
            print(f"  Sample filenames: {sample_names}")

            # Check if names match identifier pattern
            txt_files = [f for f in flat_files if f.suffix == ".txt"]
            if txt_files:
                sample_stem = txt_files[0].stem
                db_match = conn.execute(
                    "SELECT identifier, text_filename FROM items WHERE identifier = ?",
                    (sample_stem,),
                ).fetchone()

                if db_match:
                    print(f"\n  Matching DB record for '{txt_files[0].name}':")
                    print(f"    identifier:    {db_match['identifier']}")
                    print(f"    text_filename: {db_match['text_filename']}")

                    # Check the mismatch
                    expected_db = db_match["text_filename"]
                    actual_disk = txt_files[0].name

                    if expected_db != actual_disk:
                        print("\n  ⚠ NAMING MISMATCH DETECTED:")
                        print(f"    DB says:   {expected_db}")
                        print(f"    Disk has:  {actual_disk}")
                        print(
                            f"    → Files are {actual_disk.split('.')[-1]} on disk, DB expects {expected_db}"
                        )
    print()

    # 5. Cross-validate sample of DB records against disk
    print("5. DB ↔ DISK CROSS-VALIDATION")
    print("-" * 40)

    rows = conn.execute(
        "SELECT identifier, text_filename FROM items WHERE downloaded_at IS NOT NULL LIMIT ?",
        (args.sample,),
    ).fetchall()

    found_patterns = {
        "db_path_exists": 0,  # {raw_dir}/{identifier}/{text_filename}
        "flat_txt_exists": 0,  # {raw_dir}/{identifier}.txt
        "flat_dbname_exists": 0,  # {raw_dir}/{text_filename}
        "not_found": 0,
    }

    not_found_examples = []

    for row in rows:
        identifier = row["identifier"]
        text_filename = row["text_filename"]

        # Try different path patterns
        db_path = args.raw_dir / identifier / text_filename if text_filename else None
        flat_txt = args.raw_dir / f"{identifier}.txt"
        flat_dbname = args.raw_dir / text_filename if text_filename else None

        if db_path and db_path.exists():
            found_patterns["db_path_exists"] += 1
        elif flat_txt.exists():
            found_patterns["flat_txt_exists"] += 1
        elif flat_dbname and flat_dbname.exists():
            found_patterns["flat_dbname_exists"] += 1
        else:
            found_patterns["not_found"] += 1
            if len(not_found_examples) < 3:
                not_found_examples.append(identifier)

    print("  Pattern: {raw_dir}/{identifier}/{text_filename}")
    print(f"    Found: {found_patterns['db_path_exists']}/{args.sample}")

    print("  Pattern: {raw_dir}/{identifier}.txt")
    print(f"    Found: {found_patterns['flat_txt_exists']}/{args.sample}")

    print("  Pattern: {raw_dir}/{text_filename}")
    print(f"    Found: {found_patterns['flat_dbname_exists']}/{args.sample}")

    print(f"  Not found anywhere: {found_patterns['not_found']}/{args.sample}")

    if not_found_examples:
        print(f"    Examples: {not_found_examples}")

    # Determine recommended pattern
    best_pattern = max(found_patterns.items(), key=lambda x: x[1])
    print(f"\n  → RECOMMENDED PATTERN: {best_pattern[0]} ({best_pattern[1]}/{args.sample} matches)")
    print()

    # 6. Summary
    print("6. SUMMARY")
    print("-" * 40)

    issues = []
    if missing_required:
        issues.append(f"Missing required columns: {missing_required}")
    if missing_triage:
        issues.append(f"Missing triage columns (run ALTER TABLE): {missing_triage}")
    if found_patterns["not_found"] > args.sample * 0.1:
        issues.append(f">{10}% of files not found on disk")
    if found_patterns["db_path_exists"] == 0 and found_patterns["flat_txt_exists"] > 0:
        issues.append("Code assumes nested paths but files are FLAT - fix path building!")

    if issues:
        print("  ❌ ISSUES FOUND:")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print("  ✓ All checks passed!")

    conn.close()

    return 1 if issues else 0


if __name__ == "__main__":
    sys.exit(main())
