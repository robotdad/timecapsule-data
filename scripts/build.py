#!/usr/bin/env python3
"""
Cross-platform build script for timecapsule-data.

Handles:
- git pull
- uv sync --reinstall-package rust-ocr-clean (forces rebuild when Rust changes)
- Verification that patterns work

Usage:
    uv run scripts/build.py          # Full rebuild
    uv run scripts/build.py --quick  # Skip reinstall (use existing)
    uv run scripts/build.py --verify # Just verify current install works
"""

import subprocess
import sys
from pathlib import Path


def run(cmd: list[str], cwd: Path | None = None, check: bool = True) -> bool:
    """Run a command, return True if successful."""
    print(f"  → {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd)
    if check and result.returncode != 0:
        print(f"  ✗ Command failed with code {result.returncode}")
        return False
    return result.returncode == 0


def verify_patterns() -> bool:
    """Verify that OCR patterns are working."""
    print("\n[3/3] Verifying patterns...")
    try:
        from rust_ocr_clean import rust_ocr_clean

        tests = [
            ("OFTHE", "of the"),
            ("oFthe", "of the"),
            ("fymptoms", "symptoms"),
            ("Majefty's", "majesty's"),
        ]

        all_passed = True
        for input_text, expected_output in tests:
            result = rust_ocr_clean.clean_text(input_text)
            cleaned, count = result[0], result[1]

            if cleaned != expected_output:
                print(f"  ✗ '{input_text}' -> '{cleaned}' (expected '{expected_output}')")
                all_passed = False
            elif count == 0:
                print(f"  ✗ '{input_text}' -> no substitutions made")
                all_passed = False
            else:
                print(f"  ✓ '{input_text}' -> '{cleaned}'")

        return all_passed

    except ImportError as e:
        print(f"  ✗ Failed to import rust_ocr_clean: {e}")
        return False
    except Exception as e:
        print(f"  ✗ Verification failed: {e}")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Build timecapsule-data")
    parser.add_argument("--quick", action="store_true", help="Skip reinstall")
    parser.add_argument("--verify", action="store_true", help="Just verify, don't build")
    parser.add_argument("--no-pull", action="store_true", help="Skip git pull")
    args = parser.parse_args()

    root = Path(__file__).parent.parent

    if args.verify:
        success = verify_patterns()
        sys.exit(0 if success else 1)

    print("=" * 60)
    print("Building timecapsule-data")
    print("=" * 60)

    # Step 1: Git pull
    if not args.no_pull:
        print("\n[1/3] Pulling latest code...")
        if not run(["git", "pull"], cwd=root):
            sys.exit(1)
    else:
        print("\n[1/3] Skipping git pull (--no-pull)")

    # Step 2: Sync with uv (rebuilds rust-ocr-clean if source changed)
    print("\n[2/3] Syncing environment...")
    if args.quick:
        if not run(["uv", "sync"], cwd=root):
            print("\n✗ Sync failed!")
            sys.exit(1)
    else:
        # Force reinstall of rust-ocr-clean to ensure fresh build
        if not run(["uv", "sync", "--reinstall-package", "rust-ocr-clean"], cwd=root):
            print("\n✗ Sync failed!")
            sys.exit(1)

    # Step 3: Verify
    if verify_patterns():
        print("\n" + "=" * 60)
        print("✓ Build successful - all patterns verified")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("✗ Build completed but verification FAILED")
        print("  Patterns are not working correctly!")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
