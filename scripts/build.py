#!/usr/bin/env python3
"""
Cross-platform build script for timecapsule-data.

Handles:
- git pull
- cargo clean + maturin build (Rust module)
- pip install -e . (Python package)
- Verification that patterns work

Usage:
    uv run scripts/build.py          # Full rebuild
    uv run scripts/build.py --quick  # Skip cargo clean (faster, use if just Python changed)
    uv run scripts/build.py --verify # Just verify current install works
"""

import subprocess
import sys
from pathlib import Path


def run(cmd: list[str], cwd: Path | None = None, check: bool = True) -> bool:
    """Run a command, return True if successful."""
    print(f"  → {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=False)
    if check and result.returncode != 0:
        print(f"  ✗ Command failed with code {result.returncode}")
        return False
    return result.returncode == 0


def verify_patterns() -> bool:
    """Verify that OCR patterns are working."""
    print("\n[4/4] Verifying patterns...")
    try:
        import rust_ocr_clean

        tests = [
            ("OFTHE", "of the", "word_runtogether"),
            ("oFthe", "of the", "word_runtogether"),
            ("fymptoms", "symptoms", "long_s"),
            ("Majefty's", "majesty's", "long_s"),
        ]

        all_passed = True
        for input_text, expected_output, expected_category in tests:
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
    parser.add_argument("--quick", action="store_true", help="Skip cargo clean")
    parser.add_argument("--verify", action="store_true", help="Just verify, don't build")
    parser.add_argument("--no-pull", action="store_true", help="Skip git pull")
    args = parser.parse_args()

    root = Path(__file__).parent.parent
    rust_dir = root / "rust-ocr-clean"

    if args.verify:
        success = verify_patterns()
        sys.exit(0 if success else 1)

    print("=" * 60)
    print("Building timecapsule-data")
    print("=" * 60)

    # Step 1: Git pull
    if not args.no_pull:
        print("\n[1/4] Pulling latest code...")
        if not run(["git", "pull"], cwd=root):
            sys.exit(1)
    else:
        print("\n[1/4] Skipping git pull (--no-pull)")

    # Step 2: Clean and build Rust module
    if not args.quick:
        print("\n[2/4] Cleaning Rust build cache...")
        run(["cargo", "clean"], cwd=rust_dir, check=False)

    print("\n[3/4] Building Rust module...")
    if not run(["maturin", "develop", "--release"], cwd=rust_dir):
        print("\n✗ Rust build failed!")
        sys.exit(1)

    # Step 3: Install Python package
    print("\n[3/4] Installing Python package...")
    if not run(["uv", "pip", "install", "-e", "."], cwd=root):
        print("\n✗ Python install failed!")
        sys.exit(1)

    # Step 4: Verify
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
