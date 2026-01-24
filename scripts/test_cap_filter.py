#!/usr/bin/env python3
"""Test that capitalized English words are filtered from vocab extraction."""

import os
import tempfile

import rust_ocr_clean

rust_ocr_clean.init_dictionaries("rust-ocr-clean/dictionaries")

test_text = "One day There was a Time when Hello world. The quick Brown fox jumps."

with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
    f.write(test_text)
    tmp = f.name

count, results = rust_ocr_clean.extract_vocab_batch([tmp], 40)
os.unlink(tmp)

print(f"Input: {test_text}")
print(f"Total words counted: {count}")
print(f"Candidates returned: {len(results)}")
print("Words in output:")
for word_lower, (word, cnt, is_cap, is_susp, reason, ctx) in sorted(results.items()):
    print(f"  {word}")

# Check if filter is working
cap_words = ["One", "There", "Time", "Hello", "The", "Brown"]
leaked = [w for w in cap_words if w.lower() in results]
if leaked:
    print(f"\nFAIL: Capitalized words leaked through: {leaked}")
    print("The Rust module needs rebuilding!")
else:
    print("\nPASS: All capitalized English words were filtered out")
