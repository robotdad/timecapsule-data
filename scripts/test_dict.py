#!/usr/bin/env python3
"""Quick test to verify dictionary loading works."""

import rust_ocr_clean

rust_ocr_clean.init_dictionaries("rust-ocr-clean/dictionaries")
print(f"Dictionaries loaded: {rust_ocr_clean.dictionaries_loaded()}")
print(f"is_known_word('one'): {rust_ocr_clean.is_known_word('one')}")
print(f"is_known_word('there'): {rust_ocr_clean.is_known_word('there')}")
print(f"is_known_word('xyzgarbage'): {rust_ocr_clean.is_known_word('xyzgarbage')}")
