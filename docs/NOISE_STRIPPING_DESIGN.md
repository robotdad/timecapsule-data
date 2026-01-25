# Noise Stripping Post-Processing Design

This document describes the design for stripping OCR noise words (G: garbage, R: repeated categories) from cleaned corpus files.

## Overview

After OCR cleanup and vocabulary extraction, we have a list of words flagged as noise (categories G and R). These should be stripped from training data to improve LLM training quality.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Workflow                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. tc-ocr-clean batch     →  Cleaned files                     │
│  2. tc-ocr-vocab extract   →  _vocab_candidates.txt             │
│  3. tc-ocr-strip batch     →  Training-ready files (NEW)        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow

```
_vocab_candidates.txt          Cleaned corpus files
        │                              │
        ▼                              │
┌───────────────────┐                  │
│ Parse G/R words   │                  │
│ into HashSet      │                  │
└───────────────────┘                  │
        │                              │
        ▼                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  Rayon Parallel Processing                   │
│                                                              │
│  For each file:                                              │
│    1. Read content                                           │
│    2. Find word boundaries (regex)                           │
│    3. Replace noise words with "" or " "                     │
│    4. Collapse multiple spaces                               │
│    5. Write to output                                        │
│                                                              │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
  Training-ready files
```

## Rust Implementation

### New Functions in `lib.rs`

```rust
use std::collections::HashSet;
use std::sync::RwLock;

// Global noise word set (like the dictionary pattern)
lazy_static! {
    static ref NOISE_WORDS: RwLock<HashSet<String>> = RwLock::new(HashSet::new());
}

/// Initialize noise word set from vocab candidates file
/// Filters to only G (garbage) and R (repeated) categories
#[pyfunction]
fn init_noise_words(vocab_path: &str) -> PyResult<usize> {
    let content = std::fs::read_to_string(vocab_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(
            format!("Failed to read vocab file: {}", e)
        ))?;
    
    let mut words = HashSet::new();
    
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        
        // Parse format: FREQ | FLAGS | CAT | WORD | CONTEXT
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() >= 4 {
            let cat = parts[2].trim();
            let word = parts[3].trim();
            
            // Only include G (garbage) and R (repeated) categories
            if cat == "G" || cat == "R" {
                // Store lowercase for case-insensitive matching
                words.insert(word.to_lowercase());
            }
        }
    }
    
    let count = words.len();
    *NOISE_WORDS.write().unwrap() = words;
    Ok(count)
}

/// Strip noise words from text
/// Returns (cleaned_text, words_stripped)
#[pyfunction]
fn strip_noise_words(text: &str) -> (String, usize) {
    let noise_words = NOISE_WORDS.read().unwrap();
    if noise_words.is_empty() {
        return (text.to_string(), 0);
    }
    
    let word_re = Regex::new(r"\b([a-zA-Z][a-zA-Z']*[a-zA-Z]|[a-zA-Z])\b").unwrap();
    let mut result = String::with_capacity(text.len());
    let mut last_end = 0;
    let mut stripped = 0;
    
    for cap in word_re.captures_iter(text) {
        let m = cap.get(0).unwrap();
        let word = m.as_str();
        let word_lower = word.to_lowercase();
        
        // Copy text before this word
        result.push_str(&text[last_end..m.start()]);
        
        if noise_words.contains(&word_lower) {
            // Skip this word (replace with single space to avoid word collision)
            result.push(' ');
            stripped += 1;
        } else {
            // Keep this word
            result.push_str(word);
        }
        
        last_end = m.end();
    }
    
    // Copy remaining text
    result.push_str(&text[last_end..]);
    
    // Collapse multiple spaces
    let collapsed = MULTI_SPACE_RE.replace_all(&result, " ");
    
    (collapsed.to_string(), stripped)
}

/// Strip noise words from file
#[pyfunction]
fn strip_noise_file(input_path: &str, output_path: &str) -> PyResult<(bool, usize)> {
    let content = std::fs::read_to_string(input_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(
            format!("Failed to read file: {}", e)
        ))?;
    
    let (cleaned, stripped) = strip_noise_words(&content);
    
    if let Some(parent) = std::path::Path::new(output_path).parent() {
        std::fs::create_dir_all(parent).ok();
    }
    
    std::fs::write(output_path, &cleaned)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(
            format!("Failed to write file: {}", e)
        ))?;
    
    Ok((stripped > 0, stripped))
}

/// PyClass for batch statistics
#[pyclass]
#[derive(Clone)]
struct StripBatchStats {
    #[pyo3(get)]
    files_processed: u64,
    #[pyo3(get)]
    files_modified: u64,
    #[pyo3(get)]
    total_words_stripped: u64,
    #[pyo3(get)]
    total_bytes: u64,
}

/// Batch strip noise words with Rayon parallelization
#[pyfunction]
fn strip_noise_batch_parallel(
    file_paths: Vec<String>,
    output_dir: &str,
    num_threads: usize,
) -> PyResult<StripBatchStats> {
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    
    // Configure thread pool
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Failed to create thread pool: {}", e)
        ))?;
    
    let files_processed = AtomicU64::new(0);
    let files_modified = AtomicU64::new(0);
    let total_stripped = AtomicU64::new(0);
    let total_bytes = AtomicU64::new(0);
    
    let output_path = std::path::Path::new(output_dir);
    std::fs::create_dir_all(output_path).ok();
    
    pool.install(|| {
        file_paths.par_iter().for_each(|input_path| {
            let path = std::path::Path::new(input_path);
            
            // Read file
            let content = match std::fs::read_to_string(path) {
                Ok(c) => c,
                Err(_) => return,
            };
            
            total_bytes.fetch_add(content.len() as u64, Ordering::Relaxed);
            
            // Strip noise
            let (cleaned, stripped) = strip_noise_words(&content);
            
            // Write output
            let filename = path.file_name().unwrap();
            let output_file = output_path.join(filename);
            
            if std::fs::write(&output_file, &cleaned).is_ok() {
                files_processed.fetch_add(1, Ordering::Relaxed);
                if stripped > 0 {
                    files_modified.fetch_add(1, Ordering::Relaxed);
                    total_stripped.fetch_add(stripped as u64, Ordering::Relaxed);
                }
            }
        });
    });
    
    Ok(StripBatchStats {
        files_processed: files_processed.load(Ordering::Relaxed),
        files_modified: files_modified.load(Ordering::Relaxed),
        total_words_stripped: total_stripped.load(Ordering::Relaxed),
        total_bytes: total_bytes.load(Ordering::Relaxed),
    })
}
```

### Register in Module

```rust
// In rust_ocr_clean module registration:
m.add_function(wrap_pyfunction!(init_noise_words, m)?)?;
m.add_function(wrap_pyfunction!(strip_noise_words, m)?)?;
m.add_function(wrap_pyfunction!(strip_noise_file, m)?)?;
m.add_function(wrap_pyfunction!(strip_noise_batch_parallel, m)?)?;
m.add_class::<StripBatchStats>()?;
```

## Python CLI

### New file: `src/timecapsule_data/utils/ocr_strip.py`

```python
#!/usr/bin/env python3
"""
Strip OCR noise words from cleaned corpus files.

Removes words flagged as G (garbage) or R (repeated) category
from the vocabulary candidates file.

Usage:
    tc-ocr-strip batch ./cleaned -o ./training --vocab _vocab_candidates.txt
    tc-ocr-strip file input.txt -o output.txt --vocab _vocab_candidates.txt
"""

import argparse
import sys
import time
from pathlib import Path


def cmd_batch(args):
    """Strip noise words from batch of files."""
    import rust_ocr_clean
    
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir) if args.output_dir else input_dir.parent / "training"
    vocab_path = Path(args.vocab)
    
    # Initialize noise word set
    print(f"Loading noise words from: {vocab_path}", file=sys.stderr)
    noise_count = rust_ocr_clean.init_noise_words(str(vocab_path))
    print(f"Loaded {noise_count:,} noise words (G + R categories)", file=sys.stderr)
    
    # Find all files
    files = list(input_dir.rglob("*.txt"))
    print(f"Found {len(files):,} files to process", file=sys.stderr)
    
    # Process
    start = time.time()
    stats = rust_ocr_clean.strip_noise_batch_parallel(
        [str(f) for f in files],
        str(output_dir),
        args.threads or 24,
    )
    elapsed = time.time() - start
    
    # Report
    print(f"\n{'=' * 60}", file=sys.stderr)
    print("COMPLETE", file=sys.stderr)
    print(f"{'=' * 60}", file=sys.stderr)
    print(f"  Files processed: {stats.files_processed:,}", file=sys.stderr)
    print(f"  Files modified:  {stats.files_modified:,}", file=sys.stderr)
    print(f"  Words stripped:  {stats.total_words_stripped:,}", file=sys.stderr)
    print(f"  Time elapsed:    {elapsed:.1f}s", file=sys.stderr)
    print(f"  Throughput:      {stats.files_processed / elapsed:.1f} files/s", file=sys.stderr)
    print(f"{'=' * 60}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Strip OCR noise words from corpus",
        epilog="""
Examples:
  tc-ocr-strip batch ./cleaned -o ./training --vocab _vocab_candidates.txt
  tc-ocr-strip file doc.txt -o doc_clean.txt --vocab _vocab_candidates.txt
        """,
    )
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Batch command
    batch_parser = subparsers.add_parser("batch", help="Process directory of files")
    batch_parser.add_argument("input_dir", help="Input directory")
    batch_parser.add_argument("-o", "--output-dir", help="Output directory")
    batch_parser.add_argument("--vocab", required=True, help="Vocab candidates file")
    batch_parser.add_argument("--threads", type=int, default=24, help="Thread count")
    
    args = parser.parse_args()
    
    if args.command == "batch":
        cmd_batch(args)


if __name__ == "__main__":
    main()
```

### Register CLI entry point in `pyproject.toml`

```toml
[project.scripts]
tc-ocr-strip = "timecapsule_data.utils.ocr_strip:main"
```

## Usage

```bash
# After tc-ocr-clean and tc-ocr-vocab:

# Strip G and R category words
tc-ocr-strip batch /mnt/e/datasets/timecapsule-prewwi/cleaned/ia \
    -o /mnt/e/datasets/timecapsule-prewwi/training/ia \
    --vocab /mnt/e/datasets/timecapsule-prewwi/cleaned/_vocab_candidates_2.txt

# The output directory now contains training-ready files with:
# - All OCR pattern fixes applied (from tc-ocr-clean)
# - Boilerplate stripped
# - G/R noise words removed
```

## Performance Expectations

Based on existing Rayon batch operations:
- ~50-60 files/s throughput
- 80,000 files in ~25 minutes
- Low memory footprint (streaming, no full corpus in memory)

## Categories Stripped

| Category | Action | Rationale |
|----------|--------|-----------|
| **G** (garbage) | Strip | Zero semantic content |
| **R** (repeated) | Strip | Character stuttering |
| **M** (mixed_case) | Keep | Real words, context provides signal |
| **C** (confusable) | Keep | Only 0.1%, tokenizer handles |
| **F** (fragment) | Keep | Very rare (9 words) |
| **X** (modern) | Keep | Very rare (7 words) |

## Future Enhancements

1. **Category flags**: `--strip G,R,F` to customize which categories
2. **In-place mode**: `--in-place` to modify files directly
3. **Dry-run mode**: `--dry-run` to report what would be stripped
4. **Statistics file**: Output JSON report of stripping stats per file
