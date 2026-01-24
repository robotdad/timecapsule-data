# OCR Cleaning Pipeline

This document describes the tiered approach to cleaning OCR errors in the TimeCapsule corpus.

## The Problem

Historical documents digitized via OCR (Optical Character Recognition) contain various errors:

- **Character substitutions**: "tbe" → "the", "liave" → "have"
- **Word fragments**: "ing", "tion" split from parent words
- **Garbage text**: Random character sequences from poor scans
- **Missing/extra characters**: "th" → "the", "thhe" → "the"
- **Encoding errors**: Mojibake like "Ã©" instead of "é"
- **Non-English content**: Documents in other languages mixed in

These errors degrade the quality of text for LLM training. However, not all documents need the same level of cleanup - some have excellent OCR, others are severely corrupted.

## Pipeline Overview

The OCR cleanup pipeline runs entirely in Rust for performance at scale (2M+ documents):

```
┌─────────────────────────────────────────────────────────────────────┐
│  PREPROCESSING (Rust)                                               │
│  1. Unicode normalization - Fix mojibake, HTML entities, encoding   │
│  2. Language detection - Reject non-English (whatlang library)      │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  DOCUMENT TRIAGE (tc-doc-triage, Rust)                              │
│  Classify documents based on quality signals:                       │
│  - alpha_ratio: % alphabetic characters                             │
│  - line_length_cv: coefficient of variation in line lengths         │
│  - fragment_ratio: short incomplete lines                           │
│  - list_pattern_ratio: index/catalog patterns                       │
│  Actions: process | review | reject                                 │
└─────────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
           process         review          reject
              │               │               │
              ▼               ▼               ▼
┌─────────────────────┐ ┌───────────┐ ┌─────────────────┐
│ BOILERPLATE STRIP   │ │ Human     │ │ Logged to       │
│ Remove digitization │ │ Review    │ │ rejected.jsonl  │
│ headers/footers     │ │ Queue     │ │ (not processed) │
└─────────────────────┘ └───────────┘ └─────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TIER 1: OCR Cleanup (tc-ocr-clean, Rust)                           │
│  150+ patterns for character-level fixes (~35x speedup)             │
│  - Long-s artifacts, li/h confusion, ll→U errors                    │
│  - Watermark removal (Google, IA)                                   │
└─────────────────────────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TIER 2: SymSpell Correction (for poor quality)                     │
│  tc-ocr-symspell - Dictionary-based edit distance corrections       │
└─────────────────────────────────────────────────────────────────────┘
              │
              ▼ (Still bad? Score >= 0.20)
┌─────────────────────────────────────────────────────────────────────┐
│  TIER 3: LLM Correction (future)                                    │
│  Llama-3.1-70B via vLLM - Only for worst ~5% of corpus              │
│  See: LLM_OCR_CORRECTION_RESEARCH.md                                │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Preprocessing (Rust)

Before any OCR pattern matching, the pipeline applies preprocessing in Rust:

### Unicode Normalization

Fixes common encoding issues that corrupt text:

| Issue | Example | Fixed |
|-------|---------|-------|
| Mojibake (UTF-8 as Latin-1) | "Ã©" | "é" |
| HTML entities | "&amp;" | "&" |
| Double-encoded entities | "&amp;amp;" | "&" |
| Unicode whitespace | Non-breaking spaces | Regular spaces |
| BOM markers | Zero-width chars | Removed |

**Implementation**: Rust `unicode-normalization` crate (NFC) plus custom mojibake patterns.

### Language Detection

Rejects non-English documents to maintain corpus coherence:

| Language | Action | Logged To |
|----------|--------|-----------|
| English (confidence ≥ 0.5) | Process | - |
| Non-English | Reject | `rejected_files.jsonl` |
| Unknown/too short | Assume English | - |

**Implementation**: Rust `whatlang` library (no external model files needed).

---

## Tools

### tc-doc-triage - Document Classification

Classifies documents based on structural quality signals before OCR cleanup.

```bash
# Triage a directory
tc-doc-triage /path/to/corpus -o triage_results.jsonl

# Triage with custom thresholds
tc-doc-triage /path/to/corpus -o results.jsonl --alpha-min 0.7
```

**Triage Actions:**

| Action | Criteria | Handling |
|--------|----------|----------|
| `process` | Good quality signals | Proceed to OCR cleanup |
| `review` | Ambiguous quality | Queue for human review |
| `reject` | Poor quality / catalogs / non-English | Log and skip |

**Quality Signals:**

| Signal | Description | Reject If |
|--------|-------------|-----------|
| `alpha_ratio` | % alphabetic characters | < 0.6 |
| `line_length_cv` | Line length variation | > 1.5 (multicolumn) |
| `fragment_ratio` | Short incomplete lines | > 0.4 |
| `list_pattern_ratio` | Index/catalog patterns | > 0.3 |
| `is_english` | Language detection | False |

### tc-ocr-clean - Pattern Replacement

Rust-powered OCR cleanup with 150+ patterns, including boilerplate stripping.

```bash
# Clean a single file
tc-ocr-clean clean input.txt -o output.txt

# Batch process (includes triage + boilerplate stripping by default)
tc-ocr-clean batch ./corpus -o ./cleaned

# Skip triage (if already done separately)
tc-ocr-clean batch ./corpus -o ./cleaned --skip-triage

# Skip boilerplate stripping
tc-ocr-clean batch ./corpus -o ./cleaned --skip-boilerplate

# Strip boilerplate only (standalone command)
tc-ocr-clean strip-boilerplate ./corpus -o ./stripped --log boilerplate_audit.jsonl
```

**Boilerplate Patterns Detected:**

| Source | Patterns |
|--------|----------|
| Google Books | "Digitized by Google", usage guidelines, watermarks (including OCR-damaged: "OOglC", "byGoogle") |
| Internet Archive | Archive notices, "Generated by" lines, URL footers |
| JSTOR | "Early Journal Content" blocks |
| Project Gutenberg | Start/end license blocks |
| HathiTrust | Public domain notices |

Boilerplate stripping logs to `_boilerplate_stripped.jsonl` for auditing what was removed.

**Pattern Categories:**

| Category | Examples | Count |
|----------|----------|-------|
| Long-s artifacts | fuch→such, faid→said | ~50 |
| li/h confusion | tlie→the, wliich→which | ~40 |
| ll→U confusion | wiU→will, pubUc→public | ~75 |
| rn/m confusion | tirne→time, frorn→from | ~10 |
| Google watermarks | "Digitized by Google" | ~10 |

**Performance**: ~35x faster than Python (~14 MB/s on NVMe).

### tc-ocr-score - Quality Scoring

Scores files based on dictionary word recognition using pyenchant.

```bash
# Score a single file
tc-ocr-score check document.txt

# Analyze entire corpus
tc-ocr-score analyze ./corpus --report quality_report.json

# Filter corpus by quality
tc-ocr-score filter ./corpus --threshold 0.10 \
    --output-good ./good --output-bad ./needs_cleanup
```

**Quality Tiers:**

| Tier | Score | Description |
|------|-------|-------------|
| GOOD | < 0.05 | Minimal errors, ready to use |
| MODERATE | < 0.10 | Some errors, basic cleanup sufficient |
| POOR | < 0.20 | Many errors, needs SymSpell |
| GARBAGE | >= 0.20 | Severe corruption, may need LLM or discard |

### tc-ocr-symspell - Dictionary Spell Correction

Uses SymSpell algorithm for fast, dictionary-based correction.

```bash
# Analyze what would be corrected (dry run)
tc-ocr-symspell analyze document.txt

# Clean a file
tc-ocr-symspell clean document.txt -o cleaned.txt

# Batch process
tc-ocr-symspell batch ./corpus -o ./cleaned --report stats.json
```

**Limitations:**
- Cannot fix severely corrupted words (edit distance > 2)
- May incorrectly "fix" proper nouns, place names, historical terms
- Word fragments (split words) remain unfixed

### tc-ocr-vocab - Vocabulary Extraction

Extract vocabulary from cleaned corpus for analysis or whitelist building.

```bash
# Extract vocabulary with minimum frequency
tc-ocr-vocab /path/to/cleaned -o vocab.json --min-freq 5
```

**Use cases:**
- Identify additional OCR patterns to add
- Build whitelist for spell-checking
- Analyze corpus quality

---

## Recommended Workflow

### 1. Triage documents (optional, included in batch)

```bash
tc-doc-triage /path/to/corpus -o triage_results.jsonl
```

Review `triage_results.jsonl` to understand corpus quality distribution.

### 2. Run OCR cleanup

```bash
# Full pipeline (triage + preprocess + OCR patterns)
tc-ocr-clean batch /path/to/raw -o /path/to/cleaned
```

This will:
- Apply unicode normalization (Rust)
- Detect and reject non-English documents (Rust)
- Run document triage (Rust)
- Apply 150+ OCR patterns (Rust)
- Log rejected files to `rejected_files.jsonl`

### 3. Score cleaned corpus

```bash
tc-ocr-score analyze /path/to/cleaned --report quality_report.json
```

### 4. Apply SymSpell to poor quality files

```bash
tc-ocr-symspell batch /path/to/poor_quality -o /path/to/symspell_cleaned
```

### 5. Extract vocabulary (optional)

```bash
tc-ocr-vocab /path/to/cleaned -o vocab.json --min-freq 5
```

---

## Output Files

| File | Description |
|------|-------------|
| `cleaned/*.txt` | Processed text files |
| `_cleanup_report.json` | Processing statistics with metadata |
| `_triage_results.jsonl` | Detailed triage decisions and signals |
| `_boilerplate_stripped.jsonl` | Audit log of removed boilerplate sections |
| `rejected_files.jsonl` | Files rejected (non-English, garbage, catalogs) |

**rejected_files.jsonl format:**

```json
{"path": "file.txt", "reason": "non_english", "lang": "fra", "confidence": 0.95}
{"path": "file2.txt", "reason": "low_alpha_ratio", "alpha_ratio": 0.45}
{"path": "file3.txt", "reason": "catalog_index", "list_pattern_ratio": 0.52}
```

---

## Known Issues & Limitations

### False Positives in SymSpell

SymSpell may incorrectly "correct":
- **Proper nouns**: "Taggart" → "Haggard", "Sunbury" → "Sudbury"
- **Historical terms**: "connexion" → "connection" 
- **Brand names**: "Ripans" → "Ripens" (Ripans Tabules was a medicine)

We mitigate this with vocabulary whitelists and conservative thresholds.

### Multi-Column Text

Documents with multi-column layouts (newspapers, some books) produce garbled text when OCR'd line-by-line. These are detected by high `line_length_cv` and flagged for review or rejection.

**Future work**: Column detection and reordering.

### Word Fragments

OCR often splits words across line breaks:
- "ap- propriate" → "ap" + "propriate"

Hyphen rejoining handles most cases, but some fragments remain.

---

## Dependencies

**Rust crates** (in rust-ocr-clean):
- `whatlang` - Language detection
- `unicode-normalization` - NFC normalization
- `regex` - Pattern matching
- `pyo3` - Python bindings

**Python packages:**
- `symspellpy` - Spell correction
- `pyenchant` - Dictionary lookups

**System packages** (for pyenchant):
- Ubuntu/Debian: `libenchant-2-dev`
- macOS: `enchant` (via Homebrew)

---

## Performance

Benchmarked on Ryzen 5950X, NVMe storage:

| Component | Language | Throughput |
|-----------|----------|------------|
| Preprocessing | Rust | ~20 MB/s |
| OCR patterns | Rust | ~14 MB/s |
| Scoring | Python (C-backed) | ~5 MB/s |
| SymSpell | Python | ~2 MB/s |

For 2M+ documents, the Rust components are critical for reasonable processing times.

---

## Future Work

1. **Tier 3 LLM Correction**: See `LLM_OCR_CORRECTION_RESEARCH.md`
2. **Multi-column detection**: Reorder columns before OCR cleanup
3. **Custom vocabulary expansion**: Corpus-derived vocabulary
4. **Perplexity scoring**: KenLM as complement to dictionary scoring
