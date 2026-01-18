# OCR Cleaning Pipeline

This document describes the tiered approach to cleaning OCR errors in the TimeCapsule corpus.

## The Problem

Historical documents digitized via OCR (Optical Character Recognition) contain various errors:

- **Character substitutions**: "tbe" → "the", "liave" → "have"
- **Word fragments**: "ing", "tion" split from parent words
- **Garbage text**: Random character sequences from poor scans
- **Missing/extra characters**: "th" → "the", "thhe" → "the"

These errors degrade the quality of text for LLM training. However, not all documents need the same level of cleanup - some have excellent OCR, others are severely corrupted.

## Tiered Approach

We use a tiered strategy to balance quality vs computational cost:

```
┌─────────────────────────────────────────────────────────────────┐
│  TIER 0: Scoring & Triage (tc-ocr-score)                        │
│  Fast dictionary-based scoring to identify problem files        │
│  Score = % unknown words + 2×(% garbage patterns)               │
└─────────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
            Score < 0.10          Score >= 0.10
            (Good/Moderate)       (Poor/Garbage)
                    │                   │
                    ▼                   ▼
┌───────────────────────────┐   ┌───────────────────────────────────┐
│ TIER 1: Basic Cleanup     │   │ TIER 2: SymSpell Correction       │
│ tc-ocr-clean (patterns)   │   │ tc-ocr-symspell (dictionary)      │
│ ~40 common substitutions  │   │ Edit distance corrections         │
└───────────────────────────┘   └───────────────────────────────────┘
                                        │
                              Still bad (Score >= 0.20)?
                                        │
                                        ▼
                    ┌───────────────────────────────────────────┐
                    │ TIER 3: LLM Correction (future)           │
                    │ Llama-3.1-70B via vLLM                    │
                    │ Only for worst ~5% of corpus              │
                    │ See: LLM_OCR_CORRECTION_RESEARCH.md       │
                    └───────────────────────────────────────────┘
```

---

## Tools

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

**How scoring works:**
1. Extract all words from text
2. Check each word against English dictionary (pyenchant)
3. Detect "garbage" patterns (long consonant runs, repeated chars)
4. Calculate: `score = unknown_rate + 2 × garbage_rate`

### tc-ocr-clean - Pattern Replacement

Simple pattern-based cleanup for common OCR substitutions.

```bash
tc-ocr-clean input.txt -o output.txt
tc-ocr-clean batch ./corpus -o ./cleaned
```

Handles ~40 common patterns like:
- `tbe` → `the`
- `liave` → `have`
- `wliich` → `which`

**Limitation**: Only fixes known patterns. Cannot handle novel errors.

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

**How it works:**
1. Load 82,000+ word frequency dictionary
2. For each word, find closest dictionary match within edit distance 2
3. Apply correction if match has high frequency
4. Preserve case (THE → THE, the → the, The → The)

**Limitations:**
- Cannot fix severely corrupted words (edit distance > 2)
- May incorrectly "fix" proper nouns, place names, historical terms
- Word fragments (split words) remain unfixed
- Conservative to avoid false positives

---

## Recommended Workflow

### 1. Score the corpus

```bash
tc-ocr-score analyze /path/to/corpus --report quality_report.json
```

Review the quality distribution to understand your corpus.

### 2. Separate by quality tier

```bash
# Good files (minimal cleanup)
tc-ocr-score filter /path/to/corpus --threshold 0.05 \
    --output-good ./tier_good

# Moderate files (basic cleanup)
tc-ocr-score filter /path/to/corpus --threshold 0.10 \
    --output-good ./tier_moderate --output-bad ./tier_needs_work

# etc.
```

### 3. Apply appropriate cleanup

```bash
# Tier 1: Basic pattern cleanup for moderate files
tc-ocr-clean batch ./tier_moderate -o ./tier_moderate_clean

# Tier 2: SymSpell for poor files
tc-ocr-symspell batch ./tier_needs_work -o ./tier_symspell_clean \
    --report symspell_stats.json
```

### 4. Re-score and verify

```bash
# Check if cleanup improved quality
tc-ocr-score analyze ./tier_symspell_clean --report post_cleanup.json
```

### 5. Handle remaining garbage

Files still scoring >= 0.20 after SymSpell are candidates for:
- Tier 3 LLM correction (see `LLM_OCR_CORRECTION_RESEARCH.md`)
- Manual review
- Exclusion from training corpus

---

## Known Issues & Limitations

### False Positives in SymSpell

SymSpell may incorrectly "correct":
- **Proper nouns**: "Taggart" → "Haggard", "Sunbury" → "Sudbury"
- **Historical terms**: "connexion" → "connection" 
- **Brand names**: "Ripans" → "Ripens" (Ripans Tabules was a medicine)
- **Technical terms**: Domain-specific vocabulary

We mitigate this by:
- Requiring high frequency for suggestions (>1000 occurrences)
- Skipping short words and acronyms
- Skipping common word fragments
- Using conservative edit distance thresholds

### Word Fragments

OCR often splits words across line breaks or incorrectly:
- "ap- propriate" → "ap" + "propriate"
- "associa- tion" → "associa" + "tion"

These fragments are detected but not corrected by current tools.
They require context-aware LLM correction (Tier 3).

### Garbage Text

Severely corrupted text (score >= 0.35) often cannot be recovered:
- Missing pages from scans
- Heavily damaged original documents
- Non-text content (tables, images) OCR'd incorrectly

Consider excluding these from training corpus.

---

## Dependencies

```bash
# Python packages
pip install symspellpy pyenchant

# Optional (for NLTK fallback if pyenchant unavailable)
pip install nltk
```

**System packages** (for pyenchant):
- Ubuntu/Debian: `libenchant-2-dev`
- macOS: `enchant` (via Homebrew)

---

## Future Work

1. **Tier 3 LLM Correction**: See `LLM_OCR_CORRECTION_RESEARCH.md`
2. **Custom vocabulary**: Add corpus-derived vocabulary to reduce false positives
3. **Word fragment joining**: Detect and rejoin split words
4. **Confidence scoring**: Provide per-correction confidence for review
