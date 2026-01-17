# TimeCapsule-Data v2.0 Improvement Plan

## Architecture Decision: Hybrid Approach

**Question**: Use external libraries vs our own code?

**Answer**: **Hybrid** - Use battle-tested libraries for complex algorithms, keep our own code for domain-specific logic.

```
┌─────────────────────────────────────────────────────────────────┐
│                    OUR CODE (domain-specific)                    │
├─────────────────────────────────────────────────────────────────┤
│  • Collectors (Gutenberg, Perseus, IA) - we know our sources    │
│  • Temporal validation - unique to our use case                 │
│  • OCR patterns - tuned for historical texts                    │
│  • Gutenberg boilerplate removal - source-specific              │
│  • Metadata management - our schema                             │
├─────────────────────────────────────────────────────────────────┤
│                 EXTERNAL LIBS (complex algorithms)               │
├─────────────────────────────────────────────────────────────────┤
│  • text-dedup: MinHash/SimHash fuzzy dedup (proven at scale)    │
│  • internetarchive: Official IA client (rate limit handling)    │
│  • fasttext: Language detection (trained model)                 │
│  • Quality filters: Port Gopher heuristics (simple, no lib)     │
└─────────────────────────────────────────────────────────────────┘
```

**Rationale**:
- MinHash is mathematically complex - don't reinvent it
- IA library handles their rate limits/retries correctly
- Language detection needs trained models
- Quality filters are simple heuristics - we implement ourselves

---

## Improvement Components

### 1. IA Collector → Official Library

**Current**: Custom requests with 2s hardcoded delay
**After**: `internetarchive` library with parallel downloads

```python
# Before (our code)
time.sleep(2.0)  # Conservative delay
response = requests.get(url)

# After (official library)
from internetarchive import search_items, download

# Search still works the same
results = search_items(query)

# Download with built-in rate handling + parallelism
download(identifier, concurrent=True, retries=3)
```

**Impact**: 3-6x faster downloads, proper retry logic

---

### 2. Fuzzy Deduplication with text-dedup

**Current**: MD5 exact hash only
**After**: MD5 + MinHash LSH for fuzzy matching

```python
# We ADD MinHash, keep our exact dedup
from text_dedup.minhash import MinHashDeduplicator

# Our pipeline becomes:
# 1. Exact hash dedup (fast, catches identical files)
# 2. MinHash dedup (catches near-duplicates, OCR variants)

deduplicator = MinHashDeduplicator(
    num_perm=128,       # Hash permutations
    threshold=0.8,      # 80% similarity = duplicate
    ngram_size=5,       # 5-gram shingles
)
```

**Why text-dedup over datatrove?**
- Simpler API for our use case
- Doesn't require Spark/distributed setup
- Works on local files directly
- datatrove is overkill (designed for Common Crawl scale)

---

### 3. Quality Filters (Our Implementation)

**Current**: None
**After**: Gopher-style heuristics (we implement ourselves)

These are simple rules from the Gopher/RefinedWeb papers - no library needed:

```python
class QualityFilter:
    """Gopher-style quality heuristics for historical texts."""
    
    def __init__(self):
        self.min_words = 50
        self.max_symbol_ratio = 0.1      # symbols / words
        self.max_repeated_line_ratio = 0.3
        self.min_mean_word_length = 3
        self.max_mean_word_length = 10
    
    def score(self, text: str) -> tuple[float, dict]:
        """Return (0-1 quality score, details dict)."""
        words = text.split()
        if len(words) < self.min_words:
            return 0.0, {"reason": "too_short"}
        
        # Check symbol ratio
        symbols = sum(1 for c in text if not c.isalnum() and not c.isspace())
        symbol_ratio = symbols / len(words)
        if symbol_ratio > self.max_symbol_ratio:
            return 0.0, {"reason": "high_symbol_ratio", "value": symbol_ratio}
        
        # Check repeated lines (OCR artifacts, headers)
        lines = text.split('\n')
        unique_lines = set(lines)
        if len(lines) > 0:
            repeat_ratio = 1 - (len(unique_lines) / len(lines))
            if repeat_ratio > self.max_repeated_line_ratio:
                return 0.0, {"reason": "repeated_lines", "value": repeat_ratio}
        
        # Mean word length (catches garbled OCR)
        mean_len = sum(len(w) for w in words) / len(words)
        if mean_len < self.min_mean_word_length or mean_len > self.max_mean_word_length:
            return 0.0, {"reason": "abnormal_word_length", "value": mean_len}
        
        # Passed all checks - compute quality score
        score = 1.0
        score -= symbol_ratio * 2  # Penalize symbols
        score -= repeat_ratio * 0.5  # Penalize repetition
        return max(0.0, score), {"passed": True}
```

**Why not use datatrove's GopherQualityFilter?**
- It's designed for web crawl data
- Our historical texts have different characteristics
- We want to tune thresholds for 19th century prose
- Simple enough to implement ourselves

---

### 4. Enhanced OCR Cleanup (Keep Ours + Add)

**Current**: 60+ substitution patterns
**After**: Same patterns + quality-based filtering + optional language verification

```python
# Our existing OCR patterns stay
from timecapsule_data.utils.ocr_cleanup import clean_text

# Add language verification (catches completely garbled text)
import fasttext
lang_model = fasttext.load_model('lid.176.bin')

def enhanced_ocr_pipeline(text: str) -> tuple[str, dict]:
    # 1. Our pattern-based cleanup
    cleaned, sub_count = clean_text(text)
    
    # 2. Language verification (is this even English/Greek/Latin?)
    lang, confidence = lang_model.predict(cleaned[:1000])
    if confidence < 0.5:
        return cleaned, {"warning": "low_language_confidence", "detected": lang}
    
    # 3. Quality check
    score, details = quality_filter.score(cleaned)
    
    return cleaned, {
        "substitutions": sub_count,
        "language": lang,
        "language_confidence": confidence,
        "quality_score": score,
        **details
    }
```

---

## Comparison Methodology

### Test Dataset Creation

```bash
# Create controlled test sets
mkdir -p tests/benchmark/{dedup,quality,ocr}

# 1. Dedup test: Real texts + artificial near-duplicates
# - Take 50 Gutenberg texts
# - Create variants: OCR-degraded, reformatted, partial
# - Ground truth: which pairs are "same work"

# 2. Quality test: Good + intentionally bad
# - 30 clean Gutenberg texts (label: good)
# - 20 artificially degraded (label: bad)
# - Ground truth labels

# 3. OCR test: IA texts with known issues
# - Download 50 IA texts with low quality scores
# - Manually review 10 for ground truth
```

### Metrics Collection Script

```python
# benchmark.py - run before and after improvements
def run_benchmark(corpus_path: str, version: str):
    results = {
        "version": version,
        "timestamp": datetime.now().isoformat(),
        
        # Dedup metrics
        "exact_dupes_found": run_exact_dedup(corpus_path),
        "fuzzy_dupes_found": run_fuzzy_dedup(corpus_path),  # 0 for v1
        
        # Quality metrics  
        "docs_filtered": run_quality_filter(corpus_path),
        "quality_score_distribution": get_quality_dist(corpus_path),
        
        # OCR metrics
        "patterns_matched": run_ocr_analysis(corpus_path),
        
        # Speed metrics
        "ia_download_rate": measure_ia_speed(10),
    }
    
    with open(f"benchmark_{version}.json", "w") as f:
        json.dump(results, f, indent=2)
```

### A/B Comparison

```bash
# 1. Run v1 benchmark
python benchmark.py ./test_corpus v1.0

# 2. Apply improvements
# ... code changes ...

# 3. Run v2 benchmark on SAME test corpus
python benchmark.py ./test_corpus v2.0

# 4. Generate comparison report
python compare_benchmarks.py benchmark_v1.0.json benchmark_v2.0.json
```

---

## Implementation Order

| Phase | Component | Dependency | Risk |
|-------|-----------|------------|------|
| 1 | Create benchmark test sets | None | Low |
| 2 | Run v1 baseline benchmark | Phase 1 | Low |
| 3 | Migrate IA to official lib | None | Low |
| 4 | Add quality filters | None | Low |
| 5 | Integrate MinHash dedup | `text-dedup` | Medium |
| 6 | Add language detection | `fasttext` | Low |
| 7 | Run v2 benchmark | Phases 3-6 | Low |
| 8 | Compare and document | Phase 7 | Low |

---

## Dependencies to Add

```toml
# pyproject.toml additions
dependencies = [
    # ... existing ...
    "internetarchive>=4.0.0",   # Official IA client
    "text-dedup>=0.3.0",        # MinHash deduplication
    "fasttext-wheel>=0.9.2",    # Language detection (wheel for easy install)
]
```

---

## Success Criteria

| Metric | v1 Baseline | v2 Target | Method |
|--------|-------------|-----------|--------|
| IA download speed | 0.3/s | >1.0/s | Official lib + parallel |
| Fuzzy dedup detection | 0% | >80% | MinHash with 0.8 threshold |
| Quality filtering | 0 docs | Filters garbage | Gopher heuristics |
| False positive rate | N/A | <5% | Manual review of flagged |

---

## Rollback Plan

All improvements are additive - original code paths remain:

```python
# User can choose pipeline version
tc-dedup --method exact      # v1 behavior
tc-dedup --method fuzzy      # v2 with MinHash
tc-dedup --method both       # v2 default: exact + fuzzy
```
