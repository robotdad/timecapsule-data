# TimeCapsule-Data Pipeline Baseline v1.0

**Date**: 2026-01-17
**Purpose**: Benchmark current capabilities before improvements

---

## Current Pipeline Components

### 1. Collectors

| Collector | Source | Method | Rate | Quality Assessment |
|-----------|--------|--------|------|---------------------|
| `tc-gutenberg` | Project Gutenberg | CSV catalog + direct download | ~1 req/s | HIGH - human proofread |
| `tc-perseus` | Perseus Digital Library | CTS API | ~0.5 req/s | HIGH - scholarly editions |
| `tc-ia` | Internet Archive | Search API + text fetch | **2s/req (slow)** | VARIABLE - OCR quality varies |

### 2. Post-Processing

| Tool | Function | Method |
|------|----------|--------|
| `tc-validate` | Temporal validation | Regex patterns for anachronisms |
| `tc-ocr-clean` | OCR error repair | Substitution patterns (60+ rules) |
| `tc-dedup` | Deduplication | **Exact MD5 hash only** |

### 3. Known Limitations (v1.0)

1. **IA Download Speed**: 2s delay = ~43,200 items/day max
2. **Deduplication**: Only exact matches - misses:
   - Same text with different OCR errors
   - Same text with different formatting
   - Near-duplicate passages
3. **Quality Filtering**: None - we download everything that passes temporal filter
4. **OCR Cleanup**: Pattern-based only - misses context-dependent errors

---

## Baseline Metrics

### Test Corpus: 82 Gutenberg + 2 fake duplicates

```
Exact duplicates detected:     2/2 (100%)
Fuzzy duplicates detected:     0/? (unknown - not implemented)
OCR errors found:              ~12 instances (0.0006% of words)
Quality issues flagged:        0 (no quality filter)
Temporal violations:           7 suspicious files
```

### Download Speed Benchmarks

| Source | Items | Time | Rate |
|--------|-------|------|------|
| Gutenberg (100 texts) | 100 | ~60s | 1.7/s |
| Perseus (catalog) | 3,461 | 3.7s | (catalog only) |
| IA (10 items) | 10 | >30s | <0.3/s |

---

## Comparison Rubric

After improvements, measure:

### A. Download Efficiency
- [ ] IA items/second (target: >1/s, currently ~0.3/s)
- [ ] Total corpus download time estimate

### B. Deduplication Quality
- [ ] Exact duplicate detection rate (baseline: 100%)
- [ ] Fuzzy duplicate detection rate (baseline: 0%)
- [ ] False positive rate (duplicates incorrectly flagged)

### C. Quality Filtering
- [ ] Low-quality documents filtered (baseline: 0)
- [ ] Quality score distribution of corpus
- [ ] Manual spot-check accuracy

### D. OCR Cleanup
- [ ] Error patterns caught (baseline: 60+ patterns)
- [ ] False positive rate (correct words "fixed")
- [ ] Reduction in corpus perplexity (if measurable)

### E. Overall Corpus Quality
- [ ] Final corpus size after all filtering
- [ ] Estimated unique content percentage
- [ ] Manual quality assessment (sample 50 docs)

---

## Test Datasets for Comparison

Will create standardized test sets:

1. **dedup_test/**: 100 Gutenberg texts + artificially degraded copies
2. **quality_test/**: Mix of good and intentionally bad documents  
3. **ocr_test/**: IA texts with known OCR issues

These allow apples-to-apples comparison before/after improvements.
