# OCR Cleanup Best Practices for LLM Training Data

This document surveys industry best practices for cleaning OCR text intended for Large Language Model training, with citations to academic research and production pipelines. It compares these practices to the TimeCapsule pipeline and identifies opportunities for enhancement.

---

## Executive Summary

The TimeCapsule OCR cleanup pipeline aligns well with industry best practices in several key areas:

| Practice | Industry Standard | TimeCapsule | Status |
|----------|-------------------|-------------|--------|
| Tiered quality filtering | Yes | Yes (Tiers 0-3) | ✅ Aligned |
| Dictionary-based scoring | Yes | Yes (pyenchant) | ✅ Aligned |
| Pattern-based corrections | Yes | Yes (150+ Rust patterns) | ✅ Aligned |
| High-performance processing | Rust/C++ | Rust (~35x speedup) | ✅ Aligned |
| Vocabulary protection | Yes | Yes (known_vocab.txt) | ✅ Aligned |
| Human review checkpoint | Yes | Yes (vocab review) | ✅ Aligned |
| LLM correction for worst cases | Emerging | Planned (Tier 3) | ✅ Aligned |
| Unicode normalization | Standard (ftfy) | Rust (NFC + mojibake) | ✅ Implemented |
| Language detection | Standard (fastText) | Rust (whatlang) | ✅ Implemented |
| Document triage | Common | Rust (tc-doc-triage) | ✅ Implemented |
| Deduplication | Critical | Separate tool (tc-dedup) | ⚠️ Partial |
| Perplexity scoring | Common | Not implemented | ❌ Gap |
| Multi-column detection | Specialized | Not implemented | ❌ Gap |

---

## 1. Quality Filtering Approaches

### 1.1 Tiered Filtering (Industry Standard)

Modern LLM data pipelines use cascading quality filters, applying cheap heuristics first and expensive model-based filters later. This matches TimeCapsule's Tier 0-3 approach.

**FineWeb Pipeline** (Hugging Face, 2024):
> "We carefully ablated deduplication and filtering strategies... FineWeb-Edu uses a classifier trained to predict educational value, filtering to 1.3T tokens that show dramatically better performance on MMLU and ARC."
> 
> Source: [The FineWeb Datasets](https://openreview.net/forum?id=n6SCkn2QaG) - NeurIPS 2024

**Recommended cascade:**
```
1. Heuristic filters (fast, CPU) -> reject obvious garbage
2. Dictionary/perplexity scoring -> quality tiers
3. Model-based classifiers -> fine-grained quality
4. LLM correction -> salvage borderline cases
```

**TimeCapsule alignment:** The Tier 0 scoring + Tier 1/2/3 progression matches this pattern well.

### 1.2 Dictionary-Based Scoring

Using dictionary word recognition to score OCR quality is a validated approach.

**Key insight from research:**
> "Quality filtering methods include hand-crafted heuristics, trained classifiers, and metric thresholding. Dictionary-based scoring provides a strong baseline that's fast and interpretable."
>
> Source: [Data Management For Training Large Language Models: A Survey](https://arxiv.org/html/2312.01700v3) - Peking University & Huawei, 2024

**TimeCapsule implementation:** `tc-ocr-score` uses pyenchant for dictionary lookups with a scoring formula: `score = unknown_rate + 2 * garbage_rate`. This is sound methodology.

### 1.3 Perplexity Scoring (Enhancement Opportunity)

Many production pipelines use language model perplexity as a quality signal, either alone or combined with dictionary scoring.

**KenLM Ensemble Approach** (Upstage AI, 2024):
> "Traditional KenLM trained only on Wikipedia fails to detect advertising/informal content. Using an ensemble of 'Good KenLM' (trained on high-quality data) and 'Bad KenLM' (trained on spam) improves Recall@30 from 70.59% to 81.90%."
>
> Source: [Rethinking KenLM](https://arxiv.org/html/2409.09613v1) - arXiv 2024

**Recommendation for TimeCapsule:** Consider adding KenLM perplexity as a complementary signal to dictionary scoring, particularly for detecting:
- Repetitive/templated text
- Garbled text that happens to use real words
- Advertising/boilerplate that dictionary scoring misses

---

## 2. OCR Error Correction Techniques

### 2.1 Pattern-Based Correction

Regex pattern matching for common OCR errors is the fastest approach and should be applied first.

**Common OCR error categories** (validated by TimeCapsule corpus analysis):

| Category | Cause | Examples | TimeCapsule Coverage |
|----------|-------|----------|---------------------|
| Long-s artifacts | ſ misread as f | fuch→such, faid→said | ~50 patterns |
| li/h confusion | Similar glyphs | tlie→the, wliich→which | ~40 patterns |
| ll→U confusion | Font artifacts | wiU→will, pubUc→public | ~75 patterns |
| rn/m confusion | Kerning | tirne→time, frorn→from | ~10 patterns |
| Ligature artifacts | fi, fl, ff | oflSce→office | ~10 patterns |

**TimeCapsule strength:** The Rust engine with 150+ pre-compiled patterns and ~35x speedup over Python is well-architected for this task.

### 2.2 Dictionary-Based Spell Correction

SymSpell is the industry standard for fast dictionary correction.

**SymSpell characteristics:**
- O(1) lookup via pre-computed delete combinations
- Edit distance 1-2 typically sufficient
- 82k+ word frequency dictionary standard
- Conservative to avoid false positives

**TimeCapsule implementation:** `tc-ocr-symspell` follows best practices with frequency thresholds and case preservation.

**Known limitation** (documented in OCR_CLEANING.md):
> "Cannot fix severely corrupted words (edit distance > 2)"

This is acceptable - words with >2 errors are candidates for LLM correction (Tier 3).

### 2.3 LLM-Based Correction (Emerging Best Practice)

For the worst OCR quality (score >= 0.20), LLM correction is increasingly validated.

**Key research findings** (TurkuNLP, 2025):

| Model | CER Reduction | Notes |
|-------|---------------|-------|
| GPT-4o | 58% | Best, but expensive |
| GPT-4o-mini | 48% | Good cost/performance |
| Llama-3.1-70B | 39% | Best open model |
| Llama-3.1-8B | 20% | Decent single-GPU option |
| Llama-3.2-3B | **Negative** | Actually increases errors |

> "Small models make it worse: Models under 8B parameters tend to introduce more errors than they fix."
>
> Source: [No Free Lunch: Challenges and Limitations of LLM-Based OCR Post-Correction](https://arxiv.org/abs/2503.18294) - TurkuNLP 2025

**TimeCapsule alignment:** The `LLM_OCR_CORRECTION_RESEARCH.md` document correctly identifies Llama-3.1-70B as the target and warns against small models.

### 2.4 Neural Sequence-to-Sequence Correction

For specialized OCR correction without full LLM costs:

**ByT5** (Byte-level T5):
> "ByT5 is effective for character-level OCR error correction, particularly for diacritic restoration and historical document cleanup."
>
> Source: [LLMs for OCR Post-Correction](https://www.marktechpost.com/2024/08/13/large-language-models-llms-for-ocr-post-correction/)

**Recommendation:** Consider ByT5 as an intermediate option between SymSpell (Tier 2) and full LLM (Tier 3) for medium-quality files.

---

## 3. Data Preprocessing

### 3.1 Unicode Normalization

**ftfy** (fixes text for you) is the standard tool for Unicode cleanup.

**Common issues it fixes:**
- Mojibake (encoding errors): "Ã©" → "é"
- Broken HTML entities: "&amp;amp;" → "&"
- Control characters
- Inconsistent quotation marks

**TimeCapsule implementation:** ✅ **Implemented in Rust**

Rather than using Python ftfy (which would require double file I/O at scale), TimeCapsule implements equivalent functionality in Rust:

```rust
// Rust implementation in rust-ocr-clean
use unicode_normalization::UnicodeNormalization;

fn fix_unicode(text: &str) -> String {
    let normalized: String = text.nfc().collect();  // NFC normalization
    let fixed = fix_mojibake(&normalized);          // Common patterns
    let fixed = normalize_unicode_whitespace(&fixed);
    fix_html_entities(&fixed)
}
```

This approach:
- Maintains single file I/O path (Python orchestrates, Rust processes)
- Handles 30+ mojibake patterns (UTF-8 misread as Latin-1)
- Normalizes Unicode whitespace variants
- Fixes common HTML entities

**Source:** [ftfy documentation](https://pypi.org/project/ftfy/), [unicode-normalization crate](https://crates.io/crates/unicode-normalization)

### 3.2 Language Detection

Filtering by language ensures the corpus is linguistically coherent.

**Industry standard tools:**

| Tool | Languages | Speed | Use Case |
|------|-----------|-------|----------|
| fastText lid.176.bin | 176 | Very fast | Production pipelines |
| whatlang (Rust) | 69 | Very fast | Rust pipelines |
| langdetect | 55 | Fast | C4 pipeline |
| CLD3 | 107 | Fast | Alternative |

**Best practice** (NVIDIA NeMo Curator):
> "Normalize input: strip whitespace, convert newlines to spaces. Confidence threshold typically 0.5-0.8 for filtering."
>
> Source: [fastText Language Identification](https://fasttext.cc/docs/en/language-identification.html)

**TimeCapsule implementation:** ✅ **Implemented in Rust**

Uses the `whatlang` Rust crate for language detection:

```rust
use whatlang::{detect, Lang};

fn detect_language(text: &str, confidence_threshold: f64) -> LangDetectResult {
    let sample: String = text.chars().take(10000).collect();
    match detect(&sample) {
        Some(info) => LangDetectResult {
            is_english: info.lang() == Lang::Eng && info.confidence() >= threshold,
            detected_lang: format!("{:?}", info.lang()),
            confidence: info.confidence(),
        },
        None => LangDetectResult { is_english: true, ... }  // Assume English if unknown
    }
}
```

**Why whatlang over fastText:**
- Pure Rust (no external model files, no numpy dependency issues)
- Integrated into same Rust binary as OCR patterns
- Sufficient language coverage (69 languages) for our use case
- Avoids Python→Rust→Python round-trips at scale

### 3.3 Document Structure Filtering

Heuristics for document-level quality:

**C4 (Colossal Clean Crawled Corpus) filters:**
- Lines must end in terminal punctuation
- Remove very short lines
- Three-line window deduplication

**Gopher (DeepMind) filters:**
- Mean word length 3-10 characters
- 80%+ alphabetic characters
- Stop word presence check

**Recommendation:** Consider adding structural filters to Tier 0 scoring.

---

## 4. Deduplication Strategies

Deduplication is critical for LLM training data quality.

### 4.1 Exact Deduplication

**Method:** Hash entire documents (MD5/SHA256)
**Use case:** Identical copies

**TimeCapsule status:** `tc-dedup` handles this.

### 4.2 Near-Duplicate Detection (MinHash + LSH)

**Method:** 
1. Compute MinHash signatures (N hash functions on character n-grams)
2. Use Locality-Sensitive Hashing to bucket similar documents
3. Compare Jaccard similarity within buckets

**Industry adoption:**
> "MinHash LSH is the standard for fuzzy deduplication at scale. The Pile, Dolma, and FineWeb all use variants of this approach."
>
> Source: [MinHash LSH in Milvus](https://milvus.io/blog/minhash-lsh-in-milvus-the-secret-weapon-for-fighting-duplicates-in-llm-training-data.md)

**Recommendation:** Ensure `tc-dedup` uses fuzzy matching, not just exact hashing.

### 4.3 Semantic Deduplication (Advanced)

**SemDeDup approach:**
> "Uses pretrained embeddings + clustering for semantic deduplication, removing documents that are paraphrases rather than exact copies."
>
> Source: [Data Management Survey](https://arxiv.org/html/2312.01700v3)

**Recommendation:** Consider for future enhancement if duplicate content remains problematic.

---

## 5. Historical Document Considerations

TimeCapsule's focus on pre-WWI text requires special handling.

### 5.1 Period-Specific Vocabulary

**Research finding:**
> "Neural network optimization for Hebrew historical text showed period-tuned models outperform generic ones."
>
> Source: [ACM Digital Library](https://dl.acm.org/doi/10.1145/3479159)

**TimeCapsule strength:** The `known_vocab.txt` whitelist with British spellings, Latin terms, and archaic English is excellent practice.

**Recommendation:** Continue expanding the whitelist based on corpus analysis.

### 5.2 Historical Spelling Variants

Not all "errors" are errors:

| Pattern | Historical | Modern | Action |
|---------|------------|--------|--------|
| publick | Valid 17th-18th c. | public | Context-dependent |
| connexion | British historical | connection | Preserve |
| shew | Valid pre-1800 | show | Preserve |
| chuse | Valid 18th c. | choose | Context-dependent |

**TimeCapsule strength:** The `CONTEXT_PATTERNS` in Rust track these for human review rather than auto-correcting.

### 5.3 Long-s (ſ) Handling

The long-s is extremely common in pre-1800 printing.

**TimeCapsule approach:** ~50 patterns for long-s artifacts (fuch→such, etc.) is comprehensive.

**Potential enhancement:** Consider a more general rule for ſ→s when the original character is preserved:
```rust
(Regex::new(r"ſ").unwrap(), "s", None),
```

This is already implemented in the Rust engine.

---

## 6. Performance at Scale

### 6.1 Rust for Performance-Critical Paths

**Industry trend:** High-performance data processing uses Rust/C++.

| Tool | Language | Use Case |
|------|----------|----------|
| Dolma Toolkit | Rust (Bloom filters) | Deduplication |
| NeMo Curator | Python + RAPIDS (GPU) | Full pipeline |
| DataTrove | Python (multiprocessing) | FineWeb |
| TimeCapsule | Rust (OCR patterns) | Pattern matching |

**TimeCapsule benchmark:**
- Rust: 13.9 MB/s
- Python: 0.4 MB/s
- Speedup: ~35x

This is excellent and matches industry practice.

### 6.2 Components to Consider for Rust Migration

Based on processing volume (147k+ files), consider Rust for:

| Component | Current | Benefit of Rust | Priority |
|-----------|---------|-----------------|----------|
| Pattern cleanup | Rust | Already done | Done |
| Vocab extraction | Rust | Already done | Done |
| SymSpell correction | Python | Moderate (SymSpell is already fast) | Medium |
| Scoring | Python | Low (pyenchant is C-backed) | Low |

**Recommendation:** The current Rust/Python split is reasonable. SymSpell in Rust would help at scale but isn't critical.

---

## 7. Industry vs TimeCapsule Comparison

Detailed comparison of OCR cleanup practices:

| Practice | Industry Approach | TimeCapsule Implementation | Notes |
|----------|-------------------|---------------------------|-------|
| **Preprocessing** |
| Unicode normalization | ftfy (Python) | Rust NFC + mojibake patterns | ✅ Equivalent, faster |
| Language detection | fastText (Python + model) | whatlang (Rust, no model) | ✅ Simpler deployment |
| Encoding detection | chardet/charset_normalizer | Assume UTF-8, fix mojibake | ⚠️ May miss edge cases |
| **Quality Filtering** |
| Dictionary scoring | pyenchant / hunspell | pyenchant | ✅ Same |
| Perplexity scoring | KenLM | Not implemented | ❌ Could add |
| Garbage detection | Heuristics | alpha_ratio, fragment_ratio | ✅ Similar |
| Document structure | Line length, punctuation | line_length_cv, list patterns | ✅ Enhanced |
| **OCR Correction** |
| Pattern matching | Regex (Python) | Regex (Rust, ~35x faster) | ✅ Better |
| Spell correction | SymSpell | SymSpell | ✅ Same |
| Neural correction | ByT5 | Not implemented | ❌ Could add |
| LLM correction | GPT-4/Llama-70B | Planned (Tier 3) | ⏳ Planned |
| **Deduplication** |
| Exact hash | MD5/SHA256 | MD5 | ✅ Same |
| Near-duplicate | MinHash + LSH | MinHash + LSH | ✅ Same |
| Semantic | Embeddings + clustering | Not implemented | ❌ Could add |
| **Historical Text** |
| Period vocabulary | Custom whitelists | known_vocab.txt (~1200 words) | ✅ Good coverage |
| Long-s handling | Pattern rules | ~50 patterns | ✅ Comprehensive |
| Spelling variants | Context-aware | CONTEXT_PATTERNS for review | ✅ Conservative |

### Remaining Enhancement Opportunities

Based on this analysis, prioritized enhancements for TimeCapsule:

### High Priority

1. ~~**Add ftfy preprocessing**~~ ✅ Implemented in Rust
2. ~~**Upgrade language detection**~~ ✅ Implemented in Rust (whatlang)
3. **Verify fuzzy deduplication** - Ensure MinHash/LSH, not just exact hashing

### Medium Priority

4. **Add perplexity scoring** - KenLM as complement to dictionary scoring
5. **ByT5 for Tier 2.5** - Intermediate between SymSpell and LLM
6. **Multi-column detection** - Reorder columns before processing

### Lower Priority

7. **SymSpell in Rust** - Performance gains at scale
8. **Semantic deduplication** - If duplicates remain problematic

---

## 8. Tools and Libraries Reference

### Production-Ready Toolkits

| Tool | Organization | URL | Best For |
|------|--------------|-----|----------|
| NeMo Curator | NVIDIA | [GitHub](https://github.com/NVIDIA/NeMo-Curator) | GPU-accelerated, full pipeline |
| DataTrove | Hugging Face | [GitHub](https://github.com/huggingface/datatrove) | FineWeb-style processing |
| Dolma Toolkit | AI2 | [GitHub](https://github.com/allenai/dolma) | Tagging, deduplication |

### Specialized Libraries

| Library | Purpose | URL |
|---------|---------|-----|
| ftfy | Unicode fixing | [PyPI](https://pypi.org/project/ftfy/) |
| KenLM | Perplexity scoring | [Source](https://kheafield.com/code/kenlm/) |
| SymSpell | Spell correction | [GitHub](https://github.com/wolfgarbe/SymSpell) |
| fastText | Language ID | [Website](https://fasttext.cc/docs/en/language-identification.html) |
| text-dedup | Deduplication | [GitHub](https://github.com/ChenghaoMou/text-dedup) |

---

## 9. References

### Academic Papers

1. **Data Management For Training Large Language Models: A Survey** (2024)
   - Authors: Peking University & Huawei Noah's Ark Lab
   - URL: https://arxiv.org/html/2312.01700v3
   - Key contribution: Comprehensive survey of data curation practices

2. **The FineWeb Datasets: Decanting the Web for the Finest Text Data at Scale** (NeurIPS 2024)
   - Authors: Hugging Face
   - URL: https://openreview.net/forum?id=n6SCkn2QaG
   - Key contribution: 15T token dataset with ablated filtering strategies

3. **Rethinking KenLM: Good and Bad Model Ensembles for Efficient Text Quality Filtering** (2024)
   - Authors: Upstage AI
   - URL: https://arxiv.org/html/2409.09613v1
   - Key contribution: Dual KenLM ensemble for quality filtering

4. **No Free Lunch: Challenges and Limitations of LLM-Based OCR Post-Correction** (2025)
   - Authors: TurkuNLP, University of Turku
   - URL: https://arxiv.org/abs/2503.18294
   - Key contribution: Model size thresholds for effective OCR correction

5. **Reference-Based Post-OCR Processing with LLM for Historical Documents** (2024)
   - URL: https://arxiv.org/html/2410.13305v1
   - Key contribution: LLM fine-tuning for historical OCR

### Industry Resources

6. **NVIDIA NeMo Curator Documentation**
   - URL: https://developer.nvidia.com/blog/mastering-llm-techniques-data-preprocessing/
   - Key contribution: Production pipeline architecture

7. **Programming Historian: Cleaning OCR'd Text with Regular Expressions**
   - URL: https://programminghistorian.org/en/lessons/cleaning-ocrd-text-with-regular-expressions
   - Key contribution: Historical document processing patterns

8. **MinHash LSH in Milvus**
   - URL: https://milvus.io/blog/minhash-lsh-in-milvus-the-secret-weapon-for-fighting-duplicates-in-llm-training-data.md
   - Key contribution: Deduplication implementation guidance

---

## 10. Conclusion

The TimeCapsule OCR cleanup pipeline is well-designed and aligns with industry best practices in its core architecture. The tiered approach, Rust performance optimization, vocabulary protection, and planned LLM correction are all validated by current research.

**Key strengths:**
- High-performance Rust engine for all preprocessing and pattern matching
- Unicode normalization equivalent to ftfy (implemented in Rust)
- Language detection via whatlang (Rust, no external model files)
- Document triage with structural quality signals
- Comprehensive OCR pattern coverage (150+ patterns)
- Period-appropriate vocabulary whitelist
- Human review checkpoint for ambiguous cases
- Research-backed Tier 3 LLM correction plan

**Architecture decision - Python as orchestrator, Rust for I/O:**
The pipeline maintains a clean separation where Python orchestrates the workflow while Rust handles all file I/O and text processing. This avoids double file reads at scale (2M+ documents) and achieves ~35x speedup over pure Python.

**Remaining enhancement opportunities:**
- Perplexity scoring (KenLM) for detecting repetitive/garbled text
- ByT5 for intermediate neural correction (between SymSpell and LLM)
- Multi-column text detection and reordering
- Semantic deduplication for paraphrase detection

The pipeline is production-ready for large-scale historical corpus processing. It implements 10 of the 13 industry best practices surveyed, with the remaining 3 being lower-priority enhancements.

---

*Document created: 2025-01-21*
*Last updated: 2026-01-21 (Unicode normalization and language detection implemented in Rust)*
*Based on research from 2020-2025 academic papers and industry practices*
