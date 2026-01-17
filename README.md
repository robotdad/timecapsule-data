# TimeCapsule Data Collection Tools

Tools for collecting temporally-filtered text corpora for training time-constrained language models.

## The Problem

When training a language model that should only "know" information up to a certain date (e.g., 1914), you need training data that:

1. **Was written before the cutoff** - No anachronistic knowledge
2. **Is high quality** - Proofread text, not raw OCR garbage
3. **Is properly licensed** - Public domain for unrestricted use
4. **Has verifiable provenance** - Know where each text came from

This toolkit solves these problems by collecting from curated sources with temporal metadata.

## Installation

Requires Python 3.11+ and [uv](https://github.com/astral-sh/uv).

```bash
# Clone the repository
git clone https://github.com/robotdad/timecapsule-data.git
cd timecapsule-data

# Install with uv
uv sync

# Or install in development mode
uv pip install -e .
```

## Quick Start

```bash
# Collect pre-1914 English texts from Project Gutenberg
tc-gutenberg --death-year 1914 --language en -o ./corpus/gutenberg

# Collect classical Greek and Latin from Perseus
tc-perseus --languages grc,lat --no-translations -o ./corpus/perseus

# Collect pre-1914 newspapers from Internet Archive
tc-ia --year-end 1914 --content-type newspaper -o ./corpus/ia-newspapers

# Validate temporal purity of your corpus
tc-validate ./corpus/ --cutoff-year 1914

# Deduplicate across sources
tc-dedup ./corpus/ -o ./corpus-deduped/
```

## Sources

### Project Gutenberg (`tc-gutenberg`)

**Quality**: ⭐⭐⭐⭐⭐ Excellent - Human proofread

The gold standard for public domain texts. Uses author death year for temporal filtering (most reliable metadata).

```bash
# English texts by authors who died before 1900
tc-gutenberg --death-year 1900 --language en -o ./gutenberg

# Multiple languages
tc-gutenberg --death-year 1914 --language en,fr,de -o ./gutenberg-multi

# Preview without downloading
tc-gutenberg --death-year 1914 --dry-run
```

### Perseus Digital Library (`tc-perseus`)

**Quality**: ⭐⭐⭐⭐⭐ Excellent - Scholarly editions

Classical Greek and Latin texts in original languages. These are the actual Plato, Aristotle, Homer, Cicero, etc.

```bash
# Greek and Latin originals only
tc-perseus --languages grc,lat --no-translations -o ./perseus

# Include English translations
tc-perseus --languages grc,lat,eng -o ./perseus-with-translations

# List available texts
tc-perseus --list-only
```

### Internet Archive (`tc-ia`)

**Quality**: ⭐⭐⭐ Variable - OCR with quality filtering

Massive collection with variable quality. The collector includes:
- Pre-download deduplication against Gutenberg
- OCR quality estimation and filtering
- Collection-based quality scoring
- Content type inference (newspaper, magazine, book, etc.)

```bash
# Pre-1914 newspapers
tc-ia --year-end 1914 --content-type newspaper -o ./ia-news

# High-quality books from known-good collections
tc-ia --year-end 1900 --collection americana --min-quality 0.85 -o ./ia-americana

# Dedupe against existing Gutenberg corpus
tc-ia --gutenberg-metadata ./gutenberg/metadata.csv --year-end 1914 -o ./ia

# Dry run to see what's available
tc-ia --year-end 1914 --dry-run --max-items 100
```

## Utilities

### Validate Temporal Purity (`tc-validate`)

Check your corpus for anachronistic content:

```bash
tc-validate ./corpus/ --cutoff-year 1914 --report validation-report.json
```

### Deduplicate (`tc-dedup`)

Remove duplicates across multiple sources:

```bash
tc-dedup ./corpus/ -o ./corpus-clean/ --prefer gutenberg
```

### Analyze Catalog (`tc-analyze`)

Analyze Gutenberg catalog for available texts:

```bash
tc-analyze --death-year 1914 --language en
```

## Metadata Schema

All collectors output consistent metadata for downstream processing:

```json
{
  "source": "gutenberg|perseus|internet_archive",
  "identifier": "unique-id",
  "title": "Text Title",
  "author": "Author Name",
  "date": "1850",
  "language": "en",
  "filepath": "relative/path/to/file.txt",
  "word_count": 50000,
  "quality_score": 0.95
}
```

Internet Archive includes additional fields for quality analysis:
- `collections`: Which IA collections the item belongs to
- `content_type`: Inferred type (newspaper, magazine, book, etc.)
- `ocr_quality`: Estimated OCR accuracy (0-1)
- `collection_score`: Quality score based on source collection

## Recommended Corpus Configurations

| Use Case | Cutoff | Sources | Est. Size |
|----------|--------|---------|-----------|
| Classical only | Ancient | Perseus | ~1 GB |
| Pre-modern | 1800 | Gutenberg + Perseus | ~10 GB |
| Victorian era | 1900 | Gutenberg + IA | ~50 GB |
| Pre-WWI (maximum) | 1914 | All sources | ~100 GB |

## Rate Limiting

The Internet Archive collector implements adaptive rate limiting:
- Starts at 2 seconds between requests
- Backs off exponentially on errors (429, 503)
- Speeds up gradually after consistent success

For large-scale collection, contact Internet Archive at info@archive.org.

## Project Structure

```
timecapsule-data/
├── src/timecapsule_data/
│   ├── collectors/
│   │   ├── gutenberg.py      # Project Gutenberg collector
│   │   ├── internet_archive.py  # Internet Archive collector
│   │   └── perseus.py        # Perseus Digital Library collector
│   └── utils/
│       ├── schema.py         # Unified metadata schema
│       ├── validate.py       # Temporal purity validation
│       ├── dedup.py          # Cross-source deduplication
│       └── analyze.py        # Catalog analysis
├── pyproject.toml
└── README.md
```

## License

MIT License - See LICENSE file.

## Related Projects

- [TimeCapsuleLLM](https://github.com/haykgrigo3/TimeCapsuleLLM) - The model training framework
- [Project Gutenberg](https://www.gutenberg.org/) - Source for proofread public domain texts
- [Perseus Digital Library](https://www.perseus.tufts.edu/) - Classical texts
- [Internet Archive](https://archive.org/) - Massive text archive

## Automated Pre-WWI Corpus Collection

For a fully automated end-to-end collection, use the orchestration script:

```bash
cd timecapsule-data

# Mini validation run first (~5-10 minutes, ~200 items)
uv run python scripts/collect_prewwi_corpus.py --mode mini

# Full collection (~3-5 days, ~147k items)
uv run python scripts/collect_prewwi_corpus.py --mode full

# Custom output directory
uv run python scripts/collect_prewwi_corpus.py --mode full --output /path/to/corpus

# Check status of running collection
uv run python scripts/collect_prewwi_corpus.py --status

# Resume interrupted collection
uv run python scripts/collect_prewwi_corpus.py --resume
```

The script handles the complete pipeline:
1. **Gutenberg** - ~17k high-quality proofread texts
2. **Internet Archive Books** - ~50k books with Gutenberg deduplication
3. **Internet Archive Newspapers** - ~80k newspaper issues
4. **Validation** - Temporal purity check
5. **OCR Cleanup** - Fix common OCR errors in IA texts
6. **Deduplication** - Merge sources, prefer Gutenberg quality
7. **Summary** - Generate metadata and statistics

Features:
- **Resume support** - Ctrl+C anytime, `--resume` to continue
- **Progress tracking** - Real-time file counts and ETAs
- **State persistence** - Saves progress after each stage
- **Human-readable output** - Times like "2h 15m", sizes like "1.2 GB"

Output defaults to `./corpus-prewwi/` relative to current directory.

---

## Manual Pre-WWI Corpus Workflow

For manual step-by-step collection with full control:
### Step 1: Collect from Sources

```bash
# Create corpus directory structure
mkdir -p corpus/{gutenberg,perseus,ia}

# Collect from Project Gutenberg (highest quality, ~17k pre-1914 English texts)
tc-gutenberg --cutoff-year 1914 --language en -o ./corpus/gutenberg

# Collect from Perseus (classical Greek/Latin)
tc-perseus --languages grc,lat --no-translations -o ./corpus/perseus

# Collect from Internet Archive (newspapers, with Gutenberg dedup)
tc-ia --year-end 1914 \
      --gutenberg-metadata ./corpus/gutenberg/metadata.csv \
      --content-type newspaper \
      --min-quality 0.7 \
      -o ./corpus/ia
```

### Step 2: Validate Temporal Purity

```bash
# Check all sources for anachronistic content
tc-validate ./corpus/gutenberg/en --cutoff-year 1914
tc-validate ./corpus/perseus --cutoff-year 1914
tc-validate ./corpus/ia --cutoff-year 1914
```

### Step 3: Clean OCR Errors (for IA texts)

```bash
# Analyze OCR quality
tc-ocr-clean analyze ./corpus/ia --report ocr-report.json

# Clean OCR errors
tc-ocr-clean batch ./corpus/ia -o ./corpus/ia-cleaned --report ocr-fixes.json
```

Note: Gutenberg texts are human-proofread and typically don't need OCR cleanup.
Perseus texts are scholarly editions, also high quality.

### Step 4: Deduplicate Across Sources

```bash
# Analyze duplicates
tc-dedup analyze ./corpus/gutenberg/en ./corpus/perseus ./corpus/ia-cleaned

# Merge with Gutenberg preferred (highest quality)
tc-dedup merge ./corpus/gutenberg/en ./corpus/perseus ./corpus/ia-cleaned \
         -o ./corpus/merged \
         --prefer gutenberg
```

### Step 5: Final Validation

```bash
# Validate merged corpus
tc-validate ./corpus/merged --cutoff-year 1914

# Check OCR quality of final corpus
tc-ocr-clean analyze ./corpus/merged
```

### Estimated Sizes

| Source | Pre-1914 Texts | Est. Size |
|--------|----------------|-----------|
| Gutenberg (English) | ~17,000 | ~15 GB |
| Perseus (Greek/Latin) | ~3,500 | ~1 GB |
| Internet Archive (newspapers) | ~80,000+ | ~50 GB |
| **Total (deduped)** | ~90,000 | ~50 GB |

### Validation Test Run

To verify the pipeline works before a full run:

```bash
# Quick test with limits
tc-gutenberg --cutoff-year 1914 --language en --limit 100 -o ./test/gutenberg
tc-perseus --languages grc,lat --limit 50 -o ./test/perseus
tc-validate ./test/gutenberg/en --cutoff-year 1914
tc-ocr-clean analyze ./test/gutenberg/en
tc-dedup analyze ./test/gutenberg/en ./test/perseus
```
