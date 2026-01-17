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
