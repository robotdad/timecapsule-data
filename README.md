# TimeCapsule Data Collection Tools

Tools for collecting temporally-filtered text corpora from Project Gutenberg
for training time-constrained language models.

## The Temporal Purity Problem

When training a model that should only "know" pre-1900 information, we face
two contamination risks:

1. **Source selection**: Including texts from authors who wrote after 1900
2. **Modern additions**: Gutenberg adds headers, footers, and editorial notes

### Solution: Author Death Year + Text Cleaning

**Filtering strategy**: We filter by **author death year**, not publication date.
- Gutenberg doesn't track original publication dates
- But the catalog includes author birth/death years
- If an author died in 1870, ALL their works are pre-1870 knowledge
- This is conservative but guarantees temporal purity

**Cleaning strategy**: We strip ALL Gutenberg-added content:
- Headers ("The Project Gutenberg EBook of...")
- Footers (license text, donation requests)
- Editorial notes ("Transcriber's note:", "Produced by...")
- Any line containing modern references

## Available Tools

### 1. `analyze_catalog.py` - Survey What's Available

Before collecting, see what exists by time period:

```bash
python analyze_catalog.py

# Output shows:
# - Texts available by era (Ancient, Medieval, Victorian, etc.)
# - Cumulative counts for different cutoff years
# - Sample authors and subjects per era
```

### 2. `gutenberg_collector.py` - Download & Clean Texts

```bash
# Greek classics and ancient texts
python gutenberg_collector.py --cutoff-year 500 --output-dir ./corpus_ancient

# Pre-industrial (Enlightenment era)  
python gutenberg_collector.py --cutoff-year 1800 --output-dir ./corpus_1800

# Victorian era (like TimeCapsuleLLM)
python gutenberg_collector.py --cutoff-year 1900 --output-dir ./corpus_1900

# Test with small sample first
python gutenberg_collector.py --cutoff-year 1900 --limit 100 --output-dir ./test_corpus
```

Options:
- `--cutoff-year`: Include authors who died on/before this year
- `--language`: ISO code (default: en)
- `--concurrency`: Download workers (default: 4)
- `--limit`: Limit books for testing

### 3. `validate_temporal_purity.py` - Check for Contamination

After collection, verify no modern content leaked through:

```bash
python validate_temporal_purity.py ./corpus_1900 --cutoff-year 1900

# Checks for:
# - Modern technology terms (computer, internet, etc.)
# - Post-cutoff historical references (World Wars, etc.)
# - Gutenberg boilerplate that wasn't stripped
# - Modern vocabulary (robot, radar, etc.)
# - Date references after cutoff year
```

## Workflow

```bash
# 1. Analyze what's available
python analyze_catalog.py --language en

# 2. Collect corpus (start small)
python gutenberg_collector.py -y 1900 -o ./corpus_1900 --limit 100

# 3. Validate purity
python validate_temporal_purity.py ./corpus_1900 -y 1900 --verbose

# 4. If clean, collect full corpus
python gutenberg_collector.py -y 1900 -o ./corpus_1900

# 5. Final validation
python validate_temporal_purity.py ./corpus_1900 -y 1900
```

## Expected Corpus Sizes (English)

| Cutoff Year | Era | ~Texts Available |
|-------------|-----|------------------|
| 500 | Ancient (Greeks, Romans) | ~200 |
| 1000 | Classical + Early Medieval | ~300 |
| 1600 | + Renaissance | ~500 |
| 1800 | + Enlightenment | ~2,000 |
| 1875 | + Early Victorian | ~8,000 |
| 1900 | + Late Victorian | ~15,000 |
| 1950 | + Early 20th Century | ~40,000 |

## Combining with TimeCapsuleLLM Dataset

The original TimeCapsuleLLM uses 90GB of 1800-1875 London texts from
Internet Archive. You can combine:

```python
# In your training script, combine datasets
from datasets import concatenate_datasets, load_from_disk

# TimeCapsuleLLM's London corpus
london = load_from_disk("./data/london-1800-1875")

# Gutenberg broader corpus
gutenberg = load_dataset("text", data_dir="./corpus_1875")

# Combine
combined = concatenate_datasets([london["train"], gutenberg["train"]])
```

## Technical Notes

### Why Author Death Year?

Consider Shakespeare (1564-1616):
- Hamlet was written ~1600
- But Gutenberg doesn't record that
- We only know Shakespeare died in 1616
- So with cutoff 1700, we safely include all Shakespeare

This is conservative - we might exclude some 1890s works from authors
who died in 1910 - but it guarantees no post-cutoff knowledge.

### BCE Handling

Ancient authors are handled correctly:
- "Sophocles, 496? BCE-407 BCE" → death_year = -407
- Cutoff 500 CE means any death_year ≤ 500 qualifies
- So -407 ≤ 500 → Sophocles is included

### Contamination Patterns

The cleaner removes these modern additions:
- `***START OF THE PROJECT GUTENBERG EBOOK***` markers
- Any line containing "Project Gutenberg", "gutenberg.org"
- "Transcriber's note", "Produced by", "E-text prepared by"
- Release dates, posting dates, encoding notes
- License and donation text

## Dependencies

```bash
pip install requests urllib3
```

## License

These tools are for research purposes. Gutenberg texts are public domain.
