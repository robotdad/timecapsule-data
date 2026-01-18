# IA Collection Pipeline Redesign

## Core Insight

**Stop fighting query syntax.** Use IA's bulk export to get ALL items, filter locally, download from filtered list.

---

## New Architecture: 3-Phase Pipeline

### Phase 1: BUILD INDEX (One-time per epoch)

**Purpose:** Get complete catalog of ALL items in date range with text

**Command:** `tc-ia-index` (new tool)
```bash
tc-ia-index --year-start 1800 --year-end 1914 -o /path/to/corpus/
```

**What it does:**
1. Bulk export ALL items matching:
   - `date:[1800 TO 1914]`
   - `mediatype:texts`
   - `language:eng`
   - `(format:DjVu OR format:Text OR format:"Abbyy GZ")`
2. Request ALL available fields (identifier, title, date, creator, subject, collection, format, imagecount, etc.)
3. Export in batches of 10k items (IA seems to handle this well)
4. Save to: `corpus/metadata/ia_index_1800_1914.json`
5. **Skip if index exists** (unless --refresh flag)

**Time estimate:** 2.3M items ÷ 10k per request = 230 requests × 3s = ~12 minutes

**Output format:**
```json
{
  "query": "date:[1800 TO 1914] AND ...",
  "date_range": [1800, 1914],
  "exported_at": "2026-01-18 09:00:00",
  "total_found": 2286364,
  "total_exported": 2286364,
  "items": [
    {
      "identifier": "...",
      "title": "...",
      "date": "1909-04-01T00:00:00Z",
      "year": 1909,
      "format": ["DjVuTXT", "Text PDF", ...],
      "subject": [...],
      "collection": [...],
      "imagecount": 250,
      ...
    }
  ]
}
```

---

### Phase 2: FILTER INDEX (Optional, could be part of download)

**Purpose:** Reduce 2.3M items to downloadable subset

**Options:**

**Option A: Filter at download time** (simpler)
- No separate filter step
- tc-ia-download loads index and filters on-the-fly as it goes
- Pros: Simpler, fewer files
- Cons: Re-filtering on every run

**Option B: Pre-filter to SQLite** (more complex but queryable)
- Convert index to SQLite database
- Filter queries:
  - Has text format (DjVuTXT, Text PDF, Abbyy GZ)
  - Not in Gutenberg (check against gutenberg/metadata.csv)
  - Quality threshold (collection scoring)
  - Imagecount > N (exclude tiny pamphlets)
- Mark items as downloaded/skipped/failed
- Pros: Queryable, efficient resume, can report stats
- Cons: More complexity

**Option C: Filtered JSON** (middle ground)
- Load full index
- Apply filters
- Save to: `corpus/metadata/ia_filtered_1800_1914.json`
- Download script uses filtered list
- Pros: Simple, cached filtering
- Cons: Not queryable, harder to update filters

**Recommendation:** Start with Option A (filter at download time), can upgrade to SQLite later if needed.

**Filters to apply:**
1. **Has text format**: `any(f in ['DjVuTXT', 'Text PDF', 'Abbyy GZ', 'hOCR'] for f in item['format'])`
2. **In date range**: `1800 <= item['year'] <= 1914`
3. **Not in Gutenberg**: Check identifier/title against gutenberg/metadata.csv
4. **Quality threshold**: Collection scoring (already have QUALITY_COLLECTIONS dict)
5. **Size threshold**: `item.get('imagecount', 0) > 10` (exclude tiny items)

---

### Phase 3: DOWNLOAD

**Purpose:** Download text files for filtered items with resume support

**Command:** `tc-ia-download` (new tool)
```bash
tc-ia-download --index /path/to/ia_index_1800_1914.json \
  --max-items 50000 --workers 4 \
  -o /path/to/corpus/raw/ia/
```

**What it does:**
1. Load index from JSON
2. Load resume state from `corpus/metadata/download_state.json`:
   ```json
   {
     "downloaded": ["id1", "id2", ...],
     "failed": ["id99": "no_text_file", ...],
     "last_updated": "2026-01-18 12:30:00"
   }
   ```
3. Filter index (on-the-fly):
   - Skip already downloaded
   - Skip Gutenberg duplicates
   - Apply quality/size thresholds
4. Download with workers:
   - Parallel downloads (ThreadPoolExecutor)
   - Rate limiting (adaptive, per-worker)
   - Progress every 50 items: `[1,234 / 50,000] ✓ 45.2% - ETA: 2h 15m`
5. Save metadata.json with downloaded items
6. Update download_state.json frequently (every 100 items)

**Resume behavior:**
- Run same command again
- Loads state, skips downloaded items
- Continues from where it left off

**Progress indicators:**
```
[1,234 / 50,000] ✓ 45.2% complete - 12.3 items/min - ETA: 2h 15m
  Downloaded: 1,234 items (15.2 GB)
  Skipped: 234 (already have, low quality, etc.)
  Failed: 12 items
```

---

## File Structure

```
corpus/
  metadata/
    ia_index_1800_1914.json          # Full 2.3M item index
    download_state.json               # Resume tracking
    cache/                            # (removed - index replaces this)
  raw/
    gutenberg/
      metadata.csv                    # For dedup checking
      en/*.txt
    ia/
      *.txt                           # All IA downloads (no books/newspapers split)
      metadata.json                   # Downloaded item metadata
      failed_items.json               # Failed downloads for retry
```

**Note:** No more books/ and newspapers/ subdirectories. All IA text goes in one directory since we're not separating at search time.

---

## Commands & Workflow

### Initial Setup
```bash
# 1. Build the index (one-time, ~12 minutes)
tc-ia-index --year-start 1800 --year-end 1914 \
  -o /mnt/h/datasets/timecapsule-prewwi
```

Creates: `timecapsule-prewwi/metadata/ia_index_1800_1914.json`

### Download
```bash
# 2. Download items from index (resumable, can run multiple times)
tc-ia-download --index timecapsule-prewwi/metadata/ia_index_1800_1914.json \
  --max-items 100000 --workers 4 \
  --gutenberg-metadata timecapsule-prewwi/raw/gutenberg/metadata.csv \
  -o timecapsule-prewwi/raw/ia
```

Downloads to: `timecapsule-prewwi/raw/ia/*.txt`

### Test with Small Batches
```bash
# Build index (if not exists)
tc-ia-index --year-start 1800 --year-end 1914 -o /mnt/h/datasets/timecapsule-prewwi

# Download just 100 items (for testing)
tc-ia-download --index /mnt/h/datasets/timecapsule-prewwi/metadata/ia_index_1800_1914.json \
  --max-items 100 --workers 2 \
  -o /mnt/h/datasets/timecapsule-prewwi/raw/ia

# Check results, then continue
tc-ia-download --index /mnt/h/datasets/timecapsule-prewwi/metadata/ia_index_1800_1914.json \
  --max-items 1000 --workers 4 \
  -o /mnt/h/datasets/timecapsule-prewwi/raw/ia
```

---

## Implementation Plan

### 1. Create `tc-ia-index` tool
- Use bulk export approach (test_bulk_export.py as starting point)
- Request all available fields
- Handle pagination (10k per request)
- Save to single JSON file
- Skip if index exists (unless --refresh)

### 2. Create `tc-ia-download` tool
- Load index JSON
- Load/save download state for resume
- Filter items:
  - Has text format
  - Not in Gutenberg
  - Quality threshold
  - Size threshold
- Download with workers + rate limiting
- Progress tracking
- Save metadata.json

### 3. Update `collect_prewwi_corpus.py` orchestrator
- Stage 1: Call tc-ia-index (once)
- Stage 2: Call tc-ia-download with limits
- Remove old tc-ia calls and stage_ia_books/stage_ia_newspapers

### 4. Delete old code
- Remove search_ia() function
- Remove all content-type filtering logic
- Remove page-based search loop
- Keep: RateLimiter, download_text(), quality scoring, ExistingCorpus

---

## Questions for You

1. **Index storage**: Single JSON file OK? Or prefer SQLite for querying?
2. **Gutenberg dedup**: Filter before download, or download and dedup later in pipeline?
3. **Content type separation**: Keep all IA in one directory, or still split books/newspapers somehow?
4. **Quality filtering**: Apply during download (skip low quality), or download everything and filter later?
5. **Max index size**: Request all 2.3M items or cap at some limit (100k, 500k)?

---

## Benefits

✅ **No more query syntax hell** - One simple query that works  
✅ **Fast index building** - 12 minutes to get 2.3M identifiers  
✅ **True resume** - State file tracks exactly what's downloaded  
✅ **Flexible filtering** - Change filters without re-searching IA  
✅ **Progress visibility** - Clear indicators during long downloads  
✅ **Clean architecture** - Index → Filter → Download (3 clear phases)  
✅ **No legacy code** - Fresh start, proven approach  

---

This is the plan. Review it and let me know what adjustments you want before I start implementing.
