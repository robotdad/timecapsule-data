# Overnight IA Collection Plan

## Current Status

**Code reverted** to original working version (commit 24835e4) with these additions:
- ✅ Book content-type filter added: `NOT (subject:newspaper OR subject:magazine OR subject:periodical)`
- ✅ Base delay increased: 3.0s (was 2.0s)
- ✅ --workers argument added (stub, not implemented yet)

**Your collection so far:**
- Gutenberg: 17,099 texts ✅
- IA Newspapers: 8,069 files ✅
- IA Books: 0 files

## The Plan: Sequential Testing

Since I cannot test IA queries from this environment (network timeouts), run these commands yourself:

### Step 1: Test Newspapers (Should Work - Worked Before)

```bash
uv run tc-ia --year-end 1914 --content-type newspaper --min-quality 0.75 \
  --max-items 100 --verbose \
  -o /mnt/h/datasets/timecapsule-prewwi/raw/ia/newspapers
```

**Expected:**
- Searches IA for newspapers
- Skips 8,069 existing files
- Downloads 100 NEW newspapers
- Creates metadata.json

**If this works:** Newspapers query is good ✓

### Step 2: Test Books (Unknown - May Fail)

```bash
uv run tc-ia --year-end 1914 --content-type book --min-quality 0.75 \
  --max-items 100 --verbose \
  -o /mnt/h/datasets/timecapsule-prewwi/raw/ia/books
```

**Expected:**
- Searches IA for books (excluding newspapers/magazines/periodicals)
- Downloads 100 books
- Creates metadata.json

**If this fails:** The book filter query is broken, needs different syntax

### Step 3: If Books Fail, Test Alternative Query

Try removing the book filter entirely and see if you get results:

```bash
# Temporarily test with --content-type any to bypass book filter
uv run tc-ia --year-end 1914 --content-type any --min-quality 0.75 \
  --max-items 10 --verbose \
  -o /tmp/test-any
```

If this returns results, the issue is the `NOT (subject:...)` syntax.

## Step 4: Once Validated, Build Full Collections

### For Newspapers (assuming Step 1 worked):

```bash
nohup uv run tc-ia --year-end 1914 --content-type newspaper \
  --min-quality 0.75 --max-items 80000 --verbose \
  -o /mnt/h/datasets/timecapsule-prewwi/raw/ia/newspapers \
  > /tmp/newspapers_download.log 2>&1 &

# Get PID
echo $! > /tmp/newspapers_pid.txt
```

### For Books (if Step 2 worked):

```bash
nohup uv run tc-ia --year-end 1914 --content-type book \
  --min-quality 0.75 --max-items 50000 --verbose \
  -o /mnt/h/datasets/timecapsule-prewwi/raw/ia/books \
  > /tmp/books_download.log 2>&1 &

# Get PID
echo $! > /tmp/books_pid.txt
```

## Checking Progress

```bash
# Check if processes are running
pgrep -f "tc-ia"

# Check logs
tail -50 /tmp/newspapers_download.log
tail -50 /tmp/books_download.log

# Count downloaded files
find /mnt/h/datasets/timecapsule-prewwi/raw/ia/newspapers -name "*.txt" | wc -l
find /mnt/h/datasets/timecapsule-prewwi/raw/ia/books -name "*.txt" | wc -l
```

## If Things Go Wrong

**If you get banned (likely):**
- Look for "Rate limited, backing off" messages in logs
- Process will slow down automatically (up to 120s delays)
- If it completely stops, just resume later (it tracks downloaded files)

**To stop processes:**
```bash
pkill -f "tc-ia"
# Or kill specific PID:
kill $(cat /tmp/newspapers_pid.txt)
kill $(cat /tmp/books_pid.txt)
```

## Notes for Morning

The original code has NO caching - it searches and downloads interleaved. This means:
- Each run starts searching from page 1
- It stops when it hits `--max-items` DOWNLOADED files
- Resume works because it skips existing files, but still searches from beginning

**After validating queries work**, we can add caching as a separate layer.

## What I Could Not Test

Network requests to IA timeout from this environment. I:
- ✅ Reverted to original working code
- ✅ Added book content-type filter
- ❌ Could not verify queries actually work

You need to run Steps 1-2 to validate, then proceed with background downloads.
