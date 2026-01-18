#!/usr/bin/env python3
"""
Pre-WWI Corpus Collection Orchestrator

Automated end-to-end pipeline for collecting a temporally-filtered text corpus
(pre-1914) from multiple sources. Supports resume, progress tracking, and time
estimation.

Usage:
    # Mini validation run (~5-10 minutes)
    uv run python scripts/collect_prewwi_corpus.py --mode mini

    # Full collection (~3-5 days)
    uv run python scripts/collect_prewwi_corpus.py --mode full

    # Resume interrupted run
    uv run python scripts/collect_prewwi_corpus.py --resume

    # Retry failed stages
    uv run python scripts/collect_prewwi_corpus.py --resume --retry-failed

    # Run specific stage only
    uv run python scripts/collect_prewwi_corpus.py --stage ia_books

    # Check status
    uv run python scripts/collect_prewwi_corpus.py --status

Output is written to ./corpus-prewwi/ by default (relative to current directory).
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional

# =============================================================================
# Configuration
# =============================================================================


class Config:
    OUTPUT_BASE: Path = None
    REPO_DIR: Path = Path(__file__).parent.parent.resolve()
    YEAR_START = 0  # Start from year 0 (earliest available data)
    CUTOFF_YEAR = 1914  # Pre-WWI cutoff
    LANGUAGE = "en"
    IA_MIN_QUALITY = 0.65  # Include newspapers and general book collections
    MINI_GUTENBERG_LIMIT = 100
    MINI_IA_LIMIT = 1000  # Combined limit for mini mode

    @classmethod
    def init(cls, output_dir: Optional[str] = None):
        if output_dir:
            cls.OUTPUT_BASE = Path(output_dir).resolve()
        else:
            cls.OUTPUT_BASE = Path.cwd() / "corpus-prewwi"

    @classmethod
    def raw_dir(cls) -> Path:
        return cls.OUTPUT_BASE / "raw"

    @classmethod
    def cleaned_dir(cls) -> Path:
        return cls.OUTPUT_BASE / "cleaned"

    @classmethod
    def deduped_dir(cls) -> Path:
        return cls.OUTPUT_BASE / "deduped"

    @classmethod
    def metadata_dir(cls) -> Path:
        return cls.OUTPUT_BASE / "metadata"

    @classmethod
    def state_file(cls) -> Path:
        return cls.OUTPUT_BASE / ".collection_state.json"

    @classmethod
    def log_file(cls) -> Path:
        return cls.OUTPUT_BASE / "collection.log"


# =============================================================================
# Utilities
# =============================================================================


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"


def format_size(bytes_val: int) -> str:
    if bytes_val < 1024:
        return f"{bytes_val} B"
    elif bytes_val < 1024 * 1024:
        return f"{bytes_val / 1024:.1f} KB"
    elif bytes_val < 1024 * 1024 * 1024:
        return f"{bytes_val / 1024 / 1024:.1f} MB"
    else:
        return f"{bytes_val / 1024 / 1024 / 1024:.2f} GB"


# =============================================================================
# Stage Definitions
# =============================================================================


class Stage(Enum):
    INIT = "init"
    GUTENBERG = "gutenberg"
    IA_INDEX = "ia_index"
    IA_ENRICH = "ia_enrich"
    IA_DOWNLOAD = "ia_download"
    VALIDATE = "validate"
    OCR_CLEAN = "ocr_clean"
    VOCAB_EXTRACT = "vocab_extract"
    VOCAB_REVIEW = "vocab_review"  # Human review checkpoint
    DEDUP = "dedup"
    FINALIZE = "finalize"
    COMPLETE = "complete"


STAGE_ORDER = [
    Stage.INIT,
    Stage.GUTENBERG,
    Stage.IA_INDEX,
    Stage.IA_ENRICH,
    Stage.IA_DOWNLOAD,
    Stage.VALIDATE,
    Stage.OCR_CLEAN,
    Stage.VOCAB_EXTRACT,
    Stage.VOCAB_REVIEW,
    Stage.DEDUP,
    Stage.FINALIZE,
    Stage.COMPLETE,
]

STAGE_DESCRIPTIONS = {
    Stage.INIT: "Initializing directories",
    Stage.GUTENBERG: "Collecting from Project Gutenberg",
    Stage.IA_INDEX: "Building Internet Archive catalog index",
    Stage.IA_ENRICH: "Enriching index with text filenames",
    Stage.IA_DOWNLOAD: "Downloading texts from Internet Archive",
    Stage.VALIDATE: "Validating temporal purity",
    Stage.OCR_CLEAN: "Cleaning OCR errors",
    Stage.VOCAB_EXTRACT: "Extracting vocabulary for review",
    Stage.VOCAB_REVIEW: "STOP - Review vocabulary candidates before continuing",
    Stage.DEDUP: "Deduplicating across sources",
    Stage.FINALIZE: "Generating summary",
    Stage.COMPLETE: "Complete",
}

STAGE_ESTIMATES_FULL = {
    Stage.INIT: 0.01,
    Stage.GUTENBERG: 2.0,
    Stage.IA_INDEX: 0.25,  # ~15 minutes for 2.3M items
    Stage.IA_ENRICH: 12.0,  # ~12 hours for ~1.5M items @ 0.65 threshold
    Stage.IA_DOWNLOAD: 48.0,  # ~48 hours for 50-100k items
    Stage.VALIDATE: 2.0,
    Stage.OCR_CLEAN: 6.0,
    Stage.VOCAB_EXTRACT: 1.0,
    Stage.VOCAB_REVIEW: 0.0,  # Manual step
    Stage.DEDUP: 4.0,
    Stage.FINALIZE: 0.5,
}

STAGE_ESTIMATES_MINI = {
    Stage.INIT: 0.002,
    Stage.GUTENBERG: 0.1,
    Stage.IA_INDEX: 0.01,  # ~30 seconds for small range
    Stage.IA_ENRICH: 0.08,  # ~5 minutes for ~1000 items
    Stage.IA_DOWNLOAD: 0.05,  # ~3 minutes for 100 items
    Stage.VALIDATE: 0.02,
    Stage.OCR_CLEAN: 0.02,
    Stage.VOCAB_EXTRACT: 0.02,
    Stage.VOCAB_REVIEW: 0.0,  # Manual step
    Stage.DEDUP: 0.02,
    Stage.FINALIZE: 0.01,
}


# =============================================================================
# State Management
# =============================================================================


@dataclass
class StageProgress:
    items_total: int = 0
    items_completed: int = 0
    bytes_downloaded: int = 0
    errors: int = 0
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_seconds: float = 0.0


@dataclass
class CollectionState:
    mode: str = "mini"
    current_stage: str = Stage.INIT.value
    started_at: Optional[str] = None
    updated_at: Optional[str] = None
    stages_completed: dict = None
    stages_in_progress: dict = None
    total_files: int = 0
    total_bytes: int = 0

    def __post_init__(self):
        if self.stages_completed is None:
            self.stages_completed = {}
        if self.stages_in_progress is None:
            self.stages_in_progress = {}

    def save(self, path: Path):
        self.updated_at = datetime.now().isoformat()
        with open(path, "w") as f:
            json.dump(asdict(self), f, indent=2, default=str)

    @classmethod
    def load(cls, path: Path) -> "CollectionState":
        if not path.exists():
            return cls()
        with open(path) as f:
            data = json.load(f)
        return cls(**data)

    def mark_stage_started(self, stage: Stage):
        self.current_stage = stage.value
        self.stages_in_progress[stage.value] = {
            "start_time": datetime.now().isoformat(),
            "items_completed": 0,
            "items_total": 0,
        }

    def mark_stage_completed(self, stage: Stage, progress: StageProgress):
        self.stages_completed[stage.value] = asdict(progress)
        if stage.value in self.stages_in_progress:
            del self.stages_in_progress[stage.value]
        idx = STAGE_ORDER.index(stage)
        if idx + 1 < len(STAGE_ORDER):
            self.current_stage = STAGE_ORDER[idx + 1].value

    def clear_stage(self, stage: Stage):
        """Remove a stage from completed so it can be re-run."""
        if stage.value in self.stages_completed:
            del self.stages_completed[stage.value]

    def get_failed_stages(self) -> list:
        """Return list of stages that completed with errors."""
        failed = []
        for stage_name, info in self.stages_completed.items():
            if info.get("errors", 0) > 0:
                failed.append(Stage(stage_name))
        return failed

    def get_current_stage(self) -> Stage:
        return Stage(self.current_stage)

    def is_stage_completed(self, stage: Stage) -> bool:
        return stage.value in self.stages_completed

    def get_elapsed_time(self) -> timedelta:
        if not self.started_at:
            return timedelta(0)
        start = datetime.fromisoformat(self.started_at)
        return datetime.now() - start

    def estimate_remaining_time(self) -> timedelta:
        estimates = STAGE_ESTIMATES_MINI if self.mode == "mini" else STAGE_ESTIMATES_FULL
        remaining_hours = 0.0
        current_idx = STAGE_ORDER.index(self.get_current_stage())

        for stage in STAGE_ORDER[current_idx:]:
            if stage == Stage.COMPLETE:
                continue
            if not self.is_stage_completed(stage):
                est = estimates.get(stage, 1.0)
                if self.stages_completed:
                    ratios = []
                    for name, progress in self.stages_completed.items():
                        completed_stage = Stage(name)
                        if completed_stage in estimates and estimates[completed_stage] > 0:
                            actual = progress.get("duration_seconds", 0) / 3600
                            estimated = estimates[completed_stage]
                            if estimated > 0 and actual > 0:
                                ratios.append(actual / estimated)
                    if ratios:
                        est *= sum(ratios) / len(ratios)
                remaining_hours += est
        return timedelta(hours=remaining_hours)


# =============================================================================
# Logging Setup
# =============================================================================


def setup_logging(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("corpus_collector")
    logger.setLevel(logging.DEBUG)
    logger.handlers = []

    log_file.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# =============================================================================
# Progress Monitor
# =============================================================================


class ProgressMonitor:
    def __init__(self, watch_dir: Path, expected_total: int = 0, label: str = "files"):
        self.watch_dir = watch_dir
        self.expected_total = expected_total
        self.label = label
        self.stop_event = threading.Event()
        self.thread = None
        self.last_count = 0
        self.last_size = 0
        self.start_time = time.time()
        self.initial_count = 0  # Track starting count for accurate rate calc

    def _count_files(self) -> tuple:
        count = 0
        size = 0
        try:
            for f in self.watch_dir.rglob("*.txt"):
                count += 1
                try:
                    size += f.stat().st_size
                except:
                    pass
        except:
            pass
        return count, size

    def _monitor_loop(self):
        while not self.stop_event.is_set():
            count, size = self._count_files()

            if count != self.last_count:
                elapsed = time.time() - self.start_time
                # Calculate rate based on NEW files only, not pre-existing
                new_files = count - self.initial_count
                rate = new_files / elapsed if elapsed > 0 else 0

                if self.expected_total > 0:
                    progress = f"{count}/{self.expected_total} {self.label}"
                    pct = count / self.expected_total * 100
                    remaining_files = self.expected_total - count
                    if rate > 0 and remaining_files > 0:
                        remaining = remaining_files / rate
                        eta = format_duration(remaining)
                        print(
                            f"    Progress: {progress} ({pct:.0f}%) - {format_size(size)} - ETA: {eta} ({rate:.1f}/s)"
                        )
                    else:
                        print(f"    Progress: {progress} ({pct:.0f}%) - {format_size(size)}")
                else:
                    print(f"    Progress: {count} {self.label} - {format_size(size)}")

                sys.stdout.flush()
                self.last_count = count
                self.last_size = size

            self.stop_event.wait(3)

    def start(self):
        self.initial_count, _ = self._count_files()  # Snapshot existing files
        self.start_time = time.time()
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()

    def stop(self) -> tuple:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=1)
        return self._count_files()


# =============================================================================
# Command Execution
# =============================================================================


def run_tc_command(
    tool: str, args: list, logger: logging.Logger, monitor: Optional[ProgressMonitor] = None
) -> bool:
    cmd = ["uv", "run", tool] + args
    logger.debug(f"Running: {' '.join(cmd)}")

    if monitor:
        monitor.start()

    try:
        process = subprocess.Popen(
            cmd,
            cwd=Config.REPO_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )

        for line in iter(process.stdout.readline, ""):
            line = line.rstrip()
            if line:
                print(f"    {line}")
                sys.stdout.flush()
                logger.debug(f"[{tool}] {line}")

        process.wait()

        if monitor:
            monitor.stop()

        return process.returncode == 0

    except Exception as e:
        logger.error(f"Command error: {e}")
        if monitor:
            monitor.stop()
        return False


# =============================================================================
# Stage Implementations
# =============================================================================


def stage_init(state: CollectionState, logger: logging.Logger) -> StageProgress:
    progress = StageProgress(start_time=datetime.now().isoformat())
    dirs = [
        Config.raw_dir() / "gutenberg",
        Config.raw_dir() / "ia",  # Single IA directory (no books/newspapers split)
        Config.cleaned_dir(),
        Config.deduped_dir(),
        Config.metadata_dir(),
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        print(f"    Created: {d}")
        progress.items_completed += 1
    progress.items_total = len(dirs)
    progress.end_time = datetime.now().isoformat()
    progress.duration_seconds = (
        datetime.fromisoformat(progress.end_time) - datetime.fromisoformat(progress.start_time)
    ).total_seconds()
    return progress


def stage_gutenberg(state: CollectionState, logger: logging.Logger) -> StageProgress:
    progress = StageProgress(start_time=datetime.now().isoformat())
    output_dir = Config.raw_dir() / "gutenberg"

    limit = Config.MINI_GUTENBERG_LIMIT if state.mode == "mini" else 17000
    progress.items_total = limit

    args = [
        "--cutoff-year",
        str(Config.CUTOFF_YEAR),
        "--language",
        Config.LANGUAGE,
        "-o",
        str(output_dir),
        "--verbose",
    ]
    if state.mode == "mini":
        args.extend(["--limit", str(limit)])

    logger.info(f"Downloading up to {limit} texts from Gutenberg...")

    monitor = ProgressMonitor(output_dir / "en", limit, "texts")
    success = run_tc_command("tc-gutenberg", args, logger, monitor)

    if success:
        gutenberg_dir = output_dir / "en"
        if gutenberg_dir.exists():
            files = list(gutenberg_dir.glob("*.txt"))
            progress.items_completed = len(files)
            progress.bytes_downloaded = sum(f.stat().st_size for f in files)
        logger.info(
            f"Gutenberg complete: {progress.items_completed} texts, {format_size(progress.bytes_downloaded)}"
        )
    else:
        progress.errors = 1
        logger.error("Gutenberg collection failed")

    progress.end_time = datetime.now().isoformat()
    progress.duration_seconds = (
        datetime.fromisoformat(progress.end_time) - datetime.fromisoformat(progress.start_time)
    ).total_seconds()
    return progress


def stage_ia_index(state: CollectionState, logger: logging.Logger) -> StageProgress:
    """Build IA catalog index (Phase 1 - fast, Scraping API)."""
    progress = StageProgress(start_time=datetime.now().isoformat())

    index_file = Config.metadata_dir() / f"ia_index_{Config.YEAR_START}_{Config.CUTOFF_YEAR}.json"

    # Skip if index already exists
    if index_file.exists():
        logger.info(f"IA index already exists: {index_file}")
        progress.items_completed = 1
        progress.items_total = 1
        progress.end_time = datetime.now().isoformat()
        progress.duration_seconds = 0.1
        return progress

    logger.info(f"Building IA index for {Config.YEAR_START}-{Config.CUTOFF_YEAR}...")

    args = [
        "--year-start",
        str(Config.YEAR_START),
        "--year-end",
        str(Config.CUTOFF_YEAR),
        "-o",
        str(Config.OUTPUT_BASE),
    ]

    success = run_tc_command("tc-ia-index", args, logger)

    if success and index_file.exists():
        progress.items_completed = 1
        progress.items_total = 1
        logger.info(f"IA index built: {index_file}")
    else:
        progress.errors = 1
        logger.error("IA index build failed")

    progress.end_time = datetime.now().isoformat()
    progress.duration_seconds = (
        datetime.fromisoformat(progress.end_time) - datetime.fromisoformat(progress.start_time)
    ).total_seconds()
    return progress


def stage_ia_enrich(state: CollectionState, logger: logging.Logger) -> StageProgress:
    """Enrich IA index with text filenames (Phase 2 - selective, Metadata API)."""
    progress = StageProgress(start_time=datetime.now().isoformat())

    index_file = Config.metadata_dir() / f"ia_index_{Config.YEAR_START}_{Config.CUTOFF_YEAR}.json"

    if not index_file.exists():
        logger.error(f"IA index not found: {index_file}")
        progress.errors = 1
        progress.end_time = datetime.now().isoformat()
        progress.duration_seconds = 0.1
        return progress

    logger.info(f"Enriching IA index with text filenames (quality >= {Config.IA_MIN_QUALITY})...")

    args = [
        "--index",
        str(index_file),
        "--min-quality",
        str(Config.IA_MIN_QUALITY),
        "--workers",
        "4",
    ]

    success = run_tc_command("tc-ia-enrich", args, logger)

    if success:
        # Count enriched items
        with open(index_file) as f:
            import json

            data = json.load(f)
        enriched = data.get("enrichment_status", {}).get("total_enriched", 0)
        progress.items_completed = enriched
        progress.items_total = enriched
        logger.info(f"IA enrichment complete: {enriched:,} items with filenames")
    else:
        progress.errors = 1
        logger.error("IA enrichment failed")

    progress.end_time = datetime.now().isoformat()
    progress.duration_seconds = (
        datetime.fromisoformat(progress.end_time) - datetime.fromisoformat(progress.start_time)
    ).total_seconds()
    return progress


def stage_ia_download(state: CollectionState, logger: logging.Logger) -> StageProgress:
    """Download texts from enriched index (Phase 3 - smart download)."""
    progress = StageProgress(start_time=datetime.now().isoformat())

    index_file = Config.metadata_dir() / f"ia_index_{Config.YEAR_START}_{Config.CUTOFF_YEAR}.json"
    output_dir = Config.raw_dir() / "ia"
    gutenberg_meta = Config.raw_dir() / "gutenberg" / "metadata.csv"

    if not index_file.exists():
        logger.error(f"IA index not found: {index_file}")
        progress.errors = 1
        progress.end_time = datetime.now().isoformat()
        progress.duration_seconds = 0.1
        return progress

    # Count existing files for resume
    existing_files = list(output_dir.rglob("*.txt")) if output_dir.exists() else []
    existing_count = len(existing_files)
    if existing_count > 0:
        logger.info(f"Found {existing_count} existing files, will skip already downloaded")

    limit = Config.MINI_IA_LIMIT if state.mode == "mini" else 100000
    progress.items_total = limit

    logger.info(f"Downloading up to {limit:,} items from IA index...")

    args = [
        "--index",
        str(index_file),
        "--max-items",
        str(limit),
        "--workers",
        "4",
        "-o",
        str(output_dir),
    ]
    if gutenberg_meta.exists():
        args.extend(["--gutenberg-metadata", str(gutenberg_meta)])

    monitor = ProgressMonitor(output_dir, limit, "items")
    success = run_tc_command("tc-ia-download", args, logger, monitor)

    # Count final files
    if output_dir.exists():
        files = list(output_dir.rglob("*.txt"))
        progress.items_completed = len(files)
        progress.bytes_downloaded = sum(f.stat().st_size for f in files)

    if success:
        logger.info(
            f"IA download complete: {progress.items_completed:,} items, {format_size(progress.bytes_downloaded)}"
        )
    else:
        progress.errors = 1
        if progress.items_completed > 0:
            logger.warning(
                f"IA download had errors but got {progress.items_completed:,} files, {format_size(progress.bytes_downloaded)}"
            )
        else:
            logger.error("IA download failed")

    progress.end_time = datetime.now().isoformat()
    progress.duration_seconds = (
        datetime.fromisoformat(progress.end_time) - datetime.fromisoformat(progress.start_time)
    ).total_seconds()
    return progress


# Old IA stage functions removed - replaced by three-phase pipeline:
# - stage_ia_index: Build catalog index
# - stage_ia_enrich: Add text filenames
# - stage_ia_download: Download from enriched index


def stage_validate(state: CollectionState, logger: logging.Logger) -> StageProgress:
    progress = StageProgress(start_time=datetime.now().isoformat())

    all_files = list(Config.raw_dir().rglob("*.txt"))
    progress.items_total = len(all_files)

    logger.info(f"Validating {progress.items_total} files for temporal purity...")

    args = [str(Config.raw_dir()), "--cutoff-year", str(Config.CUTOFF_YEAR), "--verbose"]
    success = run_tc_command("tc-validate", args, logger)

    progress.items_completed = progress.items_total
    if not success:
        progress.errors = 1
        logger.warning("Validation completed with warnings")
    else:
        logger.info(f"Validation complete: {progress.items_completed} files checked")

    progress.end_time = datetime.now().isoformat()
    progress.duration_seconds = (
        datetime.fromisoformat(progress.end_time) - datetime.fromisoformat(progress.start_time)
    ).total_seconds()
    return progress


def stage_ocr_clean(state: CollectionState, logger: logging.Logger) -> StageProgress:
    progress = StageProgress(start_time=datetime.now().isoformat())
    ia_dir = Config.raw_dir() / "ia"
    cleaned_dir = Config.cleaned_dir() / "ia"

    if not ia_dir.exists():
        logger.info("No IA files to clean")
        progress.end_time = datetime.now().isoformat()
        progress.duration_seconds = 0.1
        return progress

    source_files = list(ia_dir.rglob("*.txt"))
    if not source_files:
        logger.info("No IA text files found, skipping")
        progress.end_time = datetime.now().isoformat()
        progress.duration_seconds = 0.1
        return progress

    # Check for already-cleaned files (incremental mode)
    cleaned_dir.mkdir(parents=True, exist_ok=True)
    already_cleaned = set()
    if cleaned_dir.exists():
        already_cleaned = {f.name for f in cleaned_dir.rglob("*.txt")}

    to_clean = [f for f in source_files if f.name not in already_cleaned]

    if not to_clean:
        logger.info(f"All {len(source_files)} IA files already cleaned, skipping")
        progress.items_completed = len(source_files)
        progress.items_total = len(source_files)
        progress.end_time = datetime.now().isoformat()
        progress.duration_seconds = 0.1
        return progress

    if already_cleaned:
        logger.info(
            f"Cleaning {len(to_clean)} new IA files ({len(already_cleaned)} already done)..."
        )
    else:
        logger.info(f"Cleaning OCR in {len(source_files)} IA files...")

    args = ["batch", str(ia_dir), "-o", str(cleaned_dir)]
    monitor = ProgressMonitor(cleaned_dir, len(source_files), "files")
    success = run_tc_command("tc-ocr-clean", args, logger, monitor)

    if success:
        cleaned_files = list(cleaned_dir.rglob("*.txt"))
        progress.items_completed = len(cleaned_files)
        logger.info(f"Cleaned {len(cleaned_files)} IA files")
    else:
        progress.errors = 1
        # Still count what we got
        cleaned_files = list(cleaned_dir.rglob("*.txt"))
        progress.items_completed = len(cleaned_files)
        logger.warning(f"OCR cleanup had issues, got {len(cleaned_files)} files")

    progress.items_total = progress.items_completed
    progress.end_time = datetime.now().isoformat()
    progress.duration_seconds = (
        datetime.fromisoformat(progress.end_time) - datetime.fromisoformat(progress.start_time)
    ).total_seconds()
    return progress


def stage_vocab_extract(state: CollectionState, logger: logging.Logger) -> StageProgress:
    """Extract vocabulary from cleaned files for review before SymSpell correction."""
    progress = StageProgress(start_time=datetime.now().isoformat())

    # Find cleaned directories
    sources = []
    gutenberg_dir = Config.raw_dir() / "gutenberg" / "en"

    # Check for cleaned IA files (single directory)
    cleaned_ia = Config.cleaned_dir() / "ia"
    if cleaned_ia.exists() and list(cleaned_ia.rglob("*.txt")):
        sources.append(cleaned_ia)

    # Include Gutenberg
    if gutenberg_dir.exists() and list(gutenberg_dir.glob("*.txt")):
        sources.append(gutenberg_dir)

    if not sources:
        logger.warning("No sources found for vocabulary extraction")
        progress.end_time = datetime.now().isoformat()
        progress.duration_seconds = 0.1
        return progress

    # Count total files
    total_files = sum(len(list(s.rglob("*.txt"))) for s in sources)
    progress.items_total = total_files

    vocab_dir = Config.OUTPUT_BASE / "vocab_review"
    vocab_dir.mkdir(parents=True, exist_ok=True)
    vocab_file = vocab_dir / "vocabulary_candidates.json"

    logger.info(f"Extracting vocabulary from {total_files} files across {len(sources)} sources...")

    # Run vocab extraction on each source
    for source in sources:
        source_name = source.name
        logger.info(f"  Processing {source_name}...")

        args = [str(source), "-o", str(vocab_file), "--min-freq", "3"]
        if vocab_file.exists():
            args.append("--merge")  # Merge with existing

        success = run_tc_command("tc-ocr-vocab", args, logger)

        if not success:
            progress.errors += 1
            logger.warning(f"Vocab extraction had issues for {source_name}")

    # Count results
    if vocab_file.exists():
        import json

        with open(vocab_file) as f:
            vocab_data = json.load(f)
        candidate_count = len(vocab_data.get("candidates", []))
        progress.items_completed = candidate_count
        logger.info(f"Vocabulary extraction complete: {candidate_count} candidates for review")
        logger.info(f"Review file: {vocab_file}")

    progress.end_time = datetime.now().isoformat()
    progress.duration_seconds = (
        datetime.fromisoformat(progress.end_time) - datetime.fromisoformat(progress.start_time)
    ).total_seconds()
    return progress


def stage_vocab_review(state: CollectionState, logger: logging.Logger) -> StageProgress:
    """Human review checkpoint - pipeline stops here until manually resumed."""
    progress = StageProgress(start_time=datetime.now().isoformat())

    vocab_dir = Config.OUTPUT_BASE / "vocab_review"
    vocab_file = vocab_dir / "vocabulary_candidates.json"
    approved_file = vocab_dir / "approved_vocabulary.json"

    print()
    logger.info("=" * 60)
    logger.info("VOCABULARY REVIEW CHECKPOINT")
    logger.info("=" * 60)
    print()
    print("The pipeline has stopped for human review of vocabulary candidates.")
    print()
    print(f"1. Review candidates in: {vocab_file}")
    print(f"2. Create approved list: {approved_file}")
    print()
    print("You can review interactively with:")
    print(f"  uv run tc-ocr-vocab review {vocab_file} -o {approved_file}")
    print()
    print("Or use auto-approval for capitalized words:")
    print(
        f"  uv run tc-ocr-vocab review {vocab_file} -o {approved_file} --auto-approve-capitalized"
    )
    print()
    print("Once review is complete, resume the pipeline with:")
    print("  uv run python scripts/collect_prewwi_corpus.py --resume")
    print()
    logger.info("=" * 60)

    # Check if approved file exists (allows continuing)
    if approved_file.exists():
        logger.info("Approved vocabulary file found - continuing pipeline")
        progress.items_completed = 1
    else:
        logger.info("Waiting for vocabulary review...")
        # Mark as incomplete so pipeline stops
        progress.errors = 1  # This will trigger a stop

    progress.items_total = 1
    progress.end_time = datetime.now().isoformat()
    progress.duration_seconds = (
        datetime.fromisoformat(progress.end_time) - datetime.fromisoformat(progress.start_time)
    ).total_seconds()
    return progress


def stage_dedup(state: CollectionState, logger: logging.Logger) -> StageProgress:
    progress = StageProgress(start_time=datetime.now().isoformat())
    sources = []
    source_counts = []

    gutenberg_dir = Config.raw_dir() / "gutenberg" / "en"
    if gutenberg_dir.exists():
        files = list(gutenberg_dir.glob("*.txt"))
        if files:
            sources.append(str(gutenberg_dir))
            source_counts.append(("Gutenberg", len(files)))

    # Check IA directory (single directory, no books/newspapers split)
    ia_cleaned = Config.cleaned_dir() / "ia"
    ia_raw = Config.raw_dir() / "ia"

    use_dir = ia_cleaned if ia_cleaned.exists() and list(ia_cleaned.rglob("*.txt")) else ia_raw
    if use_dir.exists():
        files = list(use_dir.rglob("*.txt"))
        if files:
            sources.append(str(use_dir))
            source_counts.append(("IA", len(files)))

    if not sources:
        logger.warning("No sources found for deduplication")
        progress.end_time = datetime.now().isoformat()
        progress.duration_seconds = 0.1
        return progress

    total_input = sum(c for _, c in source_counts)
    logger.info(f"Deduplicating {len(sources)} sources ({total_input} total files):")
    for name, count in source_counts:
        print(f"    - {name}: {count} files")

    args = ["merge", *sources, "-o", str(Config.deduped_dir()), "--prefer", "gutenberg"]

    monitor = ProgressMonitor(Config.deduped_dir(), total_input, "files")
    success = run_tc_command("tc-dedup", args, logger, monitor)

    if success:
        files = list(Config.deduped_dir().rglob("*.txt"))
        progress.items_completed = len(files)
        progress.items_total = total_input
        progress.bytes_downloaded = sum(f.stat().st_size for f in files)
        removed = total_input - len(files)
        logger.info(
            f"Deduplication complete: {progress.items_completed} unique files "
            f"({removed} duplicates removed), {format_size(progress.bytes_downloaded)}"
        )
    else:
        progress.errors = 1
        logger.error("Deduplication failed")

    progress.end_time = datetime.now().isoformat()
    progress.duration_seconds = (
        datetime.fromisoformat(progress.end_time) - datetime.fromisoformat(progress.start_time)
    ).total_seconds()
    return progress


def stage_finalize(state: CollectionState, logger: logging.Logger) -> StageProgress:
    progress = StageProgress(start_time=datetime.now().isoformat())
    total_files = 0
    total_bytes = 0

    search_dir = (
        Config.deduped_dir() if list(Config.deduped_dir().rglob("*.txt")) else Config.raw_dir()
    )

    for txt_file in search_dir.rglob("*.txt"):
        total_files += 1
        total_bytes += txt_file.stat().st_size

    total_duration = sum(
        info.get("duration_seconds", 0) for info in state.stages_completed.values()
    )

    summary = {
        "mode": state.mode,
        "cutoff_year": Config.CUTOFF_YEAR,
        "language": Config.LANGUAGE,
        "total_files": total_files,
        "total_bytes": total_bytes,
        "total_size": format_size(total_bytes),
        "total_duration": format_duration(total_duration),
        "started_at": state.started_at,
        "completed_at": datetime.now().isoformat(),
        "stages": dict(state.stages_completed),
        "output_directory": str(Config.OUTPUT_BASE),
    }

    summary_file = Config.metadata_dir() / "collection_summary.json"
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)

    print()
    logger.info("=" * 60)
    logger.info("COLLECTION COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total files: {total_files:,}")
    logger.info(f"Total size: {format_size(total_bytes)}")
    logger.info(f"Total time: {format_duration(total_duration)}")
    logger.info(f"Output: {search_dir}")
    logger.info(f"Summary: {summary_file}")
    logger.info("=" * 60)

    progress.items_total = 1
    progress.items_completed = 1
    progress.end_time = datetime.now().isoformat()
    progress.duration_seconds = (
        datetime.fromisoformat(progress.end_time) - datetime.fromisoformat(progress.start_time)
    ).total_seconds()
    state.total_files = total_files
    state.total_bytes = total_bytes
    return progress


# =============================================================================
# Pipeline Orchestration
# =============================================================================

STAGE_HANDLERS = {
    Stage.INIT: stage_init,
    Stage.GUTENBERG: stage_gutenberg,
    Stage.IA_INDEX: stage_ia_index,
    Stage.IA_ENRICH: stage_ia_enrich,
    Stage.IA_DOWNLOAD: stage_ia_download,
    Stage.VALIDATE: stage_validate,
    Stage.OCR_CLEAN: stage_ocr_clean,
    Stage.VOCAB_EXTRACT: stage_vocab_extract,
    Stage.VOCAB_REVIEW: stage_vocab_review,
    Stage.DEDUP: stage_dedup,
    Stage.FINALIZE: stage_finalize,
}


def print_status(state: CollectionState):
    print("\n" + "=" * 60)
    print("COLLECTION STATUS")
    print("=" * 60)
    print(f"Mode: {state.mode}")
    print(f"Current stage: {state.current_stage}")
    print(f"Started: {state.started_at or 'Not started'}")

    elapsed = state.get_elapsed_time()
    remaining = state.estimate_remaining_time()
    print(f"Elapsed: {format_duration(elapsed.total_seconds())}")
    print(f"Est. remaining: {format_duration(remaining.total_seconds())}")
    print()

    failed = state.get_failed_stages()
    if failed:
        print(f"Failed stages: {', '.join(s.value for s in failed)}")
        print("  (use --retry-failed to re-run these)")
        print()

    print("Stage Progress:")
    for stage in STAGE_ORDER:
        if stage == Stage.COMPLETE:
            continue

        if state.is_stage_completed(stage):
            info = state.stages_completed.get(stage.value, {})
            duration = format_duration(info.get("duration_seconds", 0))
            items = info.get("items_completed", 0)
            bytes_dl = info.get("bytes_downloaded", 0)
            size_str = f", {format_size(bytes_dl)}" if bytes_dl > 0 else ""
            errors = info.get("errors", 0)
            err_str = " [FAILED]" if errors > 0 else ""
            print(f"  [x] {stage.value}: {items} items in {duration}{size_str}{err_str}")
        elif stage.value == state.current_stage:
            print(f"  [>] {stage.value}: IN PROGRESS")
        else:
            print(f"  [ ] {stage.value}")

    print()
    print(f"Output: {Config.OUTPUT_BASE}")
    print("=" * 60 + "\n")


def run_single_stage(state: CollectionState, stage: Stage, logger: logging.Logger):
    """Run a single stage (for --stage mode)."""
    logger.info("=" * 60)
    logger.info(f"RUNNING STAGE: {stage.value.upper()}")
    logger.info("=" * 60)

    # Clear previous completion status so it runs fresh
    state.clear_stage(stage)
    state.mark_stage_started(stage)
    state.save(Config.state_file())

    handler = STAGE_HANDLERS.get(stage)
    if handler:
        try:
            progress = handler(state, logger)
            state.mark_stage_completed(stage, progress)
            state.save(Config.state_file())

            duration = format_duration(progress.duration_seconds)
            if progress.errors > 0:
                logger.warning(f"Stage {stage.value} completed with errors in {duration}")
            else:
                logger.info(f"Stage {stage.value} completed in {duration}")
        except Exception as e:
            logger.error(f"Stage {stage.value} failed: {e}")
            state.save(Config.state_file())
            raise


def run_pipeline(
    state: CollectionState,
    logger: logging.Logger,
    retry_failed: bool = False,
    stages_to_run: Optional[list] = None,
):
    if state.started_at is None:
        state.started_at = datetime.now().isoformat()

    # If retry_failed, clear failed stages
    if retry_failed:
        failed = state.get_failed_stages()
        if failed:
            logger.info(
                f"Retrying {len(failed)} failed stages: {', '.join(s.value for s in failed)}"
            )
            for stage in failed:
                state.clear_stage(stage)

    # Determine which stages to run
    if stages_to_run:
        stages = stages_to_run
    else:
        current_stage = state.get_current_stage()
        start_idx = STAGE_ORDER.index(current_stage)
        stages = STAGE_ORDER[start_idx:]

        # If retrying failed stages, include them even if before current stage
        if retry_failed:
            failed = state.get_failed_stages()
            for failed_stage in failed:
                failed_idx = STAGE_ORDER.index(failed_stage)
                if failed_idx < start_idx:
                    stages = [failed_stage] + stages

    for stage in stages:
        if stage == Stage.COMPLETE:
            logger.info("Pipeline complete!")
            break

        if state.is_stage_completed(stage):
            logger.info(f"Skipping completed stage: {stage.value}")
            continue

        elapsed = format_duration(state.get_elapsed_time().total_seconds())
        remaining = format_duration(state.estimate_remaining_time().total_seconds())

        print()
        logger.info("=" * 60)
        logger.info(f"STAGE: {stage.value.upper()} - {STAGE_DESCRIPTIONS.get(stage, '')}")
        logger.info(f"Elapsed: {elapsed} | Est. remaining: {remaining}")
        logger.info("=" * 60)

        state.mark_stage_started(stage)
        state.save(Config.state_file())

        handler = STAGE_HANDLERS.get(stage)
        if handler:
            try:
                progress = handler(state, logger)
                state.mark_stage_completed(stage, progress)
                state.save(Config.state_file())

                duration = format_duration(progress.duration_seconds)
                if progress.errors > 0:
                    logger.warning(f"Stage {stage.value} completed with errors in {duration}")
                else:
                    logger.info(f"Stage {stage.value} completed in {duration}")
            except Exception as e:
                logger.error(f"Stage {stage.value} failed: {e}")
                state.save(Config.state_file())
                raise

    state.current_stage = Stage.COMPLETE.value
    state.save(Config.state_file())


# =============================================================================
# Main Entry Point
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Pre-WWI Corpus Collection Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Mini validation run (~5-10 minutes)
  uv run python scripts/collect_prewwi_corpus.py --mode mini
  
  # Full collection (~3-5 days)
  uv run python scripts/collect_prewwi_corpus.py --mode full
  
  # Resume interrupted run
  uv run python scripts/collect_prewwi_corpus.py --resume
  
  # Retry stages that failed
  uv run python scripts/collect_prewwi_corpus.py --resume --retry-failed
  
  # Run/retry a specific stage
  uv run python scripts/collect_prewwi_corpus.py --stage ia_books
  
  # Custom output directory
  uv run python scripts/collect_prewwi_corpus.py --mode full --output /data/corpus
  
  # Check status
  uv run python scripts/collect_prewwi_corpus.py --status

Stages: init, gutenberg, ia_books, ia_newspapers, validate, ocr_clean, dedup, finalize
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["mini", "full"],
        default="mini",
        help="mini: ~200 items for testing, full: complete corpus",
    )
    parser.add_argument(
        "--output", "-o", type=str, default=None, help="Output directory (default: ./corpus-prewwi)"
    )
    parser.add_argument("--resume", action="store_true", help="Resume from last saved state")
    parser.add_argument(
        "--retry-failed", action="store_true", help="Re-run stages that previously failed"
    )

    parser.add_argument(
        "--stage", type=str, default=None, help="Run a specific stage only (e.g., ia_books)"
    )
    parser.add_argument("--status", action="store_true", help="Show current status and exit")
    parser.add_argument(
        "--reset", action="store_true", help="Reset state file (does not delete downloaded files)"
    )

    args = parser.parse_args()

    Config.init(args.output)
    Config.OUTPUT_BASE.mkdir(parents=True, exist_ok=True)

    state = CollectionState.load(Config.state_file())

    if args.status:
        print_status(state)
        return

    if args.reset:
        if Config.state_file().exists():
            Config.state_file().unlink()
        print("State reset.")
        return

    logger = setup_logging(Config.log_file())

    # Handle --stage flag for single stage execution
    if args.stage:
        try:
            stage = Stage(args.stage)
        except ValueError:
            print(f"Unknown stage: {args.stage}")
            print(f"Valid stages: {', '.join(s.value for s in STAGE_ORDER if s != Stage.COMPLETE)}")
            return

        logger.info(f"Running single stage: {stage.value}")
        run_single_stage(state, stage, logger)
        return

    if args.resume or args.retry_failed:
        if not Config.state_file().exists():
            print("No saved state. Use --mode to start new collection.")
            return
        if args.retry_failed:
            failed = state.get_failed_stages()
            if failed:
                print(f"Will retry failed stages: {', '.join(s.value for s in failed)}")
            else:
                print("No failed stages to retry.")
        else:
            print(f"Resuming from: {state.current_stage}")
    else:
        state = CollectionState(mode=args.mode)

    print()
    logger.info("=" * 60)
    logger.info("PRE-WWI CORPUS COLLECTION")
    logger.info(f"Mode: {state.mode} | Cutoff: {Config.CUTOFF_YEAR} | Language: {Config.LANGUAGE}")
    logger.info(f"Output: {Config.OUTPUT_BASE}")
    logger.info("=" * 60)

    try:
        run_pipeline(
            state,
            logger,
            retry_failed=args.retry_failed,
        )
    except KeyboardInterrupt:
        print()
        logger.info("Interrupted. State saved. Use --resume to continue.")
        state.save(Config.state_file())
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        state.save(Config.state_file())
        raise


if __name__ == "__main__":
    main()
