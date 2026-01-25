"""Type stubs for rust_ocr_clean Rust extension module.

This module provides high-performance OCR text cleanup, vocabulary extraction,
document triage, and language detection functions implemented in Rust.
"""

from typing import Optional

# =============================================================================
# Classes (PyO3 exported)
# =============================================================================

class WordInfo:
    """Information about a word extracted from text."""

    word: str
    """The word as found (preserving case)."""
    word_lower: str
    """Lowercase version of the word."""
    is_capitalized: bool
    """Whether the word was seen capitalized."""
    is_suspicious: bool
    """Whether the word has suspicious OCR patterns."""
    suspicious_reason: str
    """Reason for suspicion (empty if not suspicious)."""
    context: str
    """Surrounding context from first occurrence."""

class TriageResult:
    """Result of document quality triage."""

    path: str
    """File path (empty for text-based triage)."""
    action: str
    """Triage action: 'pass', 'quarantine', or 'reject'."""
    problems: list[str]
    """List of quality problems detected."""
    alpha_ratio: float
    """Ratio of alphabetic characters (0.0-1.0)."""
    line_length_cv: float
    """Coefficient of variation of line lengths."""
    mean_words_per_line: float
    """Average words per line."""
    fragment_ratio: float
    """Ratio of fragment lines (very short lines)."""
    list_pattern_ratio: float
    """Ratio of lines matching list patterns."""
    line_count: int
    """Total number of lines."""
    char_count: int
    """Total character count."""

class CleanupResult:
    """Result of OCR cleanup with category breakdown."""

    text: str
    """The cleaned text."""
    total_substitutions: int
    """Total number of substitutions made."""
    substitutions_by_category: dict[str, int]
    """Substitution counts by category (e.g., 'li_h_confusion': 45, 'long_s': 123)."""

class LangDetectResult:
    """Result of language detection."""

    is_english: bool
    """Whether the text is detected as English (above confidence threshold)."""
    detected_lang: str
    """Detected language code (e.g., 'eng', 'deu', 'fra')."""
    confidence: float
    """Detection confidence (0.0-1.0)."""

class PreprocessResult:
    """Result of text preprocessing (unicode fix + language detection)."""

    is_english: bool
    """Whether the text is English."""
    detected_lang: str
    """Detected language code."""
    lang_confidence: float
    """Language detection confidence."""
    unicode_was_fixed: bool
    """Whether unicode normalization made changes."""

# =============================================================================
# OCR Cleanup Functions
# =============================================================================

def clean_text(text: str) -> tuple[str, int]:
    """Clean OCR text using pattern-based corrections.

    Applies 150+ OCR correction patterns for common misrecognitions
    (h/li confusion, watermarks, common word errors, etc.)

    Args:
        text: Input text to clean.

    Returns:
        Tuple of (cleaned_text, substitution_count).
    """
    ...

def clean_text_with_categories(text: str) -> CleanupResult:
    """Clean OCR text and return detailed category breakdown.

    Same as clean_text but returns a CleanupResult with substitutions
    broken down by category (li_h_confusion, long_s, watermark, etc.)

    Args:
        text: Input text to clean.

    Returns:
        CleanupResult with text, total_substitutions, and substitutions_by_category.
    """
    ...

def clean_file_to_file(
    input_path: str, output_path: str
) -> tuple[bool, int, int, dict[str, int], list[tuple[str, str, int, int, int]]]:
    """Clean a file and write result to output path.

    Pipeline: strip boilerplate -> OCR cleanup -> write output.

    Args:
        input_path: Path to input file.
        output_path: Path to write cleaned output.

    Returns:
        Tuple of (was_modified, substitution_count, bytes_read, categories_dict, boilerplate_regions).
        - was_modified: True if any changes were made (boilerplate or OCR cleanup)
        - substitution_count: Number of OCR substitutions made
        - bytes_read: Size of input file in bytes
        - categories_dict: Maps category names (e.g., 'long_s', 'li_h_confusion') to counts
        - boilerplate_regions: List of (category, pattern_name, start_line, end_line, char_count)
    """
    ...

# =============================================================================
# Vocabulary Extraction Functions
# =============================================================================

def extract_vocab_from_file(file_path: str, context_chars: int) -> tuple[int, list[WordInfo]]:
    """Extract vocabulary from a single file.

    Args:
        file_path: Path to file to analyze.
        context_chars: Number of context characters to capture around each word.

    Returns:
        Tuple of (total_word_count, list of WordInfo for unique words).
    """
    ...

def extract_vocab_batch(
    file_paths: list[str], context_chars: int
) -> tuple[int, dict[str, tuple[str, int, bool, bool, str, str]]]:
    """Extract vocabulary from multiple files.

    Args:
        file_paths: List of file paths to analyze.
        context_chars: Number of context characters to capture.

    Returns:
        Tuple of (total_word_count, dict mapping word_lower to
        (word, count, is_capitalized, is_suspicious, suspicious_reason, context)).
    """
    ...

# =============================================================================
# Context Pattern Functions
# =============================================================================

def count_context_patterns(text: str) -> dict[str, int]:
    """Count context-dependent patterns that need review.

    These are patterns that can't be auto-corrected without context
    (e.g., 'lie' which could be 'he' or the verb 'lie').

    Args:
        text: Text to analyze.

    Returns:
        Dict mapping pattern_name to count.
    """
    ...

def count_context_patterns_file(file_path: str) -> dict[str, int]:
    """Count context-dependent patterns in a file.

    Args:
        file_path: Path to file to analyze.

    Returns:
        Dict mapping pattern_name to count.
    """
    ...

def count_context_patterns_batch(file_paths: list[str]) -> dict[str, int]:
    """Count context-dependent patterns across multiple files.

    Args:
        file_paths: List of file paths to analyze.

    Returns:
        Dict mapping pattern_name to total count across all files.
    """
    ...

# =============================================================================
# Batch Processing Classes
# =============================================================================

class BatchStats:
    """Statistics from batch OCR cleanup processing."""

    files_processed: int
    files_modified: int
    files_failed: int
    total_substitutions: int
    total_bytes: int
    long_s_fixes: int
    boilerplate_files: int
    boilerplate_chars: int

class TriageResultWithLang:
    """Triage result with integrated language detection."""

    path: str
    action: str
    problems: list[str]
    alpha_ratio: float
    line_length_cv: float
    mean_words_per_line: float
    fragment_ratio: float
    list_pattern_ratio: float
    line_count: int
    char_count: int
    detected_lang: str
    lang_confidence: float
    is_english: bool

class TriageBatchStats:
    """Statistics from batch triage processing."""

    total: int
    passed: int
    quarantined: int
    rejected: int
    non_english: int

# =============================================================================
# Batch Processing Functions (Rayon parallel)
# =============================================================================

def clean_batch_parallel(
    file_pairs: list[tuple[str, str]], num_threads: int | None = None
) -> BatchStats:
    """Clean multiple files in parallel using Rayon.

    Args:
        file_pairs: List of (input_path, output_path) tuples.
        num_threads: Number of threads (default: 24).

    Returns:
        BatchStats with aggregated statistics.
    """
    ...

def triage_batch_parallel(
    paths: list[str],
    num_threads: int | None = None,
    lang_confidence_threshold: float | None = None,
) -> tuple[list[TriageResultWithLang], TriageBatchStats]:
    """Triage multiple files in parallel with integrated language detection.

    Combines structural triage and language detection in one parallel pass.

    Args:
        paths: List of file paths to triage.
        num_threads: Number of threads (default: 24).
        lang_confidence_threshold: Minimum confidence for English (default: 0.5).

    Returns:
        Tuple of (list of TriageResultWithLang, TriageBatchStats).
    """
    ...

# =============================================================================
# Document Triage Functions
# =============================================================================

def triage_text(text: str, path: str = "") -> TriageResult:
    """Triage text for quality issues.

    Analyzes text using heuristics (alpha ratio, line patterns, etc.)
    and returns pass/quarantine/reject decision.

    Args:
        text: Text to analyze.
        path: Optional path for the result (informational only).

    Returns:
        TriageResult with action and quality metrics.
    """
    ...

def triage_file(path: str) -> TriageResult:
    """Triage a file for quality issues.

    Args:
        path: Path to file to analyze.

    Returns:
        TriageResult with action and quality metrics.
    """
    ...

def triage_batch(paths: list[str]) -> list[TriageResult]:
    """Triage multiple files in parallel.

    Uses multi-threading for performance on large batches.

    Args:
        paths: List of file paths to analyze.

    Returns:
        List of TriageResult, one per input file.
    """
    ...

# =============================================================================
# Language Detection Functions
# =============================================================================

def detect_language(text: str, confidence_threshold: Optional[float] = None) -> LangDetectResult:
    """Detect the language of text.

    Uses whatlang library for fast language detection.
    Samples first 10k characters for speed.

    Args:
        text: Text to analyze.
        confidence_threshold: Minimum confidence to consider English (default 0.5).

    Returns:
        LangDetectResult with detection info.
    """
    ...

def detect_language_file(
    path: str, confidence_threshold: Optional[float] = None
) -> LangDetectResult:
    """Detect the language of a file.

    Args:
        path: Path to file to analyze.
        confidence_threshold: Minimum confidence to consider English (default 0.5).

    Returns:
        LangDetectResult with detection info.
    """
    ...

# =============================================================================
# Unicode Normalization Functions
# =============================================================================

def fix_unicode(text: str) -> str:
    """Fix common Unicode issues in text.

    - Normalizes to NFC form (canonical composition)
    - Fixes common mojibake patterns (UTF-8 misread as Latin-1)
    - Normalizes whitespace characters
    - Fixes double-encoded HTML entities

    Args:
        text: Text to normalize.

    Returns:
        Unicode-normalized text.
    """
    ...

def fix_unicode_file(input_path: str, output_path: Optional[str] = None) -> bool:
    """Fix Unicode issues in a file.

    Args:
        input_path: Path to input file.
        output_path: Path to write output (if None, no output written).

    Returns:
        True if changes were made.
    """
    ...

# =============================================================================
# Preprocessing Functions (Combined Operations)
# =============================================================================

def preprocess_text(
    text: str, confidence_threshold: Optional[float] = None
) -> tuple[str, PreprocessResult]:
    """Preprocess text: fix unicode and detect language.

    Args:
        text: Text to preprocess.
        confidence_threshold: Language confidence threshold.

    Returns:
        Tuple of (fixed_text, PreprocessResult).
    """
    ...

def preprocess_file(
    input_path: str,
    output_path: Optional[str] = None,
    confidence_threshold: Optional[float] = None,
) -> PreprocessResult:
    """Preprocess a file: fix unicode, detect language, optionally write output.

    Only writes output if text is English and output_path is provided.

    Args:
        input_path: Path to input file.
        output_path: Path to write output (optional).
        confidence_threshold: Language confidence threshold.

    Returns:
        PreprocessResult with preprocessing info.
    """
    ...

# =============================================================================
# Dictionary Functions (Multi-language word lookup)
# =============================================================================

def init_dictionaries(dict_dir: str) -> bool:
    """Initialize multi-language dictionaries from a directory.

    Loads Hunspell dictionaries for English, German, French, and Latin.
    Dictionaries are loaded once globally and reused across calls.

    Args:
        dict_dir: Path to directory containing .aff and .dic files.

    Returns:
        True if at least one dictionary was loaded successfully.
    """
    ...

def is_known_word(word: str) -> bool:
    """Check if a word exists in any loaded dictionary.

    Checks English, German, French, and Latin dictionaries.
    Also tries lowercase version of the word.

    Args:
        word: Word to check.

    Returns:
        True if word is found in any dictionary.
    """
    ...

def word_languages(word: str) -> list[str]:
    """Get list of languages that recognize a word.

    Useful for debugging which dictionary matched.

    Args:
        word: Word to check.

    Returns:
        List of language codes (e.g., ['en', 'de']) that recognize the word.
    """
    ...

def dictionaries_loaded() -> bool:
    """Check if dictionaries have been initialized.

    Returns:
        True if init_dictionaries() has been called and at least one dictionary loaded.
    """
    ...

def init_whitelist(words: list[str]) -> int:
    """Initialize the whitelist with known good words to skip during vocab extraction.

    Words in the whitelist will be skipped during vocabulary extraction.
    Comparison is case-insensitive.

    Args:
        words: List of words to whitelist.

    Returns:
        Number of unique words added to the whitelist.
    """
    ...

# =============================================================================
# Line Unwrapping Functions
# =============================================================================

class UnwrapResult:
    """Result of line unwrapping."""

    text: str
    """The unwrapped text."""
    lines_joined: int
    """Number of lines that were joined."""
    words_dehyphenated: int
    """Number of hyphenated words that were rejoined."""
    spaces_normalized: int
    """Number of extra spaces that were normalized."""

def unwrap_lines(text: str) -> UnwrapResult:
    """Unwrap cosmetic line breaks while preserving paragraph structure.

    Removes line breaks that were inserted for printing/display purposes,
    rejoins hyphenated words split across lines, and normalizes whitespace.

    Args:
        text: Text to unwrap.

    Returns:
        UnwrapResult with unwrapped text and statistics.
    """
    ...

def unwrap_lines_file(input_path: str, output_path: str) -> UnwrapResult:
    """Unwrap lines in a file and write to output.

    Args:
        input_path: Path to input file.
        output_path: Path to write output.

    Returns:
        UnwrapResult with statistics.
    """
    ...

def unwrap_lines_batch(input_dir: str, output_dir: str) -> tuple[int, int, int, int]:
    """Batch unwrap lines in multiple files.

    Args:
        input_dir: Directory containing input files.
        output_dir: Directory to write output files.

    Returns:
        Tuple of (files_processed, lines_joined, words_dehyphenated, spaces_normalized).
    """
    ...

# =============================================================================
# Boilerplate Stripping Functions
# =============================================================================

class StrippedRegion:
    """A region that was stripped from a document."""

    category: str
    """Category of boilerplate (e.g., 'google_books', 'internet_archive', 'library_stamp')."""
    pattern_name: str
    """Specific pattern that matched (e.g., 'google_books_disclaimer')."""
    start_line: int
    """Starting line number of the stripped region."""
    end_line: int
    """Ending line number of the stripped region."""
    char_count: int
    """Number of characters stripped."""

class BoilerplateResult:
    """Result of boilerplate stripping."""

    text: str
    """The text with boilerplate removed."""
    stripped_regions: list[StrippedRegion]
    """List of regions that were stripped."""
    total_chars_stripped: int
    """Total number of characters removed."""

def strip_boilerplate(text: str) -> BoilerplateResult:
    """Strip digitization boilerplate from text.

    Removes boilerplate from Google Books, Internet Archive, HathiTrust,
    Microsoft digitization, and library stamps.

    Should run BEFORE OCR cleanup since boilerplate is modern inserted text.

    Args:
        text: Text to process.

    Returns:
        BoilerplateResult with cleaned text and list of stripped regions.
    """
    ...

def strip_boilerplate_file(input_path: str, output_path: Optional[str] = None) -> BoilerplateResult:
    """Strip boilerplate from a file.

    Args:
        input_path: Path to input file.
        output_path: Path to write output (optional).

    Returns:
        BoilerplateResult with cleaned text and stripped regions.
    """
    ...

def strip_boilerplate_batch(input_dir: str, output_dir: str) -> tuple[int, int, int]:
    """Batch strip boilerplate from files in a directory.

    Args:
        input_dir: Directory containing input files.
        output_dir: Directory to write output files.

    Returns:
        Tuple of (files_processed, files_with_boilerplate, total_chars_stripped).
    """
    ...

# =============================================================================
# Noise Word Stripping Functions
# =============================================================================

class StripBatchStats:
    """Statistics from batch noise stripping."""

    files_processed: int
    files_modified: int
    total_words_stripped: int
    total_bytes: int

def init_noise_words(vocab_path: str, categories: list[str] | None = None) -> int:
    """Initialize noise word set from vocab candidates file.

    Filters to only specified categories (default: G and R).

    Args:
        vocab_path: Path to vocab candidates file.
        categories: List of category codes to include (default: ["G", "R"]).

    Returns:
        Count of noise words loaded.
    """
    ...

def noise_words_count() -> int:
    """Get the count of currently loaded noise words.

    Returns:
        Number of noise words in the global set.
    """
    ...

def strip_noise_words(text: str) -> tuple[str, int]:
    """Strip noise words from text.

    Args:
        text: Text to process.

    Returns:
        Tuple of (cleaned_text, words_stripped).
    """
    ...

def strip_noise_file(input_path: str, output_path: str) -> tuple[bool, int, int]:
    """Strip noise words from a file and write to output.

    Args:
        input_path: Path to input file.
        output_path: Path to write output.

    Returns:
        Tuple of (was_modified, words_stripped, bytes_processed).
    """
    ...

def strip_noise_batch_parallel(
    file_pairs: list[tuple[str, str]], num_threads: int
) -> StripBatchStats:
    """Batch strip noise words with Rayon parallelization.

    Args:
        file_pairs: List of (input_path, output_path) tuples.
        num_threads: Number of threads to use.

    Returns:
        StripBatchStats with processing statistics.
    """
    ...

def strip_noise_batch_parallel_logged(
    file_pairs: list[tuple[str, str]], num_threads: int
) -> tuple[StripBatchStats, list[tuple[str, int]]]:
    """Batch strip noise words with per-file logging.

    Same as strip_noise_batch_parallel but also returns a list of
    (path, words_stripped) for files that were modified.

    Args:
        file_pairs: List of (input_path, output_path) tuples.
        num_threads: Number of threads to use.

    Returns:
        Tuple of (StripBatchStats, list of (path, words_stripped) for modified files).
    """
    ...
