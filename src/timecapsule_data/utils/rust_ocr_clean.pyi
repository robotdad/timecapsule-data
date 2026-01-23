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


def clean_file_to_file(input_path: str, output_path: str) -> tuple[bool, int, int]:
    """Clean a file and write result to output path.

    Args:
        input_path: Path to input file.
        output_path: Path to write cleaned output.

    Returns:
        Tuple of (was_modified, substitution_count, bytes_read).
    """
    ...


# =============================================================================
# Vocabulary Extraction Functions
# =============================================================================


def extract_vocab_from_file(
    file_path: str, context_chars: int
) -> tuple[int, list[WordInfo]]:
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
