#!/usr/bin/env python3
"""
OCR Cleanup Module

Repairs common OCR errors in historical texts. This goes beyond filtering -
it actually attempts to fix recognizable error patterns.

Pipeline order:
1. Language detection (skip non-English documents)
2. Whitespace normalization (strip trailing, collapse multiples)
3. Hyphen rejoining (fix line-break hyphenation)
4. Mid-word uppercase normalization (sVo -> svo)
5. OCR substitutions (pattern-based fixes)

Common OCR errors in 19th century texts:
- 'tbe' -> 'the'
- 'arid' -> 'and'
- 'wbich' -> 'which'
- 'tlie' -> 'the'
- 'li' -> 'h' (very common: tliis->this, wliich->which, liim->him)
- Long s (ſ) misread as 'f'
- Ligatures (fi, fl, ff) broken apart
- 'rn' misread as 'm' and vice versa

Usage:
    # Clean a single file
    tc-ocr-clean clean input.txt -o output.txt

    # Clean entire corpus directory
    tc-ocr-clean batch ./corpus_raw -o ./corpus_clean

    # Analyze without changing (show what would be fixed)
    tc-ocr-clean analyze ./corpus_raw --report fixes.json
"""

import argparse
import json
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import rust_ocr_clean


def get_unique_path(path: Path) -> Path:
    """Return a unique path by adding numeric suffix if file exists.

    Examples:
        _cleanup_report.json -> _cleanup_report_1.json -> _cleanup_report_2.json
    """
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    parent = path.parent

    counter = 1
    while True:
        new_path = parent / f"{stem}_{counter}{suffix}"
        if not new_path.exists():
            return new_path
        counter += 1


# =============================================================================
# Preprocessing Functions (applied before OCR substitutions)
# Uses Rust implementations for performance at scale (2M+ docs)
# =============================================================================


def detect_language(text: str, confidence_threshold: float = 0.5) -> tuple[bool, float]:
    """
    Detect if text is primarily English using Rust whatlang.

    Args:
        text: The text to analyze
        confidence_threshold: Minimum confidence to accept detection (default 0.5)

    Returns:
        (is_english, confidence) - is_english is True if detected as English with sufficient confidence
    """
    result = rust_ocr_clean.detect_language(text, confidence_threshold)
    return result.is_english, result.confidence


def fix_unicode(text: str) -> str:
    """
    Fix Unicode issues using Rust implementation.

    Fixes:
    - Mojibake (encoding errors like "Ã©" → "é")
    - Broken HTML entities
    - Unicode whitespace normalization
    - NFC normalization

    Should run BEFORE pattern matching.
    """
    return rust_ocr_clean.fix_unicode(text)


def normalize_whitespace(text: str) -> tuple[str, int]:
    """
    Normalize whitespace in text. Run BEFORE hyphen rejoining.

    - Strip trailing whitespace from lines (important for hyphen detection)
    - Collapse multiple spaces to single space
    - Normalize line endings to \n
    - Remove spaces around hyphens at line ends

    Returns:
        (normalized_text, count_of_changes)
    """
    changes = 0

    # Normalize line endings first
    if "\r\n" in text:
        text = text.replace("\r\n", "\n")
        changes += 1
    if "\r" in text:
        text = text.replace("\r", "\n")
        changes += 1

    # Strip trailing whitespace from each line (critical for hyphen detection)
    lines = text.split("\n")
    stripped_lines = []
    for line in lines:
        stripped = line.rstrip()
        if stripped != line:
            changes += 1
        stripped_lines.append(stripped)
    text = "\n".join(stripped_lines)

    # Collapse multiple spaces to single (but not at line start - preserve indentation)
    original = text
    text = re.sub(r"([^ \n]) {2,}", r"\1 ", text)
    if text != original:
        changes += text.count("  ")  # Rough count

    return text, changes


def rejoin_hyphenated(text: str) -> tuple[str, int]:
    """
    Rejoin words split by end-of-line hyphenation.

    Pattern: word-fragment + hyphen + newline + lowercase continuation
    Example: "de-\npendance" -> "dependance"

    Must run AFTER normalize_whitespace (to handle "word- \n" patterns).

    Returns:
        (rejoined_text, count_of_rejoins)
    """
    # Pattern: letters, hyphen, newline, optional whitespace, lowercase letters
    # Only rejoin if continuation starts lowercase (indicates word continuation)
    pattern = r"([a-zA-Z]{2,})-\n\s*([a-z]{2,})\b"

    count = len(re.findall(pattern, text))
    if count > 0:
        text = re.sub(pattern, r"\1\2", text)

    return text, count


def normalize_midword_caps(text: str) -> tuple[str, int]:
    """
    Fix OCR errors where a letter is incorrectly uppercase mid-word.

    Pattern: lowercase-UPPERCASE-lowercase in middle of word
    Examples: sVo -> svo, tRe -> tre, lVs -> lvs

    These are never intentional in normal text.

    Returns:
        (normalized_text, count_of_fixes)
    """
    # Pattern: lowercase letter followed by uppercase followed by lowercase
    # This catches mid-word caps that are clearly OCR errors
    pattern = r"(?<=[a-z])([A-Z])(?=[a-z])"

    count = len(re.findall(pattern, text))
    if count > 0:
        text = re.sub(pattern, lambda m: m.group(1).lower(), text)

    return text, count


# =============================================================================
# Long-s (ſ) Detection and Fixing
# =============================================================================
# In pre-1800 texts, the "long s" (ſ) was commonly used and OCR misreads it as 'f'.
# Instead of enumerating all variants, we:
# 1. Detect documents with pervasive long-s using marker words
# 2. Apply broad pattern-based fixes only to those documents

# Marker words where ſ→f is unmistakable (these patterns don't occur in normal English)
# Long-s patterns are now consolidated in Rust (rust-ocr-clean/src/lib.rs)
# The Rust module handles all long-s detection and fixing via clean_text()

# Common OCR substitution errors
# Format: (error_pattern, correction, context_required)
# context_required: None = always apply, or regex that must match around the word
OCR_SUBSTITUTIONS = [
    # ==========================================================================
    # 'li' -> 'h' confusion (VERY COMMON in newspaper OCR)
    # This is one of the most frequent OCR errors - 'h' gets misread as 'li'
    # ==========================================================================
    # 'the' variants (most common English word, most common OCR errors)
    (r"\btbe\b", "the", None),
    (r"\btlie\b", "the", None),
    (r"\btiie\b", "the", None),
    (r"\btbc\b", "the", None),
    (r"\bihe\b", "the", None),
    (r"\btne\b", "the", None),
    (r"\bthc\b", "the", None),
    # Additional 'the' variants from vocab analysis (13k+ occurrences each)
    (r"\bllie\b", "the", None),
    (r"\bllic\b", "the", None),
    (r"\bllio\b", "the", None),
    # 'this' variants (5850+ occurrences of tliis)
    (r"\btbis\b", "this", None),
    (r"\bthia\b", "this", None),
    (r"\btliis\b", "this", None),
    # 'that' variants
    (r"\btbat\b", "that", None),
    (r"\btliat\b", "that", None),
    (r"\btlmt\b", "that", None),
    (r"\bthnt\b", "that", None),
    # 'which' variants (4497+ occurrences of wliich)
    (r"\bwbich\b", "which", None),
    (r"\bwhicb\b", "which", None),
    (r"\bwliich\b", "which", None),
    (r"\bwliicli\b", "which", None),
    # 'what' variants
    (r"\bwliat\b", "what", None),
    (r"\bwlmt\b", "what", None),
    # 'when' variants
    (r"\bwlien\b", "when", None),
    (r"\bwben\b", "when", None),
    # 'where' variants
    (r"\bwliere\b", "where", None),
    (r"\bwbere\b", "where", None),
    # 'while' variants
    (r"\bwliile\b", "while", None),
    (r"\bwbile\b", "while", None),
    # 'who' variants
    (r"\bwlio\b", "who", None),
    # 'whose' variants
    (r"\bwliose\b", "whose", None),
    # 'him' variants (2863 occurrences of liim)
    (r"\bliim\b", "him", None),
    (r"\bhirn\b", "him", None),
    # 'his' variants (9347 occurrences of liis)
    (r"\bliis\b", "his", None),
    (r"\bhia\b", "his", None),
    # 'her' variants
    (r"\blier\b", "her", None),
    # 'he' - needs context since 'lie' is a real word
    (r"\blie\b", "he", r"\b(and|but|that|when|if|as|so|because)\s+lie\b"),
    # 'she' variants
    (r"\bslie\b", "she", None),
    # 'they' variants
    (r"\btliey\b", "they", None),
    (r"\btbey\b", "they", None),
    # 'their' variants
    (r"\btbeir\b", "their", None),
    (r"\btlieir\b", "their", None),
    # 'them' variants
    (r"\btbem\b", "them", None),
    (r"\btliem\b", "them", None),
    # 'then' variants
    (r"\btben\b", "then", None),
    (r"\btlien\b", "then", None),
    # 'there' variants
    (r"\btbere\b", "there", None),
    (r"\btliere\b", "there", None),
    # 'these' variants
    (r"\btbese\b", "these", None),
    (r"\btliese\b", "these", None),
    # 'those' variants
    (r"\btbose\b", "those", None),
    (r"\btliose\b", "those", None),
    # 'other' variants
    (r"\botber\b", "other", None),
    (r"\botlier\b", "other", None),
    # ==========================================================================
    # Other common OCR substitution errors
    # ==========================================================================
    # 'and' variants
    (r"\barid\b", "and", None),
    (r"\baud\b", "and", None),
    (r"\bnnd\b", "and", None),
    (r"\baiid\b", "and", None),
    # 'with' variants
    (r"\bwitb\b", "with", None),
    (r"\bwitli\b", "with", None),
    # 'have' variants
    (r"\bhavo\b", "have", None),
    (r"\bbave\b", "have", None),
    (r"\bliave\b", "have", None),
    # 'been' variants
    (r"\bboen\b", "been", None),
    # 'from' variants
    (r"\bfrorn\b", "from", None),
    # 'were' variants
    (r"\bwero\b", "were", None),
    # 'would' variants
    (r"\bwonld\b", "would", None),
    (r"\bwouid\b", "would", None),
    # 'could' variants
    (r"\bconld\b", "could", None),
    (r"\bcouid\b", "could", None),
    # 'should' variants
    (r"\bsbould\b", "should", None),
    (r"\bshouid\b", "should", None),
    # 'being' variants
    (r"\bbeiug\b", "being", None),
    # 'made' variants
    (r"\bmado\b", "made", None),
    # 'upon' variants
    (r"\bnpon\b", "upon", None),
    # 'such' variants
    (r"\bsucb\b", "such", None),
    (r"\bsucli\b", "such", None),
    # 'some' variants
    (r"\bsomo\b", "some", None),
    # 'very' variants
    (r"\bverv\b", "very", None),
    # 'first' variants (2490 occurrences of llrst)
    (r"\bllrst\b", "first", None),
    (r"\bfirst\b", "first", None),
    # 'still' variants (3097 occurrences - long s confusion)
    (r"\bftill\b", "still", None),
    # ==========================================================================
    # Long s (ſ) -> s (common in pre-1800 texts)
    # ==========================================================================
    (r"ſ", "s", None),
    # ==========================================================================
    # Common 'rn' <-> 'm' confusion
    # ==========================================================================
    (r"\brnay\b", "may", None),
    (r"\brnuch\b", "much", None),
    (r"\brnore\b", "more", None),
    (r"\bsarne\b", "same", None),
    (r"\btirne\b", "time", None),
    (r"\bnarne\b", "name", None),
    (r"\bcorne\b", "come", None),
    (r"\bhorne\b", "home", None),
    # ==========================================================================
    # 'ii' -> 'n' confusion
    # ==========================================================================
    (r"\bkiiow\b", "know", None),
    (r"\bkiiown\b", "known", None),
    # ==========================================================================
    # Common 'cl' -> 'd' confusion
    # ==========================================================================
    (r"\bclo\b", "do", r"\b(to|not|can|will|shall|would|could)\s+clo\b"),
    # ==========================================================================
    # Fix broken ligatures (fi, fl, ff, ffi, ffl) - Unicode ligature characters
    # ==========================================================================
    (r"ﬁ", "fi", None),
    (r"ﬂ", "fl", None),
    (r"ﬀ", "ff", None),
    (r"ﬃ", "ffi", None),
    (r"ﬄ", "ffl", None),
    # ==========================================================================
    # Ligature corruption: fiil -> ful (fi ligature + u mangled)
    # Common English -ful suffix words where OCR rendered 'ful' as 'fiil'
    # ==========================================================================
    # High frequency (500+ occurrences)
    (r"\b[Bb]eautifiil\b", "beautiful", None),
    (r"\b[Bb]eautifiilly\b", "beautifully", None),
    (r"\b[Uu]sefiil\b", "useful", None),
    (r"\b[Uu]sefiilness\b", "usefulness", None),
    (r"\b[Pp]owerfiil\b", "powerful", None),
    (r"\b[Pp]owerfiilly\b", "powerfully", None),
    (r"\b[Aa]wfiil\b", "awful", None),
    (r"\b[Aa]wfiilly\b", "awfully", None),
    (r"\b[Cc]arefiil\b", "careful", None),
    (r"\b[Cc]arefiilly\b", "carefully", None),
    (r"\b[Ss]uccessfiil\b", "successful", None),
    (r"\b[Ss]uccessfiilly\b", "successfully", None),
    (r"\b[Ff]aithfiil\b", "faithful", None),
    (r"\b[Ff]aithfiilly\b", "faithfully", None),
    (r"\b[Ll]awfiil\b", "lawful", None),
    (r"\b[Ll]awfiilly\b", "lawfully", None),
    (r"\b[Pp]ainfiil\b", "painful", None),
    (r"\b[Pp]ainfiilly\b", "painfully", None),
    (r"\b[Dd]oubtfiil\b", "doubtful", None),
    (r"\b[Dd]readfiil\b", "dreadful", None),
    (r"\b[Dd]readfiilly\b", "dreadfully", None),
    (r"\b[Ff]earfiil\b", "fearful", None),
    (r"\b[Ff]earfiilly\b", "fearfully", None),
    (r"\b[Gg]ratefiil\b", "grateful", None),
    (r"\b[Gg]ratefiilly\b", "gratefully", None),
    (r"\b[Gg]racefiil\b", "graceful", None),
    (r"\b[Gg]racefiilly\b", "gracefully", None),
    (r"\b[Pp]eacefiil\b", "peaceful", None),
    (r"\b[Pp]eacefiilly\b", "peacefully", None),
    (r"\b[Nn]eedfiil\b", "needful", None),
    (r"\b[Ss]kilfiil\b", "skilful", None),
    (r"\b[Ss]kilfiilly\b", "skilfully", None),
    (r"\b[Yy]outhfiil\b", "youthful", None),
    (r"\b[Ss]infiil\b", "sinful", None),
    (r"\b[Mm]ercifiil\b", "merciful", None),
    (r"\b[Mm]ercifiilly\b", "mercifully", None),
    (r"\b[Jj]oyfiil\b", "joyful", None),
    (r"\b[Jj]oyfiilly\b", "joyfully", None),
    (r"\b[Tt]hankfiil\b", "thankful", None),
    (r"\b[Tt]hankfiilly\b", "thankfully", None),
    (r"\b[Uu]nlawfiil\b", "unlawful", None),
    (r"\b[Uu]nlawfiilly\b", "unlawfully", None),
    (r"\b[Ww]ilfiil\b", "wilful", None),
    (r"\b[Ww]ilfiilly\b", "wilfully", None),
    (r"\b[Ff]ruitfiil\b", "fruitful", None),
    (r"\b[Pp]lentifiil\b", "plentiful", None),
    (r"\b[Pp]lentifiilly\b", "plentifully", None),
    (r"\b[Ff]rightfiil\b", "frightful", None),
    (r"\b[Hh]andfiil\b", "handful", None),
    (r"\b[Rr]espectfiil\b", "respectful", None),
    (r"\b[Rr]espectfiilly\b", "respectfully", None),
    (r"\b[Uu]nsuccessfiil\b", "unsuccessful", None),
    (r"\b[Ww]onderfiil\b", "wonderful", None),
    (r"\b[Ww]onderfiilly\b", "wonderfully", None),
    (r"\b[Hh]opeful\b", "hopeful", None),
    (r"\b[Hh]opefiil\b", "hopeful", None),
    (r"\b[Hh]elpfiil\b", "helpful", None),
    (r"\b[Hh]armfiil\b", "harmful", None),
    (r"\b[Hh]atefiil\b", "hateful", None),
    (r"\b[Ss]hameful\b", "shameful", None),
    (r"\b[Ss]hamefiil\b", "shameful", None),
    (r"\b[Dd]isgraceful\b", "disgraceful", None),
    (r"\b[Dd]isgracefiil\b", "disgraceful", None),
    (r"\b[Cc]heerfiil\b", "cheerful", None),
    (r"\b[Cc]heerfiilly\b", "cheerfully", None),
    (r"\b[Ww]atchfiil\b", "watchful", None),
    (r"\b[Dd]eceitfiil\b", "deceitful", None),
    (r"\b[Bb]oastfiil\b", "boastful", None),
    (r"\b[Tt]houghtfiil\b", "thoughtful", None),
    (r"\b[Dd]istressfiil\b", "distressful", None),
    (r"\b[Dd]istrustfiil\b", "distrustful", None),
    (r"\b[Rr]emorsefiil\b", "remorseful", None),
    (r"\b[Rr]eproachfiil\b", "reproachful", None),
    (r"\b[Rr]esentfiil\b", "resentful", None),
    (r"\b[Rr]estfiil\b", "restful", None),
    (r"\b[Rr]evengefiil\b", "revengeful", None),
    (r"\b[Ss]cornfiil\b", "scornful", None),
    (r"\b[Ss]cornfiilly\b", "scornfully", None),
    (r"\b[Ss]pitefiil\b", "spiteful", None),
    (r"\b[Tt]astefiil\b", "tasteful", None),
    (r"\b[Tt]ruthfiil\b", "truthful", None),
    (r"\b[Ww]istfiil\b", "wistful", None),
    (r"\b[Ww]rathfiil\b", "wrathful", None),
    (r"\b[Ff]ancifiil\b", "fanciful", None),
    (r"\b[Bb]ountifiil\b", "bountiful", None),
    (r"\b[Dd]utifiil\b", "dutiful", None),
    (r"\b[Pp]itifiil\b", "pitiful", None),
    (r"\b[Pp]layfiil\b", "playful", None),
    (r"\b[Pp]rayerfiil\b", "prayerful", None),
    (r"\b[Mm]ournfiil\b", "mournful", None),
    (r"\b[Dd]olefiil\b", "doleful", None),
    (r"\b[Ww]oefiil\b", "woeful", None),
    (r"\b[Ff]orgetfiil\b", "forgetful", None),
    (r"\b[Nn]eglectfiil\b", "neglectful", None),
    (r"\b[Uu]ngratefiil\b", "ungrateful", None),
    (r"\b[Uu]nmindfiil\b", "unmindful", None),
    (r"\b[Uu]nfaithfiil\b", "unfaithful", None),
    (r"\b[Hh]ealthfiil\b", "healthful", None),
    # Measurement words
    (r"\b[Tt]easpoonfiil\b", "teaspoonful", None),
    (r"\b[Tt]ablespoonfiil\b", "tablespoonful", None),
    (r"\b[Ss]poonfiil\b", "spoonful", None),
    (r"\b[Cc]upfiil\b", "cupful", None),
    (r"\b[Mm]outhfiil\b", "mouthful", None),
    # ==========================================================================
    # Ligature corruption: ofl[A-Z] -> off (ff ligature mangled)
    # Common words where 'ff' ligature was corrupted to 'fl' + uppercase
    # ==========================================================================
    # Office/Officer variants (very high frequency)
    (r"\b[Oo]fl[BSFT]ce\b", "office", None),
    (r"\b[Oo]fl[BSFTR]cer\b", "officer", None),
    (r"\b[Oo]fl[BSFTR]cers\b", "officers", None),
    (r"\b[Oo]fl[BSFTR]ces\b", "offices", None),
    (r"\b[Oo]fl[BSFTR]cial\b", "official", None),
    (r"\b[Oo]fl[BSFTR]cials\b", "officials", None),
    (r"\b[Oo]fl[BSFTR]cially\b", "officially", None),
    # Offer/Offered variants
    (r"\b[Oo]fl[FT]er\b", "offer", None),
    (r"\b[Oo]fl[FT]ered\b", "offered", None),
    (r"\b[Oo]fl[FT]ering\b", "offering", None),
    (r"\b[Oo]fl[FT]erings\b", "offerings", None),
    (r"\b[Oo]fl[FT]ers\b", "offers", None),
    # Offend variants
    (r"\b[Oo]fl[FT]end\b", "offend", None),
    (r"\b[Oo]fl[FT]ended\b", "offended", None),
    (r"\b[Oo]fl[FT]ender\b", "offender", None),
    (r"\b[Oo]fl[FT]enders\b", "offenders", None),
    (r"\b[Oo]fl[FT]ense\b", "offense", None),
    (r"\b[Oo]fl[FT]enses\b", "offenses", None),
    (r"\b[Oo]fl[FT]ensive\b", "offensive", None),
    # Coffee variants
    (r"\b[Cc]ofl[FSTV]ee\b", "coffee", None),
    # Coffin variants
    (r"\b[Cc]ofl[BSFT][inu]\b", "coffin", None),
    (r"\b[Cc]ofl[BSFT]ins\b", "coffins", None),
    # Different variants
    (r"\b[Dd]ifl[FT]erent\b", "different", None),
    (r"\b[Dd]ifl[FT]erence\b", "difference", None),
    (r"\b[Dd]ifl[FT]erences\b", "differences", None),
    # Suffer variants
    (r"\b[Ss]ufl[FT]er\b", "suffer", None),
    (r"\b[Ss]ufl[FT]ered\b", "suffered", None),
    (r"\b[Ss]ufl[FT]ering\b", "suffering", None),
    (r"\b[Ss]ufl[FT]erings\b", "sufferings", None),
    (r"\b[Ss]ufl[FT]icient\b", "sufficient", None),
    (r"\b[Ss]ufl[FT]iciently\b", "sufficiently", None),
    # Effect variants
    (r"\b[Ee]fl[FT]ect\b", "effect", None),
    (r"\b[Ee]fl[FT]ected\b", "effected", None),
    (r"\b[Ee]fl[FT]ects\b", "effects", None),
    (r"\b[Ee]fl[FT]ective\b", "effective", None),
    # Affair variants
    (r"\b[Aa]fl[FT]air\b", "affair", None),
    (r"\b[Aa]fl[FT]airs\b", "affairs", None),
    # Effort variants
    (r"\b[Ee]fl[FT]ort\b", "effort", None),
    (r"\b[Ee]fl[FT]orts\b", "efforts", None),
    # ==========================================================================
    # Long-s (ſ) corruption: fliip -> ship, fliire -> shire, fliing -> shing
    # In pre-1800 texts, long-s (ſ) looks like 'f' to OCR, and ſh -> fli
    # These are predictable suffix patterns safe for literal substitution
    # ==========================================================================
    # -ship suffix words (ſhip -> fliip)
    (r"\b[Ww]orfliip\b", "worship", None),
    (r"\b[Ww]orfliipped\b", "worshipped", None),
    (r"\b[Ww]orfliipping\b", "worshipping", None),
    (r"\b[Ll]ordfliip\b", "lordship", None),
    (r"\b[Ll]ordfliips\b", "lordships", None),
    (r"\b[Ll]ordfliip's\b", "lordship's", None),
    (r"\b[Ff]riendfliip\b", "friendship", None),
    (r"\b[Ff]riendfliips\b", "friendships", None),
    (r"\b[Tt]ownfliip\b", "township", None),
    (r"\b[Tt]ownfliips\b", "townships", None),
    (r"\b[Ff]ellowfliip\b", "fellowship", None),
    (r"\b[Ff]ellowfliips\b", "fellowships", None),
    (r"\b[Cc]ourtfliip\b", "courtship", None),
    (r"\b[Hh]ardfliip\b", "hardship", None),
    (r"\b[Hh]ardfliips\b", "hardships", None),
    (r"\b[Pp]artnerfliip\b", "partnership", None),
    (r"\b[Pp]artnerfliips\b", "partnerships", None),
    (r"\b[Ll]adyfliip\b", "ladyship", None),
    (r"\b[Ww]orkmanfliip\b", "workmanship", None),
    (r"\b[Cc]onfulfliip\b", "consulship", None),
    (r"\b[Kk]ingfliip\b", "kingship", None),
    (r"\b[Ss]cholarfliip\b", "scholarship", None),
    (r"\b[Cc]itizenfliip\b", "citizenship", None),
    (r"\b[Rr]elationfliip\b", "relationship", None),
    (r"\b[Mm]emberfliip\b", "membership", None),
    (r"\b[Oo]wnerfliip\b", "ownership", None),
    (r"\b[Ll]eaderfliip\b", "leadership", None),
    (r"\b[Ss]teamfliip\b", "steamship", None),
    (r"\b[Ww]arfliip\b", "warship", None),
    # -shire suffix words (ſhire -> fliire)
    (r"\b[Yy]orkfliire\b", "Yorkshire", None),
    (r"\b[Hh]ampfliire\b", "Hampshire", None),
    (r"\b[Dd]erbyfliire\b", "Derbyshire", None),
    (r"\b[Dd]evonfliire\b", "Devonshire", None),
    (r"\b[Bb]erkfliire\b", "Berkshire", None),
    (r"\b[Cc]hefliire\b", "Cheshire", None),
    (r"\b[Ll]ancafliire\b", "Lancashire", None),
    (r"\b[Ll]incolnfliire\b", "Lincolnshire", None),
    (r"\b[Oo]xfordfliire\b", "Oxfordshire", None),
    (r"\b[Ww]iltfliire\b", "Wiltshire", None),
    (r"\b[Ww]arwickfliire\b", "Warwickshire", None),
    (r"\b[Nn]orthamptonfliire\b", "Northamptonshire", None),
    (r"\b[Hh]ertfordfliire\b", "Hertfordshire", None),
    (r"\b[Ss]hropfliire\b", "Shropshire", None),
    (r"\b[Ss]taffordfliire\b", "Staffordshire", None),
    (r"\b[Ss]omerfetfliire\b", "Somersetshire", None),
    (r"\b[Ll]eicefterfliire\b", "Leicestershire", None),
    (r"\b[Nn]ottinghamfliire\b", "Nottinghamshire", None),
    (r"\b[Gg]loucefterfliire\b", "Gloucestershire", None),
    (r"\b[Ww]orcefterfliire\b", "Worcestershire", None),
    (r"\b[Nn]ewHampfliire\b", "New Hampshire", None),
    # -shing suffix words (ſhing -> fliing)
    (r"\b[Ww]afliing\b", "washing", None),
    (r"\b[Ww]ifliing\b", "wishing", None),
    (r"\b[Ff]ifliing\b", "fishing", None),
    (r"\b[Ff]inifliing\b", "finishing", None),
    (r"\b[Pp]ublifliing\b", "publishing", None),
    (r"\b[Ff]lourifliing\b", "flourishing", None),
    (r"\b[Ee]ftablifliing\b", "establishing", None),
    (r"\b[Dd]iftinguifliing\b", "distinguishing", None),
    (r"\b[Pp]unifliing\b", "punishing", None),
    (r"\b[Vv]anifliing\b", "vanishing", None),
    (r"\b[Aa]bolifliing\b", "abolishing", None),
    (r"\b[Aa]ccomplifliing\b", "accomplishing", None),
    (r"\b[Nn]ourifliing\b", "nourishing", None),
    (r"\b[Rr]efrefliing\b", "refreshing", None),
    (r"\b[Dd]iminifliing\b", "diminishing", None),
    (r"\b[Aa]ftonifliing\b", "astonishing", None),
    (r"\b[Pp]erifliing\b", "perishing", None),
    (r"\b[Ll]anguifliing\b", "languishing", None),
    (r"\b[Ff]urnifliing\b", "furnishing", None),
    (r"\b[Bb]lufliing\b", "blushing", None),
    (r"\b[Rr]ufliing\b", "rushing", None),
    (r"\b[Pp]ufliing\b", "pushing", None),
    (r"\b[Cc]rufliing\b", "crushing", None),
    (r"\b[Bb]rufliing\b", "brushing", None),
    (r"\b[Ff]lafliing\b", "flashing", None),
    (r"\b[Ss]plafliing\b", "splashing", None),
    (r"\b[Ss]mafliing\b", "smashing", None),
    (r"\b[Dd]afliing\b", "dashing", None),
    (r"\bfliining\b", "shining", None),
    (r"\bfliines\b", "shines", None),
    (r"\bfliip\b", "ship", None),
    (r"\bfliips\b", "ships", None),
    (r"\bfliipping\b", "shipping", None),
    (r"\bfliipped\b", "shipped", None),
    (r"\bfliillings\b", "shillings", None),
    (r"\bfliilling\b", "shilling", None),
    # -ssion/-ssibility patterns (ſſ -> fl)
    (r"\b[Pp]oflibility\b", "possibility", None),
    (r"\b[Ii]mpoflibility\b", "impossibility", None),
    (r"\b[Pp]oflible\b", "possible", None),
    (r"\b[Ii]mpoflible\b", "impossible", None),
    # ==========================================================================
    # Roman numeral corruption: lowercase L (l) misread as I, Y misread as V
    # Very common in pre-WWI texts where Roman numerals appear frequently
    # ==========================================================================
    # Y -> V corruption (Y and V look similar in many fonts)
    (r"\bYIII\b", "VIII", None),
    (r"\bYlll\b", "VIII", None),
    (r"\bYlII\b", "VIII", None),
    (r"\bYlli\b", "VIII", None),
    (r"\bYill\b", "VIII", None),
    (r"\bYili\b", "VIII", None),
    # Lowercase l -> I corruption (common OCR error)
    # VIII variants
    (r"\bVlll\b", "VIII", None),
    (r"\bVlII\b", "VIII", None),
    (r"\bVIll\b", "VIII", None),
    (r"\bVlli\b", "VIII", None),
    (r"\bVilI\b", "VIII", None),
    (r"\bVili\b", "VIII", None),
    (r"\bVIli\b", "VIII", None),
    (r"\bVlii\b", "VIII", None),
    # XIII variants
    (r"\bXlll\b", "XIII", None),
    (r"\bXIll\b", "XIII", None),
    (r"\bXlli\b", "XIII", None),
    (r"\bXili\b", "XIII", None),
    (r"\bXilI\b", "XIII", None),
    (r"\bXIli\b", "XIII", None),
    (r"\bXlii\b", "XIII", None),
    (r"\bXill\b", "XIII", None),
    # XVIII variants
    (r"\bXVlll\b", "XVIII", None),
    (r"\bXVIll\b", "XVIII", None),
    (r"\bXVlli\b", "XVIII", None),
    (r"\bXVili\b", "XVIII", None),
    (r"\bXVilI\b", "XVIII", None),
    (r"\bXVIli\b", "XVIII", None),
    (r"\bXVlii\b", "XVIII", None),
    (r"\bXVill\b", "XVIII", None),
    # XVII variants
    (r"\bXVll\b", "XVII", None),
    (r"\bXVlI\b", "XVII", None),
    (r"\bXVli\b", "XVII", None),
    (r"\bXVIl\b", "XVII", None),
    # XXIII variants
    (r"\bXXlll\b", "XXIII", None),
    (r"\bXXIll\b", "XXIII", None),
    (r"\bXXlli\b", "XXIII", None),
    (r"\bXXili\b", "XXIII", None),
    (r"\bXXill\b", "XXIII", None),
    # XXVIII variants
    (r"\bXXVlll\b", "XXVIII", None),
    (r"\bXXVIll\b", "XXVIII", None),
    (r"\bXXVill\b", "XXVIII", None),
    (r"\bXXVili\b", "XXVIII", None),
    # XXXII variants
    (r"\bXXXll\b", "XXXII", None),
    (r"\bXXXlI\b", "XXXII", None),
    (r"\bXXXIl\b", "XXXII", None),
    (r"\bXXXli\b", "XXXII", None),
    (r"\bXXXiI\b", "XXXII", None),
    (r"\bXXXIL\b", "XXXII", None),
    # XXXIII variants
    (r"\bXXXlll\b", "XXXIII", None),
    (r"\bXXXIll\b", "XXXIII", None),
    (r"\bXXXill\b", "XXXIII", None),
    (r"\bXXXIIL\b", "XXXIII", None),
    (r"\bXXXIlL\b", "XXXIII", None),
    # XXXVII variants
    (r"\bXXXVll\b", "XXXVII", None),
    (r"\bXXXVlI\b", "XXXVII", None),
    (r"\bXXXVIl\b", "XXXVII", None),
    (r"\bXXXVIL\b", "XXXVII", None),
    # XXXVIII variants
    (r"\bXXXVlll\b", "XXXVIII", None),
    (r"\bXXXVIll\b", "XXXVIII", None),
    (r"\bXXXVill\b", "XXXVIII", None),
    (r"\bXXXVIIL\b", "XXXVIII", None),
    # III variants (careful - many false positives possible)
    (r"\blll\b", "III", None),
    (r"\blII\b", "III", None),
    (
        r"\bIll\b",
        "III",
        r"(?:Chapter|Chap\.?|Part|Book|Vol\.?|Section|Sect\.?|Article|Art\.?|Act|Title|No\.?|Number|Plate|Fig\.?|Table|Psalm|Genesis|Exodus|Kings|Chronicles|Samuel)\s+Ill\b",
    ),
    (r"\bIlI\b", "III", None),
    # ==========================================================================
    # 'th' -> 'tli' corruption (h misread as li)
    # Common in older typefaces where 'h' resembles 'li'
    # ==========================================================================
    # High frequency words
    (r"\b[Ww]itliin\b", "within", None),
    (r"\b[Ww]itliiu\b", "within", None),
    (r"\b[Ww]itliiii\b", "within", None),
    (r"\b[Ww]ltliin\b", "within", None),
    (r"\b[Ww]itliia\b", "within", None),
    (r"\b[Ww]itlii\b", "within", None),
    (r"\b[Cc]lotliing\b", "clothing", None),
    (r"\b[Bb]reatliing\b", "breathing", None),
    (r"\b[Bb]atliing\b", "bathing", None),
    (r"\b[Ss]ootliing\b", "soothing", None),
    (r"\b[Ll]oatliing\b", "loathing", None),
    (r"\b[Ss]catliing\b", "scathing", None),
    (r"\b[Ss]eetliing\b", "seething", None),
    (r"\b[Tt]eetliing\b", "teething", None),
    (r"\b[Ww]reatliing\b", "wreathing", None),
    (r"\b[Ss]heatliing\b", "sheathing", None),
    # 'nothing' variants
    (r"\b[Nn]otliing\b", "nothing", None),
    (r"\b[Nn]otliiug\b", "nothing", None),
    (r"\b[Nn]otliiiig\b", "nothing", None),
    (r"\b[Nn]otliin\b", "nothing", None),
    (r"\b[Uu]otliing\b", "nothing", None),
    (r"\biiotliing\b", "nothing", None),
    # 'something' variants
    (r"\b[Ss]ometliing\b", "something", None),
    (r"\b[Ss]omctliing\b", "something", None),
    (r"\b[Ss]onietliing\b", "something", None),
    # 'anything/everything' variants
    (r"\b[Aa]nytliing\b", "anything", None),
    (r"\b[Ee]verytliing\b", "everything", None),
    # Other common words
    (r"\b[Gg]otliic\b", "Gothic", None),
    (r"\b[Ee]tliics\b", "ethics", None),
    (r"\b[Ee]tliical\b", "ethical", None),
    (r"\b[Mm]etliinks\b", "methinks", None),
    (r"\b[Mm]etliod\b", "method", None),
    (r"\b[Mm]etliods\b", "methods", None),
    (r"\b[Ss]ympatliies\b", "sympathies", None),
    (r"\b[Ss]ympatliize\b", "sympathize", None),
    (r"\b[Ss]ympatlietic\b", "sympathetic", None),
    (r"\b[Pp]atlios\b", "pathos", None),
    (r"\b[Pp]atlietic\b", "pathetic", None),
    (r"\b[Pp]atliology\b", "pathology", None),
    (r"\b[Pp]atliological\b", "pathological", None),
    (r"\b[Pp]htliisis\b", "phthisis", None),
    (r"\b[Ll]etlial\b", "lethal", None),
    (r"\b[Ff]atliom\b", "fathom", None),
    (r"\b[Ff]atlioms\b", "fathoms", None),
    (r"\b[Ff]atliomed\b", "fathomed", None),
    (r"\b[Ff]atliomless\b", "fathomless", None),
    (r"\b[Ff]urtlier\b", "further", None),
    (r"\b[Ff]nrtlier\b", "further", None),
    (r"\b[Ff]artlier\b", "farther", None),
    (r"\b[Gg]atlier\b", "gather", None),
    (r"\b[Gg]atliered\b", "gathered", None),
    (r"\b[Gg]atliering\b", "gathering", None),
    (r"\b[Rr]atlier\b", "rather", None),
    (r"\b[Ww]lietlier\b", "whether", None),
    (r"\b[Ww]hctlier\b", "whether", None),
    (r"\b[Tt]ogetlier\b", "together", None),
    (r"\b[Aa]ltliongh\b", "although", None),
    (r"\b[Aa]ltliough\b", "although", None),
    (r"\b[Tt]lird\b", "third", None),
    (r"\b[Tt]liird\b", "third", None),
    (r"\b[Tt]liis\b", "this", None),
    (r"\b[Tt]liat\b", "that", None),
    (r"\b[Tt]lien\b", "then", None),
    (r"\b[Tt]liey\b", "they", None),
    (r"\b[Tt]lieir\b", "their", None),
    (r"\b[Tt]liem\b", "them", None),
    (r"\b[Tt]liese\b", "these", None),
    (r"\b[Tt]liose\b", "those", None),
    (r"\b[Tt]liere\b", "there", None),
    (r"\b[Tt]lierefore\b", "therefore", None),
    (r"\b[Tt]lirough\b", "through", None),
    (r"\b[Tt]liroughout\b", "throughout", None),
    (r"\b[Tt]liousand\b", "thousand", None),
    (r"\b[Tt]liousands\b", "thousands", None),
    (r"\b[Tt]liought\b", "thought", None),
    (r"\b[Tt]lioughts\b", "thoughts", None),
    (r"\b[Tt]liink\b", "think", None),
    (r"\b[Tt]liinking\b", "thinking", None),
    (r"\b[Tt]liings\b", "things", None),
    (r"\b[Tt]liing\b", "thing", None),
    (r"\b[Oo]tlier\b", "other", None),
    (r"\b[Oo]tliers\b", "others", None),
    (r"\b[Oo]tlierwise\b", "otherwise", None),
    (r"\b[Aa]utlior\b", "author", None),
    (r"\b[Aa]utliors\b", "authors", None),
    (r"\b[Aa]utliority\b", "authority", None),
    (r"\b[Aa]utliorities\b", "authorities", None),
    (r"\b[Aa]utliorize\b", "authorize", None),
    (r"\b[Aa]utliorized\b", "authorized", None),
    # Place names
    (r"\b[Ll]otliian\b", "Lothian", None),
    # ==========================================================================
    # 'wh' -> 'wli' corruption (same cause as th -> tli)
    # ==========================================================================
    # 'which' variants (very high frequency)
    (r"\b[Ww]liieh\b", "which", None),
    (r"\b[Ww]liicb\b", "which", None),
    (r"\b[Ww]liieli\b", "which", None),
    (r"\b[Ww]liidi\b", "which", None),
    (r"\b[Ww]liioh\b", "which", None),
    (r"\b[Ww]liirh\b", "which", None),
    (r"\b[Ww]liicii\b", "which", None),
    (r"\b[Ww]liirli\b", "which", None),
    (r"\b[Ww]liioli\b", "which", None),
    (r"\b[Ww]liic\b", "which", None),
    (r"\b[Ww]liiih\b", "which", None),
    (r"\b[Ww]liili\b", "which", None),
    (r"\b[Ww]liieb\b", "which", None),
    (r"\b[Ww]liicl\b", "which", None),
    (r"\b[Ww]liit\b", "whit", None),
    (r"\b[Ww]liiit\b", "whit", None),
    # 'white' variants
    (r"\b[Ww]liite\b", "White", None),
    (r"\b[Ww]liitc\b", "White", None),
    (r"\b[Ww]liito\b", "White", None),
    (r"\b[Ww]liites\b", "Whites", None),
    # 'while/whilst' variants
    (r"\b[Ww]liile\b", "while", None),
    (r"\b[Ww]liilc\b", "while", None),
    (r"\b[Ww]liilo\b", "while", None),
    (r"\b[Ww]liilst\b", "whilst", None),
    (r"\b[Ww]liii\b", "while", None),
    (r"\b[Ww]liil\b", "while", None),
    # 'Whig' (political party)
    (r"\b[Ww]liig\b", "Whig", None),
    (r"\b[Ww]liigs\b", "Whigs", None),
    # 'what/when/where/why' variants
    (r"\b[Ww]liat\b", "what", None),
    (r"\b[Ww]lien\b", "when", None),
    (r"\b[Ww]liere\b", "where", None),
    (r"\b[Ww]liy\b", "why", None),
    (r"\b[Ww]liole\b", "whole", None),
    (r"\b[Ww]liolly\b", "wholly", None),
    (r"\b[Ww]liose\b", "whose", None),
    # 'meanwhile' variants
    (r"\b[Mm]eanwliile\b", "meanwhile", None),
    (r"\b[Aa]wliile\b", "awhile", None),
    (r"\b[Ww]ortliwliile\b", "worthwhile", None),
    # ==========================================================================
    # 'ii' -> 'u'/'h' confusion (selective - high confidence only)
    # Many 'ii' patterns are legitimate (German umlauts, Latin words)
    # Only fix clearly English words with unambiguous corrections
    # ==========================================================================
    # 'full' variants (ii for u)
    (r"\b[Ff]iill\b", "full", None),
    (r"\b[Ff]iilly\b", "fully", None),
    (r"\b[Pp]owerfiil\b", "powerful", None),
    (r"\b[Bb]eautifiil\b", "beautiful", None),
    (r"\b[Ww]onderfiil\b", "wonderful", None),
    (r"\b[Ss]uccessfiil\b", "successful", None),
    (r"\b[Cc]areftil\b", "careful", None),
    (r"\b[Cc]areftilly\b", "carefully", None),
    # 'will' variants (ii for ll)
    (r"\b[Ww]iil\b", "will", None),
    (r"\b[Ww]iill\b", "will", None),
    # 'him/his/himself' variants (ii for h)
    (r"\biiis\b", "his", None),
    (r"\bIiis\b", "His", None),
    (r"\biiim\b", "him", None),
    (r"\bIiim\b", "Him", None),
    (r"\b[Hh]iis\b", "his", None),
    (r"\b[Ll]iimself\b", "himself", None),
    (r"\b[Ll]iini\b", "him", None),
    (r"\b[Ll]iia\b", "his", None),
    # 'different' variants
    (r"\b[Dd]iiferent\b", "different", None),
    (r"\b[Dd]iifferent\b", "different", None),
    # ==========================================================================
    # Google digitization watermark artifacts (anachronistic, safe to remove)
    # These appear in Google Books scans and are never legitimate pre-WWI content
    # ==========================================================================
    (r"\bVjOOQIC\b", "", None),
    (r"\bVjOOQLC\b", "", None),
    (r"\bLjOOQIC\b", "", None),
    (r"\bLiOOQLC\b", "", None),
    (r"\bCjOOQIC\b", "", None),
    (r"\bCjOOQlC\b", "", None),
    (r"\bbyVjOOQlC\b", "", None),
    (r"\bbyVrrOOQlC\b", "", None),
    (r"\bbyCjOOQlC\b", "", None),
    (r"\bhyGoogIc\b", "", None),
    (r"\bGoOglc\b", "", None),
    (r"\bGoogXt\b", "", None),
    (r"\bDigiLizedbyGoOglc\b", "", None),
    (r"Digitized\s+by\s+[VLC]j?OOQ(?:IC|LC|lC)", "", None),
    # ==========================================================================
    # Repeated letter OCR artifacts (never legitimate words)
    # Examples: EEE, OOO, NNN, WWW, AAA, BBB, DDD, FFF (3+ same letter)
    # Excludes I, X, C, M, L, V which are Roman numeral components
    # ==========================================================================
    (r"\b([ABENODFGHJKPQRSTUWYZ])\1{2,}\b", "", None),
    # ==========================================================================
    # Clear 2-3 letter OCR noise (high frequency, never words)
    # Only the most obvious patterns with 10k+ occurrences
    # ==========================================================================
    (r"\b[I1]A\b", "", None),  # 94,675 occurrences in newspaper corpus
    (r"\b[I1]H\b", "", None),  # 65,786 occurrences in newspaper corpus
    # ==========================================================================
    # Word-joining fixes (spacing collapsed during OCR)
    # Common patterns where space was lost between words
    # ==========================================================================
    (r"\b[Oo]fthe\b", "of the", None),
    (r"\b[Tt]othe\b", "to the", None),
    (r"\b[Ii]nthe\b", "in the", None),
    (r"\b[Aa]ndthe\b", "and the", None),
    (r"\b[Ff]orthe\b", "for the", None),
    (r"\b[Oo]nthe\b", "on the", None),
    (r"\b[Aa]tthe\b", "at the", None),
    (r"\b[Bb]ythe\b", "by the", None),
    (r"\b[Ii]tis\b", "it is", None),
    (r"\b[Ii]twas\b", "it was", None),
    (r"\b[Tt]obe\b", "to be", None),
    (r"\b[Oo]fit\b", "of it", None),
    (r"\b[Ii]fthe\b", "if the", None),
    (r"\b[Aa]sthe\b", "as the", None),
    (r"\b[Oo]rthe\b", "or the", None),
    (r"\b[Oo]fhis\b", "of his", None),
    (r"\b[Oo]fher\b", "of her", None),
    (r"\b[Tt]othis\b", "to this", None),
    (r"\b[Ii]nthis\b", "in this", None),
    (r"\b[Oo]fthis\b", "of this", None),
    # "The" + word patterns (very common in pre-WWI texts)
    (r"\b[Tt]heCity\b", "the City", None),
    (r"\b[Tt]heKing\b", "the King", None),
    (r"\b[Tt]heQueen\b", "the Queen", None),
    (r"\b[Tt]heLord\b", "the Lord", None),
    (r"\b[Tt]heWorld\b", "the World", None),
    (r"\b[Tt]heSea\b", "the Sea", None),
    (r"\b[Tt]heCourt\b", "the Court", None),
    (r"\b[Tt]heTown\b", "the Town", None),
    (r"\b[Tt]heTwo\b", "the Two", None),
    (r"\b[Tt]heUnited\b", "the United", None),
    (r"\b[Tt]heFrench\b", "the French", None),
    (r"\b[Tt]heEnglish\b", "the English", None),
    (r"\b[Tt]heCommon\b", "the Common", None),
    (r"\b[Tt]heRev\b", "the Rev", None),
    (r"\b[Tt]heSouth\b", "the South", None),
    (r"\b[Tt]heNorth\b", "the North", None),
    (r"\b[Tt]heEast\b", "the East", None),
    (r"\b[Tt]heWest\b", "the West", None),
    (r"\b[Tt]heAge\b", "the Age", None),
    (r"\b[Tt]heNight\b", "the Night", None),
    (r"\b[Tt]heStory\b", "the Story", None),
    (r"\b[Tt]heDuke\b", "the Duke", None),
    (r"\b[Tt]heEarl\b", "the Earl", None),
    (r"\b[Tt]heAmerican\b", "the American", None),
    (r"\b[Tt]heGovernment\b", "the Government", None),
    (r"\b[Tt]heOrder\b", "the Order", None),
    (r"\b[Tt]heChurch\b", "the Church", None),
    (r"\b[Tt]heState\b", "the State", None),
    (r"\b[Tt]hePeople\b", "the People", None),
    (r"\b[Tt]heHouse\b", "the House", None),
    (r"\b[Tt]heSame\b", "the same", None),
    (r"\b[Tt]heOther\b", "the other", None),
    (r"\b[Tt]heFirst\b", "the first", None),
    (r"\b[Tt]heLast\b", "the last", None),
    (r"\b[Tt]heGreat\b", "the Great", None),
    (r"\b[Tt]heNew\b", "the New", None),
    (r"\b[Tt]heOld\b", "the Old", None),
    (r"\b[Tt]heWhole\b", "the whole", None),
    (r"\b[Tt]heMost\b", "the most", None),
    (r"\b[Tt]heBest\b", "the best", None),
    (r"\b[Tt]heOnly\b", "the only", None),
    (r"\b[Tt]heMan\b", "the Man", None),
    (r"\b[Tt]heMen\b", "the Men", None),
    (r"\b[Tt]heWoman\b", "the Woman", None),
    (r"\b[Tt]heWomen\b", "the Women", None),
    (r"\b[Tt]heChild\b", "the Child", None),
    (r"\b[Tt]heChildren\b", "the Children", None),
    (r"\b[Tt]heFather\b", "the Father", None),
    (r"\b[Tt]heMother\b", "the Mother", None),
    (r"\b[Tt]heSon\b", "the Son", None),
    (r"\b[Tt]heDaughter\b", "the Daughter", None),
    (r"\b[Tt]heBrother\b", "the Brother", None),
    (r"\b[Tt]heSister\b", "the Sister", None),
]

# Patterns that indicate garbage OCR (not fixable, flag for review)
GARBAGE_PATTERNS = [
    r"[^\x00-\x7F]{10,}",  # Long runs of non-ASCII
    r"[bcdfghjklmnpqrstvwxz]{6,}",  # Long consonant runs
    r"\d{2,}[a-z]+\d{2,}",  # Numbers mixed into words oddly
    r"[|l1I]{5,}",  # Pipe/l/1/I confusion runs
]


# Threshold for flagging high-substitution documents (substitutions per 1000 chars)
HIGH_SUBSTITUTION_THRESHOLD = 10.0


@dataclass
class CleanupStats:
    """Track cleanup statistics."""

    total_files: int = 0
    files_modified: int = 0
    files_flagged: int = 0
    files_skipped_language: int = 0  # Non-English documents skipped
    total_substitutions: int = 0
    whitespace_fixes: int = 0
    hyphen_rejoins: int = 0
    midword_caps_fixes: int = 0
    long_s_fixes: int = 0  # Track long-s fixes separately
    substitution_counts: Counter = field(default_factory=Counter)
    flagged_files: list = field(default_factory=list)
    skipped_files: list = field(default_factory=list)  # Non-English files
    # Per-document tracking (only interesting docs, not all 1M+)
    high_substitution_docs: list = field(default_factory=list)  # Docs above threshold
    long_s_documents: list = field(default_factory=list)  # Docs with long-s patterns
    long_s_document_count: int = 0  # Total count (list is capped)
    high_sub_document_count: int = 0  # Total count (list is capped)
    # Triage stats
    triage_passed: int = 0
    triage_quarantined: int = 0
    triage_rejected: int = 0
    triage_results: list = field(default_factory=list)  # Full triage results for JSONL export
    elapsed_seconds: float = 0.0  # Total processing time

    def track_document(
        self,
        filename: str,
        char_count: int,
        total_subs: int,
        long_s_fixes: int,
        whitespace_fixes: int,
        hyphen_fixes: int,
        midword_caps_fixes: int,
        has_long_s: bool,
    ):
        """Track per-document stats - only stores interesting documents to avoid memory bloat."""
        # Calculate substitution rate
        sub_rate = (total_subs / char_count) * 1000 if char_count > 0 else 0

        # Only store high-substitution docs (cap at 1000 to avoid memory issues)
        if sub_rate >= HIGH_SUBSTITUTION_THRESHOLD:
            self.high_sub_document_count += 1
            if len(self.high_substitution_docs) < 1000:
                ocr_pattern_fixes = (
                    total_subs - long_s_fixes - whitespace_fixes - hyphen_fixes - midword_caps_fixes
                )
                self.high_substitution_docs.append(
                    {
                        "filename": filename,
                        "char_count": char_count,
                        "total_substitutions": total_subs,
                        "substitution_rate": round(sub_rate, 2),
                        "categories": {
                            "long_s": long_s_fixes,
                            "whitespace": whitespace_fixes,
                            "hyphens": hyphen_fixes,
                            "midword_caps": midword_caps_fixes,
                            "ocr_patterns": ocr_pattern_fixes,
                        },
                    }
                )

        # Only store long-s docs (cap at 1000)
        if has_long_s:
            self.long_s_document_count += 1
            if len(self.long_s_documents) < 1000:
                self.long_s_documents.append(
                    {
                        "filename": filename,
                        "long_s_fixes": long_s_fixes,
                    }
                )

    def to_dict(self):
        return {
            "total_files": self.total_files,
            "files_modified": self.files_modified,
            "files_flagged": self.files_flagged,
            "files_skipped_language": self.files_skipped_language,
            "total_substitutions": self.total_substitutions,
            "substitution_breakdown": {
                "whitespace": self.whitespace_fixes,
                "hyphens": self.hyphen_rejoins,
                "midword_caps": self.midword_caps_fixes,
                "long_s": self.long_s_fixes,
                "ocr_patterns": self.total_substitutions
                - self.whitespace_fixes
                - self.hyphen_rejoins
                - self.midword_caps_fixes
                - self.long_s_fixes,
            },
            "top_substitutions": self.substitution_counts.most_common(50),
            "flagged_files": self.flagged_files[:100],
            "skipped_files": self.skipped_files[:100],
            # Per-document analysis (only interesting docs stored, not all 1M+)
            "long_s_documents": {
                "total_count": self.long_s_document_count,
                "sample_files": self.long_s_documents[:100],  # First 100 of up to 1000 stored
            },
            "high_substitution_documents": {
                "total_count": self.high_sub_document_count,
                "threshold_per_1000_chars": HIGH_SUBSTITUTION_THRESHOLD,
                "sample_files": sorted(
                    self.high_substitution_docs, key=lambda x: x["substitution_rate"], reverse=True
                )[:100],
            },
            # Triage stats
            "triage_passed": self.triage_passed,
            "triage_quarantined": self.triage_quarantined,
            "triage_rejected": self.triage_rejected,
            "triage_skipped_files": [r for r in self.triage_results if r["action"] != "pass"][
                :500
            ],  # Cap at 500 to avoid huge reports
        }


def check_garbage(text: str) -> list[tuple[str, int]]:
    """Check for unfixable garbage patterns. Returns list of (pattern, count)."""
    issues = []
    for pattern in GARBAGE_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if len(matches) > 5:
            issues.append((pattern, len(matches)))
    return issues


def clean_text(text: str, stats: Optional[CleanupStats] = None) -> tuple[str, int]:
    """
    Apply full OCR cleanup pipeline to text.

    Pipeline order:
    1. Unicode normalization (ftfy - fix mojibake, encoding errors)
    2. Whitespace normalization (strip trailing, collapse multiples)
    3. Hyphen rejoining (fix line-break hyphenation)
    4. Mid-word uppercase normalization (sVo -> svo)
    5. Long-s detection and fixing (for pre-1800 texts)
    6. OCR substitutions (pattern-based fixes)

    Returns: (cleaned_text, total_modification_count)
    """
    total_subs = 0

    # Step 1: Unicode normalization (fix mojibake, encoding errors)
    # Must run before pattern matching to ensure consistent character representation
    text = fix_unicode(text)

    # Step 2: Whitespace normalization (MUST be before hyphen detection)
    text, ws_count = normalize_whitespace(text)
    total_subs += ws_count
    if stats:
        stats.whitespace_fixes += ws_count

    # Step 3: Hyphen rejoining (after whitespace normalization)
    text, hyphen_count = rejoin_hyphenated(text)
    total_subs += hyphen_count
    if stats:
        stats.hyphen_rejoins += hyphen_count

    # Step 4: Mid-word uppercase normalization
    text, caps_count = normalize_midword_caps(text)
    total_subs += caps_count
    if stats:
        stats.midword_caps_fixes += caps_count

    # Step 5: Long-s patterns are now handled in Rust via clean_text()
    # The Rust module applies long-s fixes automatically during OCR cleanup

    # Step 6: OCR substitutions
    for pattern, replacement, context in OCR_SUBSTITUTIONS:
        if context:

            def contextual_replace(match):
                nonlocal total_subs
                result = re.sub(pattern, replacement, match.group(0), flags=re.IGNORECASE)
                if result != match.group(0):
                    total_subs += 1
                    if stats:
                        stats.substitution_counts[f"{pattern} -> {replacement}"] += 1
                return result

            text = re.sub(context, contextual_replace, text, flags=re.IGNORECASE)
        else:
            count_before = len(re.findall(pattern, text, re.IGNORECASE))
            if count_before > 0:
                text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
                total_subs += count_before
                if stats:
                    stats.substitution_counts[f"{pattern} -> {replacement}"] += count_before

    return text, total_subs


def clean_file(
    input_path: Path,
    output_path: Optional[Path] = None,
    stats: Optional[CleanupStats] = None,
    skip_language_check: bool = False,
) -> tuple[bool, int, list, bool]:
    """
    Clean a single file.

    Args:
        input_path: Path to input file
        output_path: Path to output file (None = don't write)
        stats: CleanupStats object to update
        skip_language_check: If True, skip language detection

    Returns: (was_modified, substitution_count, garbage_issues, was_skipped)
        was_skipped is True if file was skipped due to non-English content
    """
    try:
        with open(input_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except Exception as e:
        print(f"  Error reading {input_path}: {e}")
        return False, 0, [], False

    # Language detection - skip non-English documents
    if not skip_language_check:
        is_english, confidence = detect_language(content)
        if not is_english:
            if stats:
                stats.files_skipped_language += 1
                stats.skipped_files.append(
                    {
                        "file": str(input_path),
                        "reason": "non-english",
                        "confidence": confidence,
                    }
                )
            # Don't process, don't copy to output
            return False, 0, [], True

    # Snapshot stats before cleaning to calculate per-document breakdown
    ws_before = stats.whitespace_fixes if stats else 0
    hyphen_before = stats.hyphen_rejoins if stats else 0
    caps_before = stats.midword_caps_fixes if stats else 0
    long_s_before = stats.long_s_fixes if stats else 0

    garbage_issues = check_garbage(content)

    # Long-s patterns are now detected and fixed in Rust via clean_text()
    cleaned, sub_count = clean_text(content, stats)
    was_modified = sub_count > 0

    # Update total substitutions
    if stats:
        stats.total_substitutions += sub_count

    # Track per-document stats (only stores interesting docs - high sub rate)
    if stats and sub_count > 0:
        stats.track_document(
            filename=input_path.name,
            char_count=len(content),
            total_subs=sub_count,
            long_s_fixes=stats.long_s_fixes - long_s_before,
            whitespace_fixes=stats.whitespace_fixes - ws_before,
            hyphen_fixes=stats.hyphen_rejoins - hyphen_before,
            midword_caps_fixes=stats.midword_caps_fixes - caps_before,
            has_long_s=False,  # Long-s detection now handled in Rust
        )

    if output_path and was_modified:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(cleaned)
    elif output_path and not was_modified:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)

    return was_modified, sub_count, garbage_issues, False


def clean_batch(
    input_dir: Path,
    output_dir: Optional[Path] = None,
    file_pattern: str = "*.txt",
    use_rust: bool = True,
    skip_triage: bool = False,
    triage_output: Optional[Path] = None,
) -> CleanupStats:
    """
    Clean all text files in a directory.

    Uses Rust for file I/O when available (much faster).
    Single-threaded for clean Ctrl+C handling.

    Args:
        input_dir: Directory containing input files
        output_dir: Directory for output files (None = in-place)
        file_pattern: Glob pattern for files to process
        use_rust: Use Rust engine for speed
        skip_triage: If True, skip document triage and process all files
        triage_output: If set, write triage results to this JSONL file
    """
    import signal
    import sys
    import time

    stats = CleanupStats()
    interrupted = False

    def handle_interrupt(signum, frame):
        nonlocal interrupted
        if interrupted:
            # Second Ctrl+C - force exit
            print("\n\nForce quit.", file=sys.stderr)
            sys.exit(1)
        interrupted = True
        print("\n\nInterrupted! Finishing current file, then stopping...", file=sys.stderr)

    # Set up clean interrupt handling
    old_handler = signal.signal(signal.SIGINT, handle_interrupt)

    try:
        # Import Rust module (required - no Python fallback)
        import rust_ocr_clean  # type: ignore[import-not-found]

        rust_clean_file = rust_ocr_clean.clean_file_to_file

        # Discover files using os.scandir (faster than glob)
        print(f"Scanning {input_dir} for {file_pattern} files...", end="", flush=True)

        import fnmatch
        import os

        def fast_find_files(directory: Path, pattern: str) -> list[Path]:
            """Fast recursive file discovery using os.scandir."""
            results = []
            dirs_to_scan = [directory]
            scanned = 0

            while dirs_to_scan:
                current_dir = dirs_to_scan.pop()
                try:
                    with os.scandir(current_dir) as it:
                        for entry in it:
                            if entry.is_dir(follow_symlinks=False):
                                dirs_to_scan.append(Path(entry.path))
                            elif entry.is_file() and fnmatch.fnmatch(entry.name, pattern):
                                results.append(Path(entry.path))
                except PermissionError:
                    continue

                scanned += 1
                if scanned % 500 == 0:
                    print(".", end="", flush=True)

            return results

        input_files = fast_find_files(input_dir, file_pattern)
        stats.total_files = len(input_files)
        print(f" found {stats.total_files:,} files")

        if stats.total_files == 0:
            print("No files found.")
            return stats

        # Document triage - filter out problematic content before OCR cleanup
        files_to_process = input_files
        if not skip_triage:
            print("Running document triage...")
            try:
                import rust_ocr_clean  # type: ignore[import-not-found]

                # Process in chunks for progress and cancel support
                chunk_size = 1000
                total_files = len(input_files)
                pass_files = []
                language_counts: dict[str, int] = {}
                non_english_count = 0
                triage_start = time.time()
                last_triage_update = triage_start

                for chunk_start in range(0, total_files, chunk_size):
                    if interrupted:
                        print("\n  Triage interrupted!")
                        break

                    chunk_end = min(chunk_start + chunk_size, total_files)
                    chunk_files = input_files[chunk_start:chunk_end]
                    paths = [str(f) for f in chunk_files]

                    # Batch triage with Rust
                    triage_results = rust_ocr_clean.triage_batch(paths)

                    for r in triage_results:
                        if interrupted:
                            break

                        triage_record = {
                            "path": r.path,
                            "action": r.action,
                            "problems": list(r.problems),
                            "signals": {
                                "alpha_ratio": round(r.alpha_ratio, 4),
                                "line_length_cv": round(r.line_length_cv, 4),
                                "mean_words_per_line": round(r.mean_words_per_line, 2),
                                "fragment_ratio": round(r.fragment_ratio, 4),
                            },
                        }

                        # Language detection for files that pass structural checks
                        if r.action == "pass":
                            try:
                                lang_result = rust_ocr_clean.detect_language_file(r.path, 0.5)
                                triage_record["language"] = {
                                    "detected": lang_result.detected_lang,
                                    "confidence": round(lang_result.confidence, 4),
                                    "is_english": lang_result.is_english,
                                }
                                if not lang_result.is_english:
                                    triage_record["action"] = "reject"
                                    triage_record["problems"] = list(triage_record["problems"]) + [
                                        "non_english"
                                    ]
                                    non_english_count += 1
                                    lang = lang_result.detected_lang
                                    language_counts[lang] = language_counts.get(lang, 0) + 1
                            except Exception:
                                pass  # Assume English if detection fails

                        stats.triage_results.append(triage_record)

                        if triage_record["action"] == "pass":
                            pass_files.append(Path(r.path))
                            stats.triage_passed += 1
                        elif triage_record["action"] == "quarantine":
                            stats.triage_quarantined += 1
                        else:  # reject
                            stats.triage_rejected += 1

                    # Progress update
                    now = time.time()
                    processed = chunk_end
                    if now - last_triage_update >= 1.0 or processed == total_files or interrupted:
                        elapsed = now - triage_start
                        files_per_sec = processed / elapsed if elapsed > 0 else 0
                        remaining = (
                            (total_files - processed) / files_per_sec if files_per_sec > 0 else 0
                        )

                        if remaining >= 3600:
                            eta = f"{remaining / 3600:.1f}h"
                        elif remaining >= 60:
                            eta = f"{remaining / 60:.1f}m"
                        else:
                            eta = f"{remaining:.0f}s"

                        pct = (processed / total_files) * 100
                        print(
                            f"  [{pct:5.1f}%] {processed:,}/{total_files:,} | "
                            f"{files_per_sec:.0f} files/s | ETA: {eta} | "
                            f"pass: {stats.triage_passed:,}, "
                            f"quarantine: {stats.triage_quarantined:,}, "
                            f"reject: {stats.triage_rejected:,}"
                        )
                        last_triage_update = now

                files_to_process = pass_files

                # Show language stats
                if language_counts:
                    print(f"\n  Non-English detected: {non_english_count:,} files")
                    sorted_langs = sorted(language_counts.items(), key=lambda x: -x[1])[:10]
                    for lang, count in sorted_langs:
                        print(f"    {lang}: {count:,}")
                    if len(language_counts) > 10:
                        print(f"    ... and {len(language_counts) - 10} more languages")
                else:
                    print("\n  Language check: all files English (or detection inconclusive)")

                # Write triage results to JSONL if requested
                if triage_output:
                    import json

                    with open(triage_output, "w") as f:
                        for record in stats.triage_results:
                            f.write(json.dumps(record) + "\n")
                    print(f"  Triage results written to: {triage_output}")

            except ImportError:
                print("  Skipped (Rust module not available)")

        # Skip upfront size calculation - we'll track as we go
        print("(Size will be calculated during processing)")

        print(f"\n{'=' * 60}")
        print("OCR Cleanup - Rust engine")
        print(f"{'=' * 60}")
        print(f"  Files to process: {len(files_to_process):,}")
        if not skip_triage:
            print(f"  (Skipped by triage: {stats.triage_quarantined + stats.triage_rejected:,})")
        print(f"  Output: {output_dir or 'in-place'}")
        print(f"{'=' * 60}\n")

        start_time = time.time()
        bytes_processed = 0
        last_update = start_time
        i = 0  # Track progress even if loop is empty or interrupted

        for i, input_path in enumerate(files_to_process, 1):
            if interrupted:
                break

            if output_dir:
                relative = input_path.relative_to(input_dir)
                output_path = output_dir / relative
            else:
                output_path = input_path  # in-place

            try:
                # Use Rust for all file I/O
                was_modified, sub_count, file_bytes, categories = rust_clean_file(
                    str(input_path), str(output_path)
                )
                bytes_processed += file_bytes
                # Aggregate category counts from Rust
                stats.long_s_fixes += categories.get("long_s", 0)

                if was_modified:
                    stats.files_modified += 1
                    stats.total_substitutions += sub_count

            except Exception as e:
                print(f"\n  Error processing {input_path}: {e}", file=sys.stderr)
                continue

            # Progress update every 2 seconds or every 500 files
            now = time.time()
            total_to_process = len(files_to_process)
            if now - last_update >= 2.0 or i % 500 == 0:
                elapsed = now - start_time
                files_per_sec = i / elapsed if elapsed > 0 else 0
                mb_per_sec = (bytes_processed / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                remaining = (total_to_process - i) / files_per_sec if files_per_sec > 0 else 0

                # Format remaining time
                if remaining >= 3600:
                    eta = f"{remaining / 3600:.1f}h"
                elif remaining >= 60:
                    eta = f"{remaining / 60:.1f}m"
                else:
                    eta = f"{remaining:.0f}s"

                pct = (i / total_to_process) * 100 if total_to_process > 0 else 100
                print(
                    f"  [{pct:5.1f}%] {i:,}/{total_to_process:,} files | "
                    f"{files_per_sec:.1f} files/s | {mb_per_sec:.1f} MB/s | "
                    f"ETA: {eta} | subs: {stats.total_substitutions:,}"
                )
                last_update = now

        # Final stats
        elapsed = time.time() - start_time
        stats.elapsed_seconds = elapsed  # Store for final report
        total_to_process = len(files_to_process)
        files_per_sec = total_to_process / elapsed if elapsed > 0 else 0
        mb_per_sec = (bytes_processed / (1024 * 1024)) / elapsed if elapsed > 0 else 0

        print(f"\n{'=' * 60}")
        if interrupted:
            print(f"INTERRUPTED after {i:,} of {total_to_process:,} files")
        else:
            print("COMPLETE")
        print(f"{'=' * 60}")
        print(f"  Files processed: {i:,}")
        print(f"  Files modified:  {stats.files_modified:,}")
        print(f"  Substitutions:   {stats.total_substitutions:,}")
        if not skip_triage:
            print(
                f"  Triage skipped:  {stats.triage_quarantined + stats.triage_rejected:,} "
                f"({stats.triage_quarantined:,} quarantine, {stats.triage_rejected:,} reject)"
            )
        print(f"  Time elapsed:    {elapsed:.1f}s")
        print(f"  Throughput:      {files_per_sec:.1f} files/s, {mb_per_sec:.1f} MB/s")
        if stats.files_flagged > 0:
            print(f"  Files flagged:   {stats.files_flagged:,} (garbage patterns)")
        print(f"{'=' * 60}")

    finally:
        # Restore original signal handler
        signal.signal(signal.SIGINT, old_handler)

    return stats


def analyze_corpus(corpus_dir: Path, sample_size: int = 1000) -> dict:
    """
    Analyze a corpus for OCR error patterns without modifying.

    Returns analysis report.
    """
    files = list(corpus_dir.glob("**/*.txt"))
    if len(files) > sample_size:
        import random

        files = random.sample(files, sample_size)

    error_counts = Counter()
    garbage_files = []
    total_words = 0

    print(f"Analyzing {len(files)} files...")

    for filepath in files:
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
        except Exception:
            continue

        words = content.split()
        total_words += len(words)

        for pattern, replacement, _ in OCR_SUBSTITUTIONS:
            matches = re.findall(pattern, content, re.IGNORECASE)
            if matches:
                error_counts[f"{pattern} -> {replacement}"] += len(matches)

        garbage = check_garbage(content)
        if garbage:
            garbage_files.append(str(filepath))

    return {
        "files_analyzed": len(files),
        "total_words": total_words,
        "potential_errors": error_counts.most_common(50),
        "garbage_files": garbage_files[:50],
        "estimated_error_rate": sum(error_counts.values()) / total_words if total_words > 0 else 0,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Clean OCR errors in historical texts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  clean    Clean a single file
  batch    Clean all files in a directory
  analyze  Analyze corpus for errors without modifying

Examples:
  tc-ocr-clean clean input.txt -o output.txt
  tc-ocr-clean batch ./corpus_raw -o ./corpus_clean
  tc-ocr-clean analyze ./corpus --report analysis.json
        """,
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Clean single file
    clean_parser = subparsers.add_parser("clean", help="Clean a single file")
    clean_parser.add_argument("input", type=Path, help="Input file")
    clean_parser.add_argument("-o", "--output", type=Path, help="Output file (default: stdout)")

    # Batch clean
    batch_parser = subparsers.add_parser("batch", help="Clean all files in directory")
    batch_parser.add_argument("input_dir", type=Path, help="Input directory")
    batch_parser.add_argument("-o", "--output-dir", type=Path, help="Output directory")
    batch_parser.add_argument("--pattern", default="*.txt", help="File pattern (default: *.txt)")
    batch_parser.add_argument(
        "--report",
        type=Path,
        help="Override stats report location (default: {output_parent}/_cleanup_report.json)",
    )
    batch_parser.add_argument(
        "--triage-output",
        type=Path,
        help="Override triage results location (default: {output_parent}/_triage_results.jsonl)",
    )
    batch_parser.add_argument(
        "--no-report",
        action="store_true",
        help="Disable automatic report generation",
    )
    batch_parser.add_argument(
        "--skip-triage",
        action="store_true",
        help="Skip document triage and process all files",
    )

    # Analyze
    analyze_parser = subparsers.add_parser("analyze", help="Analyze corpus for OCR errors")
    analyze_parser.add_argument("corpus_dir", type=Path, help="Corpus directory")
    analyze_parser.add_argument("--sample", type=int, default=1000, help="Sample size")
    analyze_parser.add_argument("--report", type=Path, help="Save report to JSON")

    args = parser.parse_args()

    if args.command == "clean":
        was_modified, sub_count, garbage, was_skipped = clean_file(args.input, args.output)

        if was_skipped:
            print(f"Skipped {args.input} - detected as non-English")
        elif args.output:
            print(f"Cleaned {args.input} -> {args.output}")
            print(f"  Substitutions: {sub_count}")
            if garbage:
                print(f"  Warning: {len(garbage)} garbage patterns detected")
        else:
            with open(args.input, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
            cleaned, _ = clean_text(content)
            print(cleaned)

    elif args.command == "batch":
        from datetime import datetime

        run_start = datetime.now()

        # Determine report paths (default to parent of output dir)
        output_parent = args.output_dir.parent if args.output_dir else args.input_dir
        report_path = args.report if args.report else output_parent / "_cleanup_report.json"
        triage_path = (
            args.triage_output if args.triage_output else output_parent / "_triage_results.jsonl"
        )

        # Don't overwrite existing files - add numeric suffix
        if report_path:
            report_path = get_unique_path(report_path)
        if triage_path:
            triage_path = get_unique_path(triage_path)

        # Disable reports if requested
        if args.no_report:
            report_path = None
            triage_path = None

        stats = clean_batch(
            args.input_dir,
            args.output_dir,
            args.pattern,
            skip_triage=args.skip_triage,
            triage_output=triage_path,
        )

        run_end = datetime.now()

        print(f"\n{'=' * 60}")
        print("Batch cleanup complete")
        print(f"{'=' * 60}")

        # Format duration in human terms
        duration = stats.elapsed_seconds if hasattr(stats, "elapsed_seconds") else 0
        if duration >= 3600:
            hours = int(duration // 3600)
            mins = int((duration % 3600) // 60)
            secs = int(duration % 60)
            duration_str = f"{hours}h {mins}m {secs}s"
        elif duration >= 60:
            mins = int(duration // 60)
            secs = int(duration % 60)
            duration_str = f"{mins}m {secs}s"
        else:
            duration_str = f"{duration:.1f}s"

        print(f"  Duration: {duration_str}")
        print(f"  Total files scanned: {stats.total_files:,}")

        # Triage summary
        if stats.triage_passed > 0 or stats.triage_quarantined > 0 or stats.triage_rejected > 0:
            print("\n  Triage results:")
            print(f"    Passed (processed):  {stats.triage_passed:,}")
            print(f"    Quarantined:         {stats.triage_quarantined:,}")
            print(f"    Rejected:            {stats.triage_rejected:,}")

        # OCR cleanup results
        print("\n  OCR cleanup:")
        print(f"    Files modified:      {stats.files_modified:,}")
        print(f"    Total substitutions: {stats.total_substitutions:,}")
        if stats.files_flagged > 0:
            print(f"    Flagged (post-OCR garbage): {stats.files_flagged:,}")

        if stats.substitution_counts:
            print("\n  Top substitutions:")
            for pattern, count in stats.substitution_counts.most_common(10):
                print(f"    {pattern}: {count:,}")

        # Write reports with metadata
        print("\n  Reports:")
        print(f"    Output directory: {args.output_dir}")

        if report_path:
            report_data = {
                "metadata": {
                    "input_dir": str(args.input_dir.resolve()),
                    "output_dir": str(args.output_dir.resolve()) if args.output_dir else None,
                    "run_started": run_start.isoformat(),
                    "run_completed": run_end.isoformat(),
                    "duration_seconds": duration,
                    "duration_human": duration_str,
                    "pattern": args.pattern,
                    "skip_triage": args.skip_triage,
                },
                "stats": stats.to_dict(),
            }
            with open(report_path, "w") as f:
                json.dump(report_data, f, indent=2)
            print(f"    Stats report: {report_path}")

        if triage_path and triage_path.exists():
            print(f"    Triage results: {triage_path}")
        elif args.no_report:
            print("    (Reports disabled with --no-report)")

        print(f"{'=' * 60}")

    elif args.command == "analyze":
        report = analyze_corpus(args.corpus_dir, args.sample)

        print(f"\n{'=' * 60}")
        print("Corpus Analysis")
        print(f"{'=' * 60}")
        print(f"  Files analyzed: {report['files_analyzed']}")
        print(f"  Total words: {report['total_words']:,}")
        print(f"  Estimated error rate: {report['estimated_error_rate']:.4%}")
        print(f"  Files with garbage patterns: {len(report['garbage_files'])}")

        if report["potential_errors"]:
            print("\nTop potential errors:")
            for pattern, count in report["potential_errors"][:15]:
                print(f"  {pattern}: {count}")

        if args.report:
            with open(args.report, "w") as f:
                json.dump(report, f, indent=2)
            print(f"\nReport saved to {args.report}")


if __name__ == "__main__":
    main()
