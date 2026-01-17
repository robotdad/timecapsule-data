"""Utility modules for corpus processing."""

from .schema import TextMetadata, CorpusMetadataWriter
from .validate import main as validate_corpus
from .dedup import main as dedup_corpus
from .analyze import main as analyze_catalog

__all__ = [
    "TextMetadata", 
    "CorpusMetadataWriter",
    "validate_corpus",
    "dedup_corpus", 
    "analyze_catalog",
]
from .ocr_cleanup import main as ocr_cleanup, clean_text, clean_batch

__all__.extend(["ocr_cleanup", "clean_text", "clean_batch"])
