"""Corpus collectors for various sources."""

from .gutenberg import main as collect_gutenberg
from .internet_archive import main as collect_ia
from .perseus import main as collect_perseus

__all__ = ["collect_gutenberg", "collect_ia", "collect_perseus"]
