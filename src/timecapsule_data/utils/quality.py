#!/usr/bin/env python3
"""
Quality Filters - Gopher-style heuristics for corpus cleaning.

Implements quality heuristics from the Gopher and RefinedWeb papers,
adapted for historical text corpora.
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class QualityResult:
    """Result of quality assessment."""
    score: float  # 0.0 to 1.0
    passed: bool
    reasons: list[str]
    metrics: dict


class QualityFilter:
    """
    Gopher-style quality heuristics adapted for historical texts.
    
    Default thresholds are tuned for 19th century prose which tends to have:
    - Longer sentences than modern text
    - More varied vocabulary
    - Occasional OCR artifacts
    """
    
    def __init__(
        self,
        min_words: int = 50,
        max_words: int = 500000,
        min_mean_word_length: float = 3.0,
        max_mean_word_length: float = 12.0,
        max_symbol_ratio: float = 0.15,
        max_repeated_line_ratio: float = 0.4,
        max_repeated_para_ratio: float = 0.3,
        min_alpha_ratio: float = 0.7,
        max_digit_ratio: float = 0.2,
    ):
        self.min_words = min_words
        self.max_words = max_words
        self.min_mean_word_length = min_mean_word_length
        self.max_mean_word_length = max_mean_word_length
        self.max_symbol_ratio = max_symbol_ratio
        self.max_repeated_line_ratio = max_repeated_line_ratio
        self.max_repeated_para_ratio = max_repeated_para_ratio
        self.min_alpha_ratio = min_alpha_ratio
        self.max_digit_ratio = max_digit_ratio
    
    def assess(self, text: str) -> QualityResult:
        """
        Assess text quality.
        
        Returns QualityResult with score, pass/fail, and detailed metrics.
        """
        reasons = []
        metrics = {}
        
        # Word count
        words = text.split()
        word_count = len(words)
        metrics['word_count'] = word_count
        
        if word_count < self.min_words:
            reasons.append(f"too_short ({word_count} < {self.min_words})")
        if word_count > self.max_words:
            reasons.append(f"too_long ({word_count} > {self.max_words})")
        
        if word_count == 0:
            return QualityResult(
                score=0.0,
                passed=False,
                reasons=["empty_document"],
                metrics=metrics,
            )
        
        # Mean word length
        mean_word_len = sum(len(w) for w in words) / word_count
        metrics['mean_word_length'] = round(mean_word_len, 2)
        
        if mean_word_len < self.min_mean_word_length:
            reasons.append(f"short_words ({mean_word_len:.1f} < {self.min_mean_word_length})")
        if mean_word_len > self.max_mean_word_length:
            reasons.append(f"long_words ({mean_word_len:.1f} > {self.max_mean_word_length})")
        
        # Character composition
        total_chars = len(text)
        if total_chars > 0:
            alpha_chars = sum(1 for c in text if c.isalpha())
            digit_chars = sum(1 for c in text if c.isdigit())
            symbol_chars = sum(1 for c in text if not c.isalnum() and not c.isspace())
            
            alpha_ratio = alpha_chars / total_chars
            digit_ratio = digit_chars / total_chars
            symbol_ratio = symbol_chars / total_chars
            
            metrics['alpha_ratio'] = round(alpha_ratio, 3)
            metrics['digit_ratio'] = round(digit_ratio, 3)
            metrics['symbol_ratio'] = round(symbol_ratio, 3)
            
            if alpha_ratio < self.min_alpha_ratio:
                reasons.append(f"low_alpha ({alpha_ratio:.1%} < {self.min_alpha_ratio:.0%})")
            if digit_ratio > self.max_digit_ratio:
                reasons.append(f"high_digits ({digit_ratio:.1%} > {self.max_digit_ratio:.0%})")
            if symbol_ratio > self.max_symbol_ratio:
                reasons.append(f"high_symbols ({symbol_ratio:.1%} > {self.max_symbol_ratio:.0%})")
        
        # Repeated lines (catches OCR headers, page numbers, etc.)
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        if lines:
            unique_lines = set(lines)
            repeated_ratio = 1 - (len(unique_lines) / len(lines))
            metrics['repeated_line_ratio'] = round(repeated_ratio, 3)
            
            if repeated_ratio > self.max_repeated_line_ratio:
                reasons.append(f"repeated_lines ({repeated_ratio:.1%} > {self.max_repeated_line_ratio:.0%})")
        
        # Repeated paragraphs (catches duplicated sections)
        paragraphs = [p.strip() for p in re.split(r'\n\s*\n', text) if p.strip()]
        if len(paragraphs) > 1:
            unique_paras = set(paragraphs)
            para_repeat_ratio = 1 - (len(unique_paras) / len(paragraphs))
            metrics['repeated_para_ratio'] = round(para_repeat_ratio, 3)
            
            if para_repeat_ratio > self.max_repeated_para_ratio:
                reasons.append(f"repeated_paragraphs ({para_repeat_ratio:.1%})")
        
        # Compute overall score
        score = 1.0
        
        # Penalize based on how far outside thresholds
        if mean_word_len < self.min_mean_word_length:
            score -= 0.2 * (self.min_mean_word_length - mean_word_len)
        if mean_word_len > self.max_mean_word_length:
            score -= 0.1 * (mean_word_len - self.max_mean_word_length)
        
        if 'symbol_ratio' in metrics:
            if metrics['symbol_ratio'] > self.max_symbol_ratio:
                score -= 2 * (metrics['symbol_ratio'] - self.max_symbol_ratio)
        
        if 'repeated_line_ratio' in metrics:
            score -= metrics['repeated_line_ratio'] * 0.5
        
        score = max(0.0, min(1.0, score))
        
        return QualityResult(
            score=round(score, 3),
            passed=len(reasons) == 0,
            reasons=reasons,
            metrics=metrics,
        )


def analyze_corpus(
    corpus_path: Path,
    filter_config: Optional[dict] = None,
) -> dict:
    """
    Analyze quality of all documents in a corpus.
    
    Returns statistics and identifies low-quality documents.
    """
    qf = QualityFilter(**(filter_config or {}))
    
    # Find all text files
    if corpus_path.is_file():
        files = [corpus_path]
    else:
        files = list(corpus_path.rglob('*.txt'))
    
    print(f"Analyzing {len(files)} files...")
    
    results = []
    passed = 0
    failed = 0
    scores = []
    
    for i, path in enumerate(files, 1):
        try:
            text = path.read_text(encoding='utf-8', errors='replace')
            result = qf.assess(text)
            
            results.append({
                'file': str(path),
                'score': result.score,
                'passed': result.passed,
                'reasons': result.reasons,
                'metrics': result.metrics,
            })
            
            scores.append(result.score)
            if result.passed:
                passed += 1
            else:
                failed += 1
            
            if i % 100 == 0:
                print(f"  Analyzed {i}/{len(files)}...")
                
        except Exception as e:
            print(f"  Error analyzing {path}: {e}")
            results.append({
                'file': str(path),
                'score': 0.0,
                'passed': False,
                'reasons': [f"error: {e}"],
                'metrics': {},
            })
            failed += 1
    
    # Compute statistics
    if scores:
        avg_score = sum(scores) / len(scores)
        min_score = min(scores)
        max_score = max(scores)
    else:
        avg_score = min_score = max_score = 0.0
    
    return {
        'total_files': len(files),
        'passed': passed,
        'failed': failed,
        'pass_rate': passed / len(files) if files else 0,
        'avg_score': round(avg_score, 3),
        'min_score': round(min_score, 3),
        'max_score': round(max_score, 3),
        'results': results,
    }


def filter_corpus(
    input_path: Path,
    output_path: Path,
    min_score: float = 0.5,
    filter_config: Optional[dict] = None,
) -> dict:
    """
    Filter corpus, keeping only documents above quality threshold.
    """
    qf = QualityFilter(**(filter_config or {}))
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Find all text files
    files = list(input_path.rglob('*.txt'))
    
    print(f"Filtering {len(files)} files (min_score={min_score})...")
    
    kept = 0
    filtered = 0
    
    for i, path in enumerate(files, 1):
        try:
            text = path.read_text(encoding='utf-8', errors='replace')
            result = qf.assess(text)
            
            if result.score >= min_score:
                # Keep this file
                dest = output_path / path.name
                dest.write_text(text, encoding='utf-8')
                kept += 1
            else:
                filtered += 1
            
            if i % 100 == 0:
                print(f"  Processed {i}/{len(files)}, kept {kept}...")
                
        except Exception as e:
            print(f"  Error processing {path}: {e}")
            filtered += 1
    
    print(f"\nKept: {kept}")
    print(f"Filtered: {filtered}")
    print(f"Output: {output_path}")
    
    return {
        'input_files': len(files),
        'kept': kept,
        'filtered': filtered,
        'min_score': min_score,
    }


def main():
    parser = argparse.ArgumentParser(
        description='Quality filtering for text corpora (Gopher-style heuristics)'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze corpus quality')
    analyze_parser.add_argument('corpus', type=Path, help='Corpus directory')
    analyze_parser.add_argument('--output', '-o', type=Path,
                                help='Save analysis to JSON file')
    analyze_parser.add_argument('--show-failed', action='store_true',
                                help='Show details of failed documents')
    
    # Filter command
    filter_parser = subparsers.add_parser('filter', help='Filter corpus by quality')
    filter_parser.add_argument('input', type=Path, help='Input corpus directory')
    filter_parser.add_argument('-o', '--output', type=Path, required=True,
                               help='Output directory for filtered corpus')
    filter_parser.add_argument('--min-score', type=float, default=0.5,
                               help='Minimum quality score (default: 0.5)')
    
    # Check command (single file)
    check_parser = subparsers.add_parser('check', help='Check single file quality')
    check_parser.add_argument('file', type=Path, help='File to check')
    
    args = parser.parse_args()
    
    if args.command == 'analyze':
        results = analyze_corpus(args.corpus)
        
        print("\n" + "=" * 60)
        print("QUALITY ANALYSIS")
        print("=" * 60)
        print(f"Total files: {results['total_files']}")
        print(f"Passed: {results['passed']} ({results['pass_rate']:.1%})")
        print(f"Failed: {results['failed']}")
        print(f"Score range: {results['min_score']:.2f} - {results['max_score']:.2f}")
        print(f"Average score: {results['avg_score']:.2f}")
        
        if args.show_failed:
            failed_results = [r for r in results['results'] if not r['passed']]
            if failed_results:
                print(f"\nFailed documents ({len(failed_results)}):")
                for r in failed_results[:20]:
                    print(f"  {Path(r['file']).name}")
                    print(f"    Score: {r['score']:.2f}, Reasons: {', '.join(r['reasons'])}")
                if len(failed_results) > 20:
                    print(f"  ... and {len(failed_results) - 20} more")
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"\nAnalysis saved to {args.output}")
    
    elif args.command == 'filter':
        filter_corpus(args.input, args.output, min_score=args.min_score)
    
    elif args.command == 'check':
        text = args.file.read_text(encoding='utf-8', errors='replace')
        qf = QualityFilter()
        result = qf.assess(text)
        
        print(f"File: {args.file}")
        print(f"Score: {result.score:.2f}")
        print(f"Passed: {result.passed}")
        if result.reasons:
            print(f"Issues: {', '.join(result.reasons)}")
        print(f"Metrics: {json.dumps(result.metrics, indent=2)}")
    
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
