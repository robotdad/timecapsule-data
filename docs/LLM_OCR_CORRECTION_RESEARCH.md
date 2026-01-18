# LLM-Based OCR Correction Research

Research notes for Tier 3 OCR cleanup using Large Language Models.

**Status**: Research phase - not yet implemented  
**Target**: Files that fail Tier 1/2 cleanup (score >= 0.20 after SymSpell)

---

## Key Research Paper

**"No Free Lunch: Challenges and Limitations of LLM-Based OCR Post-Correction"**  
- Source: TurkuNLP (University of Turku)
- arXiv: 2503.18294
- GitHub: https://github.com/Shef-AIRE/llm-postcorrection-ocr

### Key Findings

| Model | CER Reduction | Notes |
|-------|---------------|-------|
| GPT-4o | 58% | Best performance, but expensive ($$$) |
| GPT-4o-mini | 48% | Good cost/performance balance |
| Llama-3.1-70B | 39% | Best open model, needs ~43GB VRAM (4-bit) |
| Llama-3.1-8B | 20% | Decent, single GPU (~6GB 4-bit) |
| Llama-3.2-3B | **Negative** | Actually increases errors |

### Critical Insights

1. **Small models make it worse**: Models under 8B parameters tend to introduce more errors than they fix. The fine-tuned Llama-3.2-3B on HuggingFace showed CER going from 0.014 to 0.022.

2. **Quantization works**: 4-bit quantization only degrades performance by 2-5% vs fp16, making larger models feasible on consumer hardware.

3. **Segment length matters**: 200-300 words is optimal. Too short loses context, too long exceeds attention.

4. **Post-processing required**: Raw LLM output needs cleaning (remove explanations, markdown, etc.)

5. **Prompt engineering critical**: Simple "fix OCR errors" prompts underperform. Need specific instructions about preserving structure, not explaining changes, etc.

---

## Recommended Model for DGX Spark

**Primary: Llama-3.1-70B-Instruct (4-bit quantized)**
- ~39% CER reduction on historical English
- Needs ~43GB VRAM (fits 2x A100 or similar)
- Good understanding of historical text patterns

**Fallback: Llama-3.1-8B-Instruct**
- ~20% CER reduction
- Fits single GPU (~6GB 4-bit)
- Faster throughput, lower quality

---

## Implementation Approach

### vLLM for Batch Inference

```python
from vllm import LLM, SamplingParams

llm = LLM(
    model="meta-llama/Llama-3.1-70B-Instruct",
    quantization="awq",  # or "gptq"
    tensor_parallel_size=2,  # Use 2 GPUs
    max_model_len=4096,
)

sampling_params = SamplingParams(
    temperature=0.1,  # Low for deterministic output
    max_tokens=1024,
    stop=["---", "Note:", "Explanation:"],
)
```

### Optimal Prompt Template

Based on research findings:

```
You are correcting OCR errors in a historical document from the 19th century.

Rules:
1. Fix obvious OCR errors (character substitutions, missing letters)
2. Preserve original spelling variants (British spellings, archaic terms)
3. Keep proper nouns as-is unless clearly corrupted
4. Maintain original punctuation and formatting
5. Do NOT add explanations or commentary
6. Output ONLY the corrected text

Text to correct:
{text}

Corrected text:
```

### Batch Processing Pipeline

```
1. Score all files with tc-ocr-score
2. Filter files with score >= 0.20 (worst ~5-10%)
3. Split files into 250-word segments
4. Batch process with vLLM (maximize GPU utilization)
5. Reassemble segments
6. Re-score to verify improvement
7. Flag files that didn't improve for manual review
```

---

## Cost/Time Estimates

For ~7,000 files (5% of 147k corpus) at ~10KB average:

| Model | Time (DGX Spark) | Quality |
|-------|------------------|---------|
| Llama-3.1-70B | ~24-48 hours | Best |
| Llama-3.1-8B | ~8-12 hours | Moderate |

---

## HuggingFace Models to Evaluate

1. **meta-llama/Llama-3.1-70B-Instruct** - Primary candidate
2. **meta-llama/Llama-3.1-8B-Instruct** - Fallback
3. **mistralai/Mixtral-8x7B-Instruct-v0.1** - MoE alternative
4. **m-biriuchinskii/Llama-3.2-3B-ocr-correction-3** - OCR-specific fine-tune (test but expect poor results)

---

## Known Limitations

1. **Proper nouns**: LLMs may "correct" unfamiliar names to common words
2. **Historical terms**: May modernize archaic vocabulary
3. **Consistency**: Same error may be corrected differently in different passages
4. **Hallucination**: May invent plausible but incorrect text
5. **Cost**: Even with local inference, GPU time is expensive

---

## Validation Strategy

After LLM correction:

1. Re-run tc-ocr-score - should show improvement
2. Sample random passages for human review
3. Compare word-level diff to identify suspicious changes
4. Flag files where score increased (got worse)

---

## Next Steps

1. [ ] Collect sample of worst files from Tier 1/2 failures
2. [ ] Test prompts on sample with Claude/GPT-4 first (quick iteration)
3. [ ] Set up vLLM on DGX Spark
4. [ ] Benchmark Llama-3.1-70B vs 8B on sample
5. [ ] Develop post-processing to clean LLM output
6. [ ] Build batch pipeline with progress tracking
7. [ ] Run on full "garbage tier" subset
8. [ ] Evaluate results and iterate

---

## References

- TurkuNLP paper: https://arxiv.org/abs/2503.18294
- Shef-AIRE implementation: https://github.com/Shef-AIRE/llm-postcorrection-ocr
- vLLM docs: https://docs.vllm.ai/
- Llama 3.1 model card: https://huggingface.co/meta-llama/Llama-3.1-70B-Instruct
