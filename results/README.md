# Benchmark Results & Statistical Reconstruction

This directory contains the complete raw experimental dataset and analysis code for the Phase 2 benchmark.

## Files

- `raw_trials.json`: Machine-readable trial dataset containing all 15 individual test runs (5 per architecture) with request counts, goodput, latency percentiles, compute times, and transit breakdowns.
- `raw_trials.csv`: Standard tabular CSV version of the 15 trials.
- `analyze.py`: Python analysis script that reconstructs the statistical summary tables, Student's t 95% confidence intervals, and hypothesis evaluations directly from the raw data.

## Running the Analysis

```bash
# From this directory:
python analyze.py

# Or from the repository root:
python results/analyze.py --data results/raw_trials.json --output results/summary_report.md
```

## Statistical Methodology

1. **Trial-Level Independence:**
   Because requests within an individual load test trial exhibit autocorrelation (shared TCP connection keep-alives, OS socket buffers, worker cache state), treated samples are the **5 independent trial percentile estimates** $(p^{(1)}, p^{(2)}, p^{(3)}, p^{(4)}, p^{(5)})$, rather than individual request timestamps.

2. **95% Confidence Intervals:**
   Reported uncertainty half-widths ($\pm$) are two-sided **Student's t Confidence Intervals** with degrees of freedom $df = n - 1 = 4$:
   $$\text{CI}_{95} = \pm 2.776 \times \frac{s}{\sqrt{5}}$$
   where $s = \sqrt{\frac{1}{4}\sum_{i=1}^5 (p^{(i)} - \bar{p})^2}$.

3. **Per-Request Transit Estimation:**
   For each request $i$, client duration $T_{\text{client}}^{(i)}$ and server execution $T_{\text{server}}^{(i)}$ are measured. The transit gap is computed per request:
   $$\Delta_{\text{transit}}^{(i)} = T_{\text{client}}^{(i)} - T_{\text{server}}^{(i)}$$
   The reported transit value is the median of these per-request differences.
