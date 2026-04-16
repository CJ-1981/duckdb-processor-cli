# VDN Comparison Tool

A high-performance Python utility for comparing vehicle software versions, models, and VDN configuration lists. It uses DuckDB for ultra-fast joining of large datasets and provides rich terminal output and detailed diagnostic reports.

## Key Features

- **Master Configuration System**: Control every aspect of the tool (normalization, formats, samples, mappings) via a central `config.json`.
- **Normalization Engine**: Group equivalent Model names or SW versions using `--normalize-models` and `--normalize-sw`. Audit trails are preserved showing `Standard(Original)`.
- **Auto-Feature Detection**: Dynamically scales comparison logic based on available headers—checks only what it finds.
- **Robust 'NO DATA' Matching**: Correct identifies when both sides are missing data as a MATCH (no false positives for empty fields).
- **Automated Reporting**: Generates a full suite of reports every run: Full Results (CSV), Mismatches Only (CSV), and Summaries in HTML, Markdown, and TXT.
- **Interactive Reports**: HTML reports include a Table of Contents with jump-links and a floating "Back to Top" button for easy navigation of large datasets.
- **Data-Grid Optimization**: HTML reports are optimized for extremely wide tables with sticky headers and zebra-striping.
- **Dynamic Feature Comparison**: Adding new columns to the `column_map` (e.g., `REGION`) automatically triggers dedicated mismatch summaries, tallies, and sampling without code changes.
- **Unique Vehicle Metrics**: Mismatch tallies (like the SW Matrix and VDN Tally) now count **unique VINs** rather than raw data occurrences for more accurate fleet statistics.
- **Exclusion Filters**: Use `--skip-filter` to drop specific records (e.g., Test vehicles or Prototype regions) from the analysis globally.
- **Aggregated VDN Diagnostics**: VDN mismatches are broken down into a **Pairwise Tally** (identifying specific code swaps) while aggregating "noisy" additions/removals for readability.

## Installation

Ensure you have Python 3.8+ installed, then install the dependencies:

```bash
pip install -r requirements.txt
```

## Configuration (config.json)

The tool looks for a `config.json` in its directory to set default values for all arguments. This allows you to "set and forget" your environment-specific settings.

Example `config.json`:
```json
{
    "compare": ["sw", "vdn", "model"],
    "normalize_models": ["EX30,V216", "EX30 CC,V216-CC"],
    "normalize_sw": ["MY27 J1,27 J1"],
    "normalize_custom": {
        "BATTERY": ["69kWh,Standard Range", "82kWh,Extended Range"]
    },
    "format": ["rich", "html"],
    "column_map": { 
        "Shared_Model_Header": "MODEL",
        "Charge_Lvl": "BATTERY" 
    },
    "source_map": { "DB_Header_VIN": "VIN" },
    "target_map": { "PIE_Header_Chassis": "VIN" },
    "skip_filter": {
        "REGION": ["Internal", "Test"],
        "STATUS": ["Prototype"]
    }
}
```
*Note: All mapping keys (`source_map`, `target_map`, `column_map`) are merged into a single "Intelligence Pool." This allows the tool to find your headers even if you swap the Source and Target files.*
*Note: Any column mapped in `column_map` that is not a standard tool header (VIN, SW, VDN, MODEL) will be treated as a **Dynamic Comparison Target**, receiving its own tally and sample sections in all reports.*

## Quick Start

### 1. Simple Usage (File Dialog)
Run the script to open a window and select your files:
```bash
python vdn_compare.py
```

### 2. Normalization Example
Group release candidates with final versions and align model names:
```bash
python vdn_compare.py --normalize-sw "1.7.0,1.7.0.RC1" --normalize-models "EX30,V216"
```

### 3. Multi-Format Audit
Generate browser-friendly HTML reports and terminal summaries with all audit entries:
```bash
python vdn_compare.py --format html rich --samples all
```

## Argument Options

- `--source`, `--target`: Manually specify input paths.
- `--use-default-input`: Bypass the file dialog and use default paths in `input/` (`DB.csv` and `PIE.csv`).
- `--samples`: Number of diagnostic samples to show in reports (integer or `all`, default: `10`).
- `--sort-vin`: Sort results by VIN (`asc`, `desc`, or `none`, default: `asc`).
- `--skip-filter`: Values to skip/exclude, in JSON format: `{"ColumnName": ["Value1", "Value2"]}`. Matching rows are dropped for both Source and Target.
- `--config`: Path to a custom configuration JSON (default: `config.json`).
- `--compare`: Comparison scope. Options: `sw`, `vdn`, `model`, `vin` (default: `sw vdn model`).
- `--format`: Output format(s). Options: `html`, `md`, `rich`, `csv`.
- `--normalize-models`: Equivalency groups for models. Format: `"Standard,Alias1,Alias2"`.
- `--normalize-sw`: Equivalency groups for software. Format: `"Standard,Alias1,Alias2"`.
- `--normalize-custom`: Custom normalization rules in JSON format mapping generic column names to lists of equivalent groups. Best configured via `config.json`.

## Output

All results are saved to the `output/` directory:
- **`full_comparison_results_[timestamp].csv`**: The complete joined dataset.
- **`mismatch-only_[timestamp].csv`**: Filtered report containing only conflicting rows.
- **`summary_[timestamp].html`**: Interactive, card-based diagnostic report (Secure).
- **`summary_[timestamp].md`**: Documentation-ready markdown summary.
- **`summary_[timestamp].txt`**: ASCII-grid formatted text report for terminal reviews.
