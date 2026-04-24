# VDN Comparison Tool

A high-performance Python utility for comparing vehicle software versions, models, and VDN configuration lists. It uses DuckDB for ultra-fast joining of large datasets and provides rich terminal output and detailed diagnostic reports.

## Key Features

- **Master Configuration System**: Control every aspect of the tool (normalization, formats, samples, mappings) via a central `config.json`.
- **Normalization Engine**: Group equivalent Model names or SW versions using `--normalize-models` and `--normalize-sw`. Audit trails are preserved showing `Standard(Original)`.
- **Auto-Feature Detection**: Dynamically scales comparison logic based on available headers—checks only what it finds.
- **Performance Optimized**: Uses DuckDB's `QUALIFY` for pre-join deduplication and vectorized set operations for VDN difference extraction, ensuring high speed even on large fleet datasets.
- **Automated Reporting**: Generates a full suite of reports every run: Full Results (CSV), Mismatches Only (CSV), and Summaries in HTML, Markdown, and TXT.
- **Global Collapsible UI**: HTML reports feature interactive toggles (details/summary) for all sample sections, making it easy to navigate thousands of mismatches.
- **Incomplete Data Auditing**: Automatically identifies vehicles with missing mandatory information (e.g., missing Model or an empty VDN list) and provides a consolidated breakdown per VIN.
- **Robust 'NO DATA' Matching**: Correct identifies when both sides are missing data as a MATCH, preventing false positives for empty fields.
- **Data-Grid Optimization**: HTML reports are optimized for wide tables with sticky headers, zebra-striping, and secure, document-ready styling.
- **Auditing & Data Integrity**: Includes a dedicated **Auditing Step** that flags duplicate VINs across files, identifies **VDN Prefix Conflicts** (e.g., multiple AT-series VDNs in one file), and catches **Incomplete Rows**.
- **Unique Vehicle Metrics**: All mismatch tallies and statistics count **unique VINs** (individual vehicles) rather than raw row occurrences, providing accurate fleet-wide diagnostics even with messy input data.
- **Pairwise VDN Diagnostics**: VDN mismatches are broken down into a **Pairwise Tally** (identifying specific code swaps reach-by-reach) with consolidated statistics for maximum readability.
- **Automated Filtering**: 
    - Use `--skip-filter` to drop specific records globally (e.g., Test vehicles) based on column values.
    - Use `--skip-nodata` to automatically exclude any vehicle that is missing comparison data entirely from either side.

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
    "compare": ["all"],
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
    "s1_map": { "DB_Header_VIN": "VIN" },
    "s2_map": { "PIE_Header_Chassis": "VIN" },
    "skip_filter": {
        "REGION": ["Internal", "Test"],
        "STATUS": ["Prototype"]
    },
    "skip_nodata": true
}
```
*Note: All mapping keys (`s1_map`, `s2_map`, `column_map`) are merged into a single "Intelligence Pool." This allows the tool to find your headers even if you swap the Source 1 and Source 2 files.*
*Note: The tool supports **any** custom column. While standard headers (VIN, SW, VDN, MODEL, REGION) have specialized parsing/normalization, any other column defined in your `column_map` will be treated as a **Dynamic Comparison Target**. You can compare them by adding their key to the `compare` list (e.g. `--compare battery sw`) or by setting `compare` to `["all"]` to include every mapped column automatically.*

## Quick Start

### 1. Simple Usage (File Dialog)
Run the script to open a window and select your files:
```bash
python vdn_compare.py
```

### 2. Normalization Example
Group release candidates with final versions and align model names:
```bash
python vdn_compare.py -s1 source1.csv -s2 source2.xlsx --normalize-sw "1.7.0,1.7.0.RC1" --normalize-models "EX30,V216"
```

### 3. Multi-Format Audit
Generate browser-friendly HTML reports and terminal summaries with all audit entries:
```bash
python vdn_compare.py --format html rich --samples all
```

## Argument Options

- `-s1`, `--source1`, `-s2`, `--source2`: Manually specify input paths.
- `--use-default-input`: Bypass the file dialog and use default paths in `input/` (`DB.csv` and `PIE.csv`).
- `--samples`: Number of diagnostic samples to show in reports (integer or `all`, default: `10`).
- `--sort-vin`: Sort results by VIN (`asc`, `desc`, or `none`, default: `asc`).
- `--skip-filter`: Values to skip/exclude, in JSON format: `{"ColumnName": ["Value1", "Value2"]}`. Matching rows are dropped for both Source 1 and Source 2.
- `--skip-nodata`: (Boolean) Automatically skip rows where any requested comparison column is empty or null (Default: `false`).
- `--config`: Path to a custom configuration JSON (default: `config.json`).
- `--compare`: Comparison scope. Options: `sw`, `vdn`, `model`, `region`, `vin`, or `all` (default: `all`). `all` automatically includes every column defined in your `column_map`.
- `--format`: Output format(s). Options: `html`, `md`, `rich`, `csv`.
- `--normalize-models`: Equivalency groups for models. Format: `"Standard,Alias1,Alias2"`.
- `--normalize-sw`: Equivalency groups for software. Format: `"Standard,Alias1,Alias2"`.
- `--normalize-custom`: Custom normalization rules in JSON format mapping generic column names to lists of equivalent groups. Best configured via `config.json`.

## Output

All results are saved to the `output/` directory:
- **`full_comparison_results_[timestamp].csv`**: The complete joined dataset with all normalization applied.
- **`mismatch-only_[timestamp].csv`**: A targeted report containing ONLY conflicting rows.
- **`summary_[timestamp].html`**: Interactive, card-based diagnostic report with collapsible data grids.
- **`summary_[timestamp].md`**: Documentation-ready markdown summary for GitLab/Knowledge Bases.
- **`summary_[timestamp].txt`**: ASCII-grid formatted text report optimized for terminal review and email bodies.
