# VDN Comparison Tool

A high-performance Python utility for comparing vehicle software versions, models, and VDN configuration lists. It uses DuckDB for ultra-fast joining of large datasets and provides rich terminal output and detailed diagnostic reports.

## Key Features

- **Master Configuration System**: Control every aspect of the tool (normalization, formats, samples, mappings) via a central `config.json`.
- **Normalization Engine**: Group equivalent Model names or SW versions using `--normalize-models` and `--normalize-sw`. Audit trails are preserved showing `Standard(Original)`. Now supports **Empty Data Normalization**: map rows with missing data to a standard value by providing an empty alias (e.g., `"Factory SW,,"`).
- **Auto-Feature Detection**: Dynamically scales comparison logic based on available headers—checks only what it finds.
- **Performance Optimized**: Uses DuckDB's `QUALIFY` for pre-join deduplication and vectorized set operations for VDN difference extraction, ensuring high speed even on large fleet datasets.
- **Automated Reporting**: Generates a full suite of reports every run: Full Results (CSV), Mismatches Only (CSV), and Summaries in HTML, Markdown, and TXT.
- **Global Collapsible UI**: HTML reports feature interactive toggles (details/summary) for all sample sections, making it easy to navigate thousands of mismatches.
- **Incomplete Data Auditing**: Automatically identifies vehicles with missing mandatory information (e.g., missing Model or an empty VDN list) and provides a consolidated breakdown per VIN.
- **Custom Source Aliasing**: Replace generic "Source 1" and "Source 2" labels with professional names (e.g., "Production" vs "Database") throughout all reports, console logs, and mismatch tallies.
- **Robust 'NO DATA' Matching**: Correct identifies when both sides are missing data as a MATCH, preventing false positives for empty fields. Also includes a `--skip-nodata` feature to exclude incomplete vehicles entirely.
- **Data-Grid Optimization**: HTML reports are optimized for wide tables with sticky headers, zebra-striping, and **Vertical Column Highlighting** for easier navigation of complex rows.
- **Auditing & Data Integrity**: Includes a dedicated **Auditing Step** that flags duplicate VINs across files, identifies **VDN Prefix Conflicts** (e.g., multiple AT-series VDNs in one file), and catches **Incomplete Rows**.
- **Unique Vehicle Metrics**: All mismatch tallies and statistics count **unique VINs** (individual vehicles) rather than raw row occurrences, providing accurate fleet-wide diagnostics even with messy input data.
- **Pairwise VDN Diagnostics**: VDN mismatches are broken down into a **Pairwise Tally** (identifying specific code swaps reach-by-reach) with consolidated statistics for maximum readability.
- **Automated Filtering**: 
    - Use `--skip-filter` to drop specific records globally (e.g., Test vehicles) based on column values. **Advanced Logic**: Filters now run *after* normalization, allowing you to skip rows based on their normalized value (e.g., skip all "Factory SW" even if they were originally "No Data").
    - **Quiet Skip Behavior**: Records excluded by skip filters are treated as a **MATCH (OK)** rather than a mismatch. They still show `SKIPPED` in the data columns for auditing but are hidden from "Mismatch-only" reports to reduce noise.
    - Use `--skip-nodata` to automatically exclude any vehicle that is missing comparison data entirely from either side.
    - Use `--vdn-ignore` to strip specific 4-character codes (e.g., `9T00`, `FALS`) or families of codes using wildcards (e.g., `ME*`) from VDN lists during comparison.
- **Dynamic Column Augmentation**: Create new virtual columns on the fly by extracting substrings from existing data (e.g., VIN positions) and mapping them to readable values using lookup tables.

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
    "source1": "DB.csv",
    "source2": "PIE.csv",
    "s1_name": "Database",
    "s2_name": "Production",
    "vdn_ignore": ["9T00", "ME*", "FALS"],
    "augment": [
        {
            "name": "PLANT",
            "source": "VIN",
            "start": 10,
            "length": 1,
            "default": "Unknown",
            "condition_col": "MODEL",
            "compare": true,
            "conditional_lookups": {
                "EX30": { "1": "Torslanda", "2": "Ghent", "B": "Chengdu" },
                "PS4": { "1": "Luqiao", "L": "Luqiao" }
            }
        }
    ]
}
```
*Note: All mapping keys (`s1_map`, `s2_map`, `column_map`) are merged into a single "Intelligence Pool." This allows the tool to find your headers even if you swap the Source 1 and Source 2 files.*
*Note: The tool supports **any** custom column. While standard headers (VIN, SW, VDN, MODEL, REGION) have specialized parsing/normalization, any other column defined in your `column_map` will be treated as a **Dynamic Comparison Target**. You can compare them by adding their key to the `compare` list (e.g. `--compare battery sw`) or by setting `compare` to `["all"]` to include every mapped column automatically.*

## Quick Start

### 1. Graphical User Interface (Recommended)
The easiest way to use the tool is via the GUI, which allows you to visually map columns and manage settings without editing JSON files.
Run the GUI script (or the bundled `.exe`):
```bash
python vdn_compare_gui.py
```

### 2. Simple CLI Usage (File Dialog)
If you prefer the command line but still want to select files visually:
```bash
python vdn_compare.py
```

### 2. Normalization Example
Group release candidates with final versions and align model names:
```bash
python vdn_compare.py -s1 source1.csv -s2 source2.xlsx --normalize-sw "1.7.0,1.7.0.RC1" --normalize-models "EX30,V216"
```

### 4. Multi-Format Audit
Generate browser-friendly HTML reports and terminal summaries with all audit entries:
```bash
python vdn_compare.py --format html rich --samples all
```

## Graphical User Interface (GUI)

The VDN Compare GUI provides a professional, tabbed interface for managing complex comparisons.

### Run Settings Tab
- **File Selection**: Browse and select Source 1 and Source 2 files (CSV or Excel).
- **Configuration Persistence**: Save and load all settings to `config.json` with a single click.
- **Reporting Options**: Choose multiple output formats (HTML, MD, CSV, RICH) via checkboxes.
- **Advanced Filtering**: Configure VIN sorting, sample counts, and data integrity audits (Skip NoData).
- **Source Aliasing**: Define custom names for your datasets that persist in the generated reports.
- **Enhanced JSON Editors**: Multi-line, pretty-printed text areas for complex `Skip Filter` and `Custom Normalization` rules.
- **Console Output**: Real-time progress and summary display directly within the window.

### Column Settings Tab
- **Dynamic Header Loading**: Headers are automatically loaded from your selected files.
- **Visual Mapping**: Map file-specific column names to internal roles (VIN, SW, VDN, Model, Region).
- **Custom Roles**: Add arbitrary columns to the comparison engine on the fly.
- **Selection Toggles**: Use checkboxes to choose exactly which mapped columns should be compared.
- **Automatic Sync**: Mappings are automatically saved and passed to the comparison engine when you click "Run".
- **Column Augmentations**: 
    - Create virtual columns by extracting substrings from source data.
    - Manage rules in a scrollable grid with native **"Compare?" checkboxes**.
    - Configure **Conditional Lookups** that change based on other column values (e.g., Plant codes varying by Model).

## Argument Options

- `-s1`, `--source1`, `-s2`, `--source2`: Manually specify input paths.
- `--s1-name`, `--s2-name`: Custom labels for Source 1 and Source 2 (default: "Source 1" and "Source 2").
- `--use-default-input`: Bypass the file dialog and use default paths in `input/` (`DB.csv` and `PIE.csv`).
- `--samples`: Number of diagnostic samples to show in reports (integer or `all`, default: `10`).
- `--sort-vin`: Sort results by VIN (`asc`, `desc`, or `none`, default: `asc`).
- `--skip-filter`: Values to skip/exclude, in JSON format: `{"ColumnName": ["Value1", "Value2"]}`. Filters are aware of **Normalized Values**—if you normalize "No Data" to "Factory SW", you can skip "Factory SW" directly.
- `--skip-nodata`: (Boolean) Automatically skip rows where any requested comparison column is empty or null (Default: `false`).
- `--config`: Path to a custom configuration JSON (default: `config.json`).
- `--compare`: Comparison scope. Options: `sw`, `vdn`, `model`, `region`, `vin`, or `all` (default: `all`). `all` automatically includes every column defined in your `column_map`.
- `--vdn-ignore`: Space-separated list of 4-character VDN codes to ignore during comparison (e.g. `9T00 FALS`). Supports glob wildcards (e.g. `ME*` to ignore all variants starting with `ME`).
- `--augment`: JSON list of augmentation rules. Each rule defines a `name`, `source` column, `start` index, `length`, and a `lookup` or `conditional_lookups` dictionary.
- `--format`: Output format(s). Options: `html`, `md`, `rich`, `csv`.
- `--normalize-models`: Equivalency groups for models. Format: `"Standard,Alias1,Alias2"`.
- `--normalize-sw`: Equivalency groups for software. Format: `"Standard,Alias1,Alias2"`.
- `--normalize-custom`: Custom normalization rules in JSON format mapping generic column names to lists of equivalent groups. Best configured via `config.json`.

## Data Augmentation (Lookups)

The tool allows you to derive new data from existing columns. This is particularly useful for decoding VINs or tagging data with business-friendly labels.

### How it works:
1. **Extraction**: You specify a source column (like `VIN`), a start index (0-based), and a length.
2. **Simple Lookup**: The tool looks up the extracted string in a flat table (e.g., `{"B": "Chengdu", "1": "Torslanda"}`).
3. **Conditional Lookup**: If the meaning of a code changes based on another column (e.g., the plant code `1` means different things for different car models), you can use a conditional lookup:
   ```json
   {
       "EX30": { "1": "Torslanda", "2": "Ghent" },
       "PS4": { "1": "Luqiao" }
   }
   ```
4. **Comparison**: By checking the **"Compare?"** checkbox in the GUI (or setting `"compare": true` in JSON), these virtual columns are automatically included in the comparison reports and mismatch tallies.

## Output

All results are saved to the `output/` directory:
- **`full_comparison_results_[timestamp].csv`**: The complete joined dataset with all normalization applied.
- **`mismatch-only_[timestamp].csv`**: A targeted report containing ONLY conflicting rows.
- **`summary_[timestamp].html`**: Interactive, card-based diagnostic report with collapsible data grids.
- **`summary_[timestamp].md`**: Documentation-ready markdown summary for GitLab/Knowledge Bases.
- **`summary_[timestamp].txt`**: ASCII-grid formatted text report optimized for terminal review and email bodies.
