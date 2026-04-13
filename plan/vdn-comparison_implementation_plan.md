# Implement VDN Comparison Analyst

This plan outlines the creation of a custom DuckDB Processor `Analyzer` that loads the two specified input files, performs necessary preprocessing, compares their contents, and outputs the comparison results.

## Proposed Changes

### [NEW] [vdn_comparison.py](file:///c:/Users/Chimin.Jung/OneDrive%20-%20Lotus%20Tech%20Innovation%20Centre%20GmbH/Documents/Obsidian%20Vault/scripts/duckdb-processor-cli/duckdb_processor/analysts/vdn_comparison.py)
This new file will implement an `Analyzer` class to perform the comparison. Because the DuckDB Processor CLI naturally loads one file as its input (`data` table), we will pass the primary CSV file to the CLI, and the Analyzer will load the secondary Excel file internally.

**Steps implemented in the Analyzer:**
1. Use `pandas` to read the Excel file (since DuckDB may require extensions to parse Excel files natively) and load the target CSV file.
2. Clean column headers:
   - Identify mapping requirements: `'vin'` -> `'VIN'`, `'DB_SW'` -> `'CONSUMER_SW_VERSION'`, `'DB_targetVdns'` -> `'VDN_LIST'`.
3. Preprocess both files using pandas before querying them using DuckDB:
   - Trim whitespace and quotes from the string data.
   - For the source file (Excel), parse the JSON string arrays in `DB_targetVdns` into standard lists of strings.
   - For the target file (CSV), split the concatenated string in `VDN_LIST` into chunks of 4 characters.
4. Load the cleaned `pandas` DataFrames directly into the `Processor`'s under-the-hood DuckDB connection using `p.con.register()`.
5. Execute a SQL Join on the `vin` / `VIN` column to compare differences in the `VDN_LIST` arrays or software versions.

### Execution Command format:
After implementation is complete, the user can run this pipeline using:
```bash
python main.py input/01-PIE_EX30_VDN_list_report_20260330.csv --run vdn_comparison
```
(We will configure the `Analyzer` to automatically look for the Excel file inside the `input/` folder if not passed directly)

## Open Questions

> [!IMPORTANT]
> The target file uses an array of strings while the CSV has a 4-char contiguous string. Do we want the comparison summary to display added/removed VDN elements per VIN? And should we save the comparison result to an output CSV/Excel?

## Verification Plan
1. Run the script with the provided sample data.
2. Ensure we gracefully parse and expand arrays correctly without string artifacts like `[]` or `""`.
3. Provide sample comparison outputs (both matching and non-matching VDN lists).
