# Convert VDN Compare to Analysts Plugin

We need to convert `util/vdn-compare/vdn_compare.py` into a reusable plugin for the `duckdb-processor-cli`. This is entirely doable since the `duckdb-processor-cli` allows for custom Python modules to be dropped into the `analysts_plugins/` directory and injected into the CLI workflow. 

## Proposed Approach

1. **Create the Plugin File**: Create a new file `analysts_plugins/vdn_compare.py`.
2. **Subclass BaseAnalyzer**: Use `@register` and subclass `BaseAnalyzer` so it is automatically discovered by `duckdb-processor-cli` (accessible via `--list-analyzers` and `--run vdn_compare`).
3. **Handle Inputs**: `vdn_compare` requires two distinct datasets (a Source DB and a Target PIE). 
   - We will check if the user passed multiple files to the CLI load command (e.g., `python main.py source:db.csv target:pie.csv`). If so, we'll extract them from the DuckDB `Processor` connection.
   - If the user simply runs `python main.py --run vdn_compare` without files, we will integrate the existing Tkinter File Dialog prompt to ask them to pick the Source and Target files, then load them dynamically into the active DuckDB connection using `p.con.execute("CREATE TABLE ... FROM read_csv_auto(...)")`.
4. **Integration**: We'll extract the core Pandas cleaning logic and the DuckDB SQL comparison query from the original `vdn_compare.py` and pipe them through the `Processor`'s connection (`p.con`).
5. **Output**: Rather than saving to hardcoded output directories inside `util/vdn-compare`, we will yield the result back to `p.last_result` to take advantage of `duckdb-processor-cli`'s built-in exporter (`--export-format csv/json/etc`). We'll also preserve the beautiful `rich` console mismatch matrix.

## User Review Required

> [!IMPORTANT]
> **What should we do with the original `vdn_compare.py` file?**
> Should I delete the original `/util/vdn-compare` folder entirely to keep the repo clean, or would you prefer I leave it intact for backwards compatibility and just add the new plugin?

> [!NOTE]
> **Argument Parsing**
> The original script used `argparse` for several specific flags (like `--sort-vin`, `--samples`, `--normalize-models`). `duckdb-processor-cli` runs plugins via a common string (`--run vdn_compare`), so we cannot easily inject custom CLI flags. I will hardcode the defaults into the plugin (e.g. `samples=10`, `sort_vin='asc'`) OR read from the existing `config.json` if it's placed in the root directory.

## Proposed Changes

### Plugin Layer

#### [NEW] [vdn_compare.py](file:///Users/chimin/Documents/script/duckdb-processor-cli/analysts_plugins/vdn_compare.py)
- Create a new subclass `VdnComparePlugin(BaseAnalyzer)`.
- Translate `main()` from `util/vdn-compare/vdn_compare.py` into `def run(self, p)`.
- Modify the file reading logic to either use CLI-provided files (`p._tables_info`) or pop open the TkDialog.

### Cleanup

#### [MODIFY/DELETE] [util/vdn-compare/vdn_compare.py](file:///Users/chimin/Documents/script/duckdb-processor-cli/util/vdn-compare/vdn_compare.py)
- Pending your preference on whether to delete or keep the standalone executable script.

## Verification Plan

### Automated Tests
1. Run `python main.py --list-analyzers` to ensure `vdn_compare` appears in the registry.
2. Run `python main.py util/vdn-compare/input/DB.csv util/vdn-compare/input/PIE.csv --run vdn_compare` to verify execution succeeds through the CLI pipeline.

### Manual Verification
1. Run `python main.py --run vdn_compare` to verify the application falls back correctly to the Tkinter prompt for missing files.
