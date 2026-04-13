import os
import sys
import argparse
import duckdb
import pandas as pd
import json
from datetime import datetime
import tkinter as tk
from tkinter import filedialog

# Windows UTF-8 re-configuration for correct box character rendering
if os.name == 'nt':
    import sys
    import io
    # Force the console to use UTF-8
    os.system('chcp 65001 > nul 2>&1')
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')

try:
    from rich.console import Console
    from rich.table import Table
    from rich import box
    has_rich = True
except ImportError:
    has_rich = False

if has_rich:
    _sys_console = Console()
    def cprint(msg, *args, **kwargs):
        _sys_console.print(msg, *args, **kwargs)
else:
    def cprint(msg, *args, **kwargs):
        print(msg)

try:
    from tqdm import tqdm
    has_tqdm = True
    tqdm.pandas()
except ImportError:
    has_tqdm = False

def load_file(file_path: str) -> pd.DataFrame:
    """Load a CSV or Excel file into a pandas DataFrame."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.xlsx' or ext == '.xls':
        return pd.read_excel(file_path)
    elif ext == '.csv':
        # Smart separator detection to avoid python engine's Sniffer issues on single columns
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                first_line = f.readline()
            if ';' in first_line: sep = ';'
            elif '\t' in first_line: sep = '\t'
            else: sep = ','
        except Exception:
            sep = ','
            
        return pd.read_csv(file_path, sep=sep, encoding='utf-8-sig')
    else:
        raise ValueError(f"Unsupported file format: {ext}")

def save_dataframe(df, file_path, fmt):
    """Helper to save a dataframe with error handling and directory creation."""
    os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)
    try:
        if fmt == 'csv':
            df.to_csv(file_path, index=False)
        else:
            try:
                # Limit markdown to avoid massive files crashing editors
                if len(df) > 5000:
                    cprint(f"[bold yellow]Warning:[/bold yellow] [bold white]{file_path}[/bold white] is very large ({len(df)} rows). Markdown might be slow to open.")
                df.to_markdown(file_path, index=False)
            except ImportError:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write('| ' + ' | '.join(df.columns) + ' |\n')
                    f.write('| ' + ' | '.join(['---'] * len(df.columns)) + ' |\n')
                    for _, row in df.iterrows():
                        f.write('| ' + ' | '.join(str(v).replace('\n', '<br>') for v in row.values) + ' |\n')
        cprint(f"[green]Saved report to[/green] [bold white]{file_path}[/bold white]")
    except PermissionError:
        cprint(f"\n[bold red][ERROR][/bold red] Permission denied: '{file_path}'. ")
        cprint("[yellow]Please ensure the file is NOT open in Excel or another program, then try again.[/yellow]")

def main():
    import time
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description="Compare Source and Target VDN reports.")
    parser.add_argument('--source', help="Source file", default="input/DB.csv")
    parser.add_argument('--target', help="Target PIE export file", default="input/PIE.csv")
    parser.add_argument('--format', nargs='+', choices=['csv', 'markdown', 'md', 'rich', 'html'], default=['rich', 'md', 'html'], help="Format(s) for summary output (can select multiple)")
    parser.add_argument('--sort-vin', choices=['none', 'asc', 'desc'], default='asc', help="Sort the output records by VIN (default: none, respects input order)")
    parser.add_argument('--samples', default='10', help="Number of samples to show in summary (integer or 'all', default: 10)")
    parser.add_argument('--pager', action='store_true', help="Use a pager to display long console tables")
    parser.add_argument('--use-default-input', action='store_true', help="Load default DB.csv and PIE.csv from /input without file select GUI dialog")
    parser.add_argument('--compare', nargs='+', default=['sw', 'vdn', 'model'], help='List of columns to compare. Options: sw, vdn, model, vin.')
    parser.add_argument('--normalize-models', nargs='+', default=['EX30,V216', 'EX30 CC,V216-CC'], help='Groups of equivalent models, comma-separated (e.g. "EX30,V216" "PS4,P417")')
    parser.add_argument('--normalize-sw', nargs='+', default=['MY27 J1,27 J1'], help='Groups of equivalent SW versions, comma-separated (e.g. "MY27 J1,27 J1" "1.8.0,1.8.0-hotfix")')
    parser.add_argument('--config', help="Path to a JSON file for custom configuration and column mapping", default="config.json")
    
    # 1. Parse known args first to find the config path
    temp_args, _ = parser.parse_known_args()
    
    config_data = {}
    if os.path.exists(temp_args.config):
        try:
            with open(temp_args.config, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            # Apply all top-level keys as default arguments (except the nested column map)
            parser_defaults = {k: v for k, v in config_data.items() if k != 'column_map'}
            if parser_defaults:
                parser.set_defaults(**parser_defaults)
            cprint(f"[cyan]Loaded configuration from: {temp_args.config}[/cyan]")
        except Exception as e:
            cprint(f"[yellow]Warning: Could not parse config file {temp_args.config}: {e}[/yellow]")
            config_data = {}

    # 2. Final parse, allowing CLI arguments to override the JSON defaults
    args = parser.parse_args()

    comp_flags = [c.lower() for c in args.compare]
    compare_sw = 'sw' in comp_flags
    compare_vdn = 'vdn' in comp_flags
    compare_model = 'model' in comp_flags

    # Determine if we should show the file dialog
    # Default behavior: show dialog UNLESS --use-default-input is used 
    # OR the user explicitly provided --source/--target via CLI
    manual_input = args.source != "input/DB.csv" or args.target != "input/PIE.csv"
    
    if not args.use_default_input and not manual_input:
        root = tk.Tk()
        root.withdraw()  # Hide the main tkinter window
        root.attributes('-topmost', True) # Bring dialog to front

        cprint("[cyan]Please select the Source file...[/cyan]")
        source_file = filedialog.askopenfilename(
            title="Select Source file",
            filetypes=[("CSV/Excel files", "*.csv *.xlsx *.xls"), ("All files", "*.*")]
        )
        if not source_file:
            cprint("[bold red]No source file selected. Exiting.[/bold red]")
            sys.exit(0)
        args.source = source_file

        cprint("[cyan]Please select Target file...[/cyan]")
        target_file = filedialog.askopenfilename(
            title="Select Target file",
            filetypes=[("CSV/Excel files", "*.csv *.xlsx *.xls"), ("All files", "*.*")]
        )
        if not target_file:
            cprint("[bold red]No target file selected. Exiting.[/bold red]")
            sys.exit(0)
        args.target = target_file
        
        root.destroy()

    cprint(f"[cyan]Loading Source:[/cyan] [bold white]{args.source}[/bold white]")
    df_source = load_file(args.source)
    
    cprint(f"[cyan]Loading Target:[/cyan] [bold white]{args.target}[/bold white]")
    df_target = load_file(args.target)

    # 1. Clean Column Headers & Map them
    # We will normalize BOTH dataframes to use standard headers: VIN, CONSUMER_SW_VERSION, VDN_LIST
    # 1. Smart Header Discovery & Mapping
    common_map = {
        'vin': 'VIN',
        'DB_SW': 'CONSUMER_SW_VERSION',
        'DB_targetVdns': 'VDN_LIST',
        'model': 'MODEL'
    }

    # Apply custom column map from config if present
    if 'column_map' in config_data:
        common_map.update(config_data['column_map'])
    
    df_source.rename(columns=common_map, inplace=True)
    df_target.rename(columns=common_map, inplace=True)
    
    # 1.5 Smart Comparison Downgrade
    # Automatically disable comparisons if columns are missing from EITHER file
    if compare_sw and ('CONSUMER_SW_VERSION' not in df_source.columns or 'CONSUMER_SW_VERSION' not in df_target.columns):
        cprint("[yellow]Auto-disabling SW comparison: Header missing in one or both files.[/yellow]")
        compare_sw = False
    if compare_model and ('MODEL' not in df_source.columns or 'MODEL' not in df_target.columns):
        cprint("[yellow]Auto-disabling Model comparison: Header missing in one or both files.[/yellow]")
        compare_model = False
    if compare_vdn and ('VDN_LIST' not in df_source.columns or 'VDN_LIST' not in df_target.columns):
        cprint("[yellow]Auto-disabling VDN comparison: Header missing in one or both files.[/yellow]")
        compare_vdn = False
    
    if 'VIN' not in df_source.columns or 'VIN' not in df_target.columns:
        cprint(f"[bold red]CRITICAL ERROR: 'VIN' column not found in one or both files.[/bold red]")
        cprint(f"Source columns: {df_source.columns.tolist()}")
        cprint(f"Target columns: {df_target.columns.tolist()}")
        sys.exit(1)

    # 2. Trim whitespaces and quotes, and normalize null-like values
    for df in (df_source, df_target):
        # Aggressive column name cleanup (strip hidden chars/quotes from headers)
        df.columns = [str(c).strip().replace('"', '') for c in df.columns]
        
        for col in df.columns:
            # Convert to string and clean
            df[col] = df[col].astype(str).str.strip().str.replace(r'^"|"$', '', regex=True)
            # Map null-like string representations back to None so DuckDB sees them as NULL
            df[col] = df[col].replace({'nan': None, 'NaN': None, 'None': None, '': None})
        
        # Custom business logic normalization for Model comparison
        if compare_model and 'MODEL' in df.columns:
            df['MODEL_NORM'] = df['MODEL']
            df['MODEL_DISPLAY'] = df['MODEL']
            for group in args.normalize_models:
                models = [m.strip() for m in group.split(',')]
                if len(models) > 1:
                    primary = models[0]
                    for alias in models[1:]:
                        # NORM receives the standard name for DuckDB matching
                        df.loc[df['MODEL'] == alias, 'MODEL_NORM'] = primary
                        # DISPLAY gets the explicit normalized indicator
                        df.loc[df['MODEL'] == alias, 'MODEL_DISPLAY'] = f"{primary}({alias})"

        # NEW: Custom business logic normalization for SW comparison
        if compare_sw and 'CONSUMER_SW_VERSION' in df.columns:
            df['SW_NORM'] = df['CONSUMER_SW_VERSION']
            df['SW_DISPLAY'] = df['CONSUMER_SW_VERSION']
            if args.normalize_sw:
                for group in args.normalize_sw:
                    versions = [v.strip() for v in group.split(',')]
                    if len(versions) > 1:
                        primary = versions[0]
                        for alias in versions[1:]:
                            df.loc[df['CONSUMER_SW_VERSION'] == alias, 'SW_NORM'] = primary
                            df.loc[df['CONSUMER_SW_VERSION'] == alias, 'SW_DISPLAY'] = f"{primary}({alias})"

    # 3. Parse VDN_LIST smartly
    def parse_vdn(val):
        if pd.isna(val) or str(val).strip() in ('nan', ''): return ['NO DATA']
        val_str = str(val).strip()
        
        # Check if it's a JSON array
        if val_str.startswith('[') and val_str.endswith(']'):
            try:
                parsed = json.loads(val_str.replace("'", '"'))
                if isinstance(parsed, list):
                    result = sorted(str(v).strip() for v in parsed if str(v).strip())
                    return result if result else ['NO DATA']
            except Exception:
                pass
                
        # If not, assume it's concatenated 4-char chunks
        chunks = [val_str[i:i+4] for i in range(0, len(val_str), 4)]
        result = sorted(c for c in chunks if c.strip())
        return result if result else ['NO DATA']

    for df in (df_source, df_target):
        if compare_vdn and 'VDN_LIST' in df.columns:
            if has_tqdm:
                cprint(f"[cyan]Parsing VDNs for {df.columns[0]}...[/cyan]")
                df['VDN_LIST_CLEAN'] = df['VDN_LIST'].progress_apply(lambda x: json.dumps(parse_vdn(x)))
            else:
                df['VDN_LIST_CLEAN'] = df['VDN_LIST'].apply(lambda x: json.dumps(parse_vdn(x)))
            # Free up memory containing original heavy string values early
            df.drop(columns=['VDN_LIST'], inplace=True)

    # 4. Compare with DuckDB
    con = duckdb.connect()
    con.register('source_db', df_source)
    con.register('target_db', df_target)

    sort_clause = f"ORDER BY vin {args.sort_vin.upper()}" if args.sort_vin != 'none' else ""
    
    s_selects = ["VIN as vin"]
    t_selects = ["VIN as vin"]
    if compare_sw: 
        s_selects.append("SW_NORM as source_sw")
        s_selects.append("SW_DISPLAY as source_sw_disp")
        t_selects.append("SW_NORM as target_sw")
        t_selects.append("SW_DISPLAY as target_sw_disp")
    if compare_model:
        s_selects.append("MODEL_NORM as source_model")
        s_selects.append("MODEL_DISPLAY as source_model_disp")
        t_selects.append("MODEL_NORM as target_model")
        t_selects.append("MODEL_DISPLAY as target_model_disp")
    if compare_vdn:
        s_selects.append("VDN_LIST_CLEAN as source_vdns_json")
        t_selects.append("VDN_LIST_CLEAN as target_vdns_json")
        
    s_selects_str = ",\n            ".join(s_selects)
    t_selects_str = ",\n            ".join(t_selects)
    
    joined_selects = ["COALESCE(s.vin, t.vin) as vin"]
    joined_selects.append("CASE WHEN s.vin IS NOT NULL THEN 1 ELSE 0 END as source_exists")
    joined_selects.append("CASE WHEN t.vin IS NOT NULL THEN 1 ELSE 0 END as target_exists")
    if compare_sw:
        joined_selects.append("CASE WHEN s.vin IS NULL THEN 'VIN not found in Source' ELSE COALESCE(s.source_sw_disp, 'NO DATA') END as source_sw_display")
        joined_selects.append("CASE WHEN t.vin IS NULL THEN 'VIN not found in Target' ELSE COALESCE(t.target_sw_disp, 'NO DATA') END as target_sw_display")
        joined_selects.append("CASE WHEN s.vin IS NULL OR t.vin IS NULL THEN 'MISMATCH' WHEN s.source_sw IS NOT DISTINCT FROM t.target_sw THEN 'MATCH' ELSE 'MISMATCH' END as sw_match")
    if compare_model:
        joined_selects.append("CASE WHEN s.vin IS NULL THEN 'N/A' ELSE COALESCE(s.source_model_disp, 'NO DATA') END as source_model_display")
        joined_selects.append("CASE WHEN t.vin IS NULL THEN 'N/A' ELSE COALESCE(t.target_model_disp, 'NO DATA') END as target_model_display")
        joined_selects.append("CASE WHEN s.vin IS NULL OR t.vin IS NULL THEN 'MISMATCH' WHEN s.source_model IS NOT DISTINCT FROM t.target_model THEN 'MATCH' ELSE 'MISMATCH' END as model_match")
    if compare_vdn:
        joined_selects.append("CASE WHEN s.vin IS NULL OR t.vin IS NULL THEN 'MISMATCH' WHEN s.source_vdns_json IS NOT DISTINCT FROM t.target_vdns_json THEN 'MATCH' ELSE 'MISMATCH' END as vdn_match")
        joined_selects.append("s.source_vdns_json as s_json")
        joined_selects.append("t.target_vdns_json as t_json")
        
    joined_selects_str = ",\n            ".join(joined_selects)
    
    final_selects = ["vin", "source_exists", "target_exists"]
    if compare_model:
        final_selects.extend(["source_model_display as source_model", "target_model_display as target_model", "model_match"])
    if compare_sw:
        final_selects.extend(["source_sw_display as source_sw", "target_sw_display as target_sw", "sw_match"])
    if compare_vdn:
        final_selects.extend(["vdn_match", "s_json as s_vdns_json", "t_json as t_vdns_json"])
        
    mismatch_conditions = []
    if compare_sw: mismatch_conditions.append("sw_match = 'MISMATCH'")
    if compare_model: mismatch_conditions.append("model_match = 'MISMATCH'")
    if compare_vdn: mismatch_conditions.append("vdn_match = 'MISMATCH'")
    
    if mismatch_conditions:
        result_cond = " OR ".join(mismatch_conditions)
        final_selects.append(f"CASE WHEN {result_cond} THEN 'NOK' ELSE 'OK' END as Result")
    else:
        final_selects.append("'OK' as Result")
        
    final_selects_str = ",\n        ".join(final_selects)

    compare_query = f"""
    WITH source_data AS (
        SELECT {s_selects_str}
        FROM source_db
        WHERE VIN IS NOT NULL AND VIN != 'nan'
    ),
    target_data AS (
        SELECT {t_selects_str}
        FROM target_db
        WHERE VIN IS NOT NULL AND VIN != 'nan'
    ),
    joined AS (
        SELECT
            {joined_selects_str}
        FROM source_data s
        FULL OUTER JOIN target_data t ON s.vin = t.vin
    )
    SELECT
        {final_selects_str}
    FROM joined
    {sort_clause}
    """
    
    result_df = con.execute(compare_query).df()
    
    import gc
    # Free up heavy dataframe hashes immediately
    del df_source
    del df_target
    con.close()
    gc.collect()

    # Advanced logic for Python-side extraction of VDN differences (Optimized)
    if compare_vdn:
        if has_tqdm:
            cprint("[cyan]Extracting VDN differences...[/cyan]")
            
        def compute_diff(row):
            if row['vdn_match'] != 'MISMATCH' or pd.isna(row['vdn_match']):
                return "", ""
                
            s_vdns_str = row.get('s_vdns_json')
            t_vdns_str = row.get('t_vdns_json')
            
            s_vdns = set(json.loads(s_vdns_str)) if pd.notna(s_vdns_str) else set()
            t_vdns = set(json.loads(t_vdns_str)) if pd.notna(t_vdns_str) else set()
            
            only_in_t = t_vdns - s_vdns
            only_in_s = s_vdns - t_vdns
            
            added = ", ".join(sorted(only_in_t)) if only_in_t else ""
            removed = ", ".join(sorted(only_in_s)) if only_in_s else ""
            
            return added, removed

        if has_tqdm:
            tqdm.pandas(desc="Computing Diffs")
            diffs = result_df.progress_apply(compute_diff, axis=1)
        else:
            diffs = result_df.apply(compute_diff, axis=1)

        result_df['Only in Target (missing in Source)'] = diffs.map(lambda x: x[0])
        result_df['Only in Source (missing in Target)'] = diffs.map(lambda x: x[1])
        
        final_output = result_df.drop(columns=['s_vdns_json', 't_vdns_json'], errors='ignore')
    else:
        final_output = result_df
    
    # Overall differences summary
    # 1. Identify VINs completely missing from one side
    missing_in_source = final_output[final_output['source_exists'] == 0]
    missing_in_target = final_output[final_output['target_exists'] == 0]
    
    # 2. Identify "True" mismatches (VIN exists in both sources but values differ)
    mismatched_sw = final_output[
        (final_output['sw_match'] == 'MISMATCH') & ~final_output['vin'].isin(missing_in_source['vin']) & ~final_output['vin'].isin(missing_in_target['vin'])
    ] if compare_sw else pd.DataFrame()
    
    mismatched_model = final_output[
        (final_output['model_match'] == 'MISMATCH') & ~final_output['vin'].isin(missing_in_source['vin']) & ~final_output['vin'].isin(missing_in_target['vin'])
    ] if compare_model else pd.DataFrame()
    
    mismatched_vdns = final_output[
        (final_output['vdn_match'] == 'MISMATCH') & ~final_output['vin'].isin(missing_in_source['vin']) & ~final_output['vin'].isin(missing_in_target['vin'])
    ] if compare_vdn else pd.DataFrame()
    
    # Prepare output paths
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs('output', exist_ok=True)
    
    full_report_path = f"output/full_comparison_results_{timestamp}.csv"
    m_path = f"output/mismatch-only_{timestamp}.csv"
    
    # Always save full report as CSV (Scalability/Performance)
    save_dataframe(final_output, full_report_path, 'csv')

    # Always save mismatches only as CSV
    mismatches_only = final_output[final_output['Result'] == 'NOK']
    save_dataframe(mismatches_only, m_path, 'csv')

    # Iterate through requested formats to generate summary files
    req_formats = list(set(f.lower() for f in args.format))
    
    # ---------------------------------------------------------
    # PART A: CONSOLE OUTPUT
    # ---------------------------------------------------------
    is_md = False # For console print
    summary_lines_console = [
        "COMPARISON METADATA",
        "-"*40,
        f"Source File: {os.path.basename(args.source)}",
        f"Target File: {os.path.basename(args.target)}",
        f"Full Report: {os.path.basename(full_report_path)}",
        f"Mismatches: {os.path.basename(m_path)}",
        "\nCOMPARISON RESULTS",
        "="*80,
        f"Total VINs Analyzed: {len(final_output)}",
        f"VINs missing in Source file: {len(missing_in_source)}",
        f"VINs missing in Target file: {len(missing_in_target)}",
        f"VINs with True Mismatched Model: {len(mismatched_model)}" if compare_model else None,
        f"VINs with True Mismatched SW: {len(mismatched_sw)}" if compare_sw else None,
        f"VINs with Matched SW: {len(final_output) - len(mismatched_sw) - len(missing_in_source) - len(missing_in_target)}" if compare_sw else None,
        f"VINs with True Mismatched VDNs: {len(mismatched_vdns)}" if compare_vdn else None,
        f"VINs with Matched VDNs: {len(final_output) - len(mismatched_vdns) - len(missing_in_source) - len(missing_in_target)}" if compare_vdn else None,
        ""
    ]
    summary_lines_console = [s for s in summary_lines_console if s is not None]
    
    for line in summary_lines_console[6:]: 
        if line.strip(): cprint(f"[bold cyan]{line.replace('- **', '').replace('**', '')}[/bold cyan]")

    # Helper function for pretty table printing to console
    def render_console_table(df, title, header_style="bold cyan", first_col_style="bold white"):
        if df.empty: return
        cprint(f"\n[{header_style}]{title}[/{header_style}]")
        if has_rich:
            console = Console(force_terminal=True)
            table_box = box.ASCII if os.name == 'nt' and args.pager else box.SQUARE
            table = Table(show_header=True, header_style=header_style, show_lines=True, box=table_box)
            for i, col in enumerate(df.columns):
                 # Truncate long content with ellipsis to keep table compact
                 table.add_column(
                     str(col), 
                     overflow="fold", 
                     style=first_col_style if i == 0 else None
                 )
            for _, row in df.iterrows():
                # Truncate very long values manually to keep table compact while allowing normal columns to wrap
                display_row = []
                for val in row.values:
                    v_str = str(val)
                    if len(v_str) > 40:
                        v_str = v_str[:37] + "..."
                    display_row.append(v_str)
                
                # Apply color styling for mismatches
                styled_row = [f"[bold red]{v}[/bold red]" if v.upper() in ['MISMATCH', 'NOK'] else v for v in display_row]
                table.add_row(*styled_row)
            if args.pager:
                with console.pager(styles=True): console.print(table)
            else: console.print(table)
        else:
            cprint(df.to_string(index=False))

    # 1. SW Version Matrix & Detailed List (Console)
    if compare_sw and not mismatched_sw.empty:
        header_text = "SW VERSION MISMATCH MATRIX (Source vs Target)"
        cprint(f"\n[bold magenta]{header_text}:[/bold magenta]")
        matrix_df = pd.crosstab(mismatched_sw['source_sw'], mismatched_sw['target_sw'], margins=True, margins_name='TOTAL')
        
        # Matrix View
        if has_rich:
            console = Console(force_terminal=True)
            matrix_df_reset = matrix_df.reset_index().rename(columns={'source_sw': 'Source SW(row)\\Target SW(col)'})
            table_box = box.ASCII if os.name == 'nt' and args.pager else box.SQUARE
            table = Table(show_header=True, header_style="bold magenta", show_lines=True, box=table_box)
            for i, col in enumerate(matrix_df_reset.columns): 
                table.add_column(str(col), overflow="fold", style="bold magenta" if i == 0 else None)
            for _, row in matrix_df_reset.iterrows(): table.add_row(*[f"[bold red]{val}[/bold red]" if str(val).isdigit() and int(val) > 0 else str(val) for val in row.values])
            if args.pager:
                with console.pager(styles=True): console.print(table)
            else: console.print(table)
        else:
            cprint(matrix_df.to_string())

        # Vertical List View
        # Detailed Table View
        sw_counts = mismatched_sw.groupby(['source_sw', 'target_sw']).size().reset_index(name='count')
        sw_counts = sw_counts.sort_values('count', ascending=False)
        render_console_table(sw_counts, "DETAILED SW MISMATCH TALLY", header_style="bold magenta")


    # 2. Sample Data for Console (Mismatches and Missing VINs)
    sample_limit = None if args.samples.lower() == 'all' else int(args.samples) if args.samples.isdigit() else 10
    
    # Model Mismatch Samples
    if compare_model and not mismatched_model.empty:
        df_mm = mismatched_model.head(sample_limit) if sample_limit else mismatched_model
        cols = ['vin', 'source_model', 'target_model', 'model_match']
        title_mm = f"{'SAMPLES: ' if sample_limit else ''}MODEL MISMATCHES ({len(df_mm)} entries out of total {len(mismatched_model)} findings)"
        render_console_table(df_mm[cols], title_mm, header_style="bold magenta")

    # Missing in Source Samples
    if not missing_in_source.empty:
        df_ms = missing_in_source.head(sample_limit) if sample_limit else missing_in_source
        cols = ['vin']
        if compare_model: cols.append('target_model')
        if compare_sw: cols.append('target_sw')
        title_ms = f"{'SAMPLES: ' if sample_limit else ''}VINs MISSING IN SOURCE ({len(df_ms)} entries out of total {len(missing_in_source)} findings)"
        render_console_table(df_ms[cols], title_ms, header_style="bold yellow")
        
    # Missing in Target Samples
    if not missing_in_target.empty:
        df_mt = missing_in_target.head(sample_limit) if sample_limit else missing_in_target
        cols = ['vin']
        if compare_model: cols.append('source_model')
        if compare_sw: cols.append('source_sw')
        title_mt = f"{'SAMPLES: ' if sample_limit else ''}VINs MISSING IN TARGET ({len(df_mt)} entries out of total {len(missing_in_target)} findings)"
        render_console_table(df_mt[cols], title_mt, header_style="bold yellow")

    # True Mismatches Samples
    mismatches = final_output[final_output['Result'] == 'NOK']
    # Filter to cases where both exist (True Mismatches)
    true_mismatches = mismatches[~mismatches['vin'].isin(missing_in_source['vin']) & ~mismatches['vin'].isin(missing_in_target['vin'])]
    if not true_mismatches.empty:
        sample_df = true_mismatches.head(sample_limit) if sample_limit else true_mismatches
        cols = ['vin']
        if compare_model: cols.extend(['source_model', 'target_model', 'model_match'])
        if compare_sw: cols.extend(['source_sw', 'target_sw', 'sw_match'])
        cols.append('Result')
        if compare_vdn: cols.extend(['vdn_match', 'Only in Target (missing in Source)', 'Only in Source (missing in Target)'])
        title_vdn = f"{'SAMPLES: ' if sample_limit else ''}VDN DATA MISMATCHES ({len(sample_df)} entries out of total {len(true_mismatches)} findings)"
        render_console_table(sample_df[cols], title_vdn, header_style="bold cyan")

    # ---------------------------------------------------------
    # PART B: GENERATE FILES
    # ---------------------------------------------------------
    for fmt in req_formats:
        is_md = fmt in ['markdown', 'md']
        is_html = fmt == 'html'
        
        sum_ext = ".html" if is_html else (".md" if is_md else (".txt" if fmt == 'rich' else ".csv"))
        curr_summary_path = f"output/summary_{timestamp}{sum_ext}"
        
        if is_md or is_html:
            title_prefix = "# " if is_md else "<h1>"
            title_suffix = "" if is_md else "</h1>"
            meta_prefix = "## " if is_md else "<h2>"
            res_prefix = "## " if is_md else "<h2>"
            
            summary_lines = [
                f"{title_prefix}Comparison Report{title_suffix}"
            ]
            # --- TABLE OF CONTENTS ---
            sample_prefix = "" if sample_limit is None else "Samples: "
            toc_lines = []
            if is_md:
                toc_lines.append("## Table of Contents")
                toc_lines.append("- [Comparison Metadata](#comparison-metadata)")
                toc_lines.append("- [Comparison Results](#comparison-results)")
                if compare_sw and not mismatched_sw.empty:
                    toc_lines.append("- [SW Mismatch Matrix](#sw-mismatch-matrix)")
                    toc_lines.append("- [Detailed SW Mismatch Tally](#detailed-sw-mismatch-tally)")
                if compare_model and not mismatched_model.empty:
                    toc_lines.append("- [Detailed Model Mismatch Tally](#detailed-model-mismatch-tally)")
                
                # Dynamic Sample Links
                if compare_model and not mismatched_model.empty: toc_lines.append(f"- [{sample_prefix}Model Mismatches](#samples-model-mismatches)")
                if not true_mismatches.empty: toc_lines.append(f"- [{sample_prefix}VDN Data Mismatches](#samples-all-data-mismatches)")
                if not missing_in_source.empty: toc_lines.append(f"- [{sample_prefix}VINs Missing in Source](#samples-missing-in-source)")
                if not missing_in_target.empty: toc_lines.append(f"- [{sample_prefix}VINs Missing in Target](#samples-missing-in-target)")
                toc_lines.append("")
            elif is_html:
                summary_lines.append("<div class='toc'><h2>Table of Contents</h2><ul>")
                summary_lines.append("<li><a href='#comparison-metadata'>Comparison Metadata</a></li>")
                summary_lines.append("<li><a href='#comparison-results'>Comparison Results</a></li>")
                if compare_sw and not mismatched_sw.empty:
                    summary_lines.append("<li><a href='#sw-mismatch-matrix'>SW Mismatch Matrix</a></li>")
                    summary_lines.append("<li><a href='#detailed-sw-mismatch-tally'>Detailed SW Mismatch Tally</a></li>")
                if compare_model and not mismatched_model.empty:
                    summary_lines.append("<li><a href='#detailed-model-mismatch-tally'>Detailed Model Mismatch Tally</a></li>")
                
                # HTML Links
                if compare_model and not mismatched_model.empty: summary_lines.append(f"<li><a href='#samples-model-mismatches'>{sample_prefix}Model Mismatches</a></li>")
                if not true_mismatches.empty: summary_lines.append(f"<li><a href='#samples-all-data-mismatches'>{sample_prefix}VDN Data Mismatches</a></li>")
                if not missing_in_source.empty: summary_lines.append(f"<li><a href='#samples-missing-in-source'>{sample_prefix}VINs Missing in Source</a></li>")
                if not missing_in_target.empty: summary_lines.append(f"<li><a href='#samples-missing-in-target'>{sample_prefix}VINs Missing in Target</a></li>")
                summary_lines.append("</ul></div>")

            summary_lines.extend(toc_lines)
            
            # --- METADATA ---
            summary_lines.append(f"{meta_prefix if is_md else '<h2 id=\"comparison-metadata\">'}Comparison Metadata{'</h2>' if is_html else ''}")
            if is_html: summary_lines.append("<ul>")
            summary_lines.extend([
                f"- **Source File**: `{os.path.basename(args.source)}`" if is_md else f"<li>Source File: {os.path.basename(args.source)}</li>",
                f"- **Target File**: `{os.path.basename(args.target)}`" if is_md else f"<li>Target File: {os.path.basename(args.target)}</li>",
                f"- **Full Report**: `{os.path.basename(full_report_path)}`" if is_md else f"<li>Full Report: {os.path.basename(full_report_path)}</li>",
                f"- **Mismatches Only**: `{os.path.basename(m_path)}`" if is_md else f"<li>Mismatches Only: {os.path.basename(m_path)}</li>"
            ])
            if is_html: summary_lines.append("</ul>")
            summary_lines.extend(["", f"{res_prefix if is_md else '<h2 id=\"comparison-results\">'}Comparison Results{'</h2>' if is_html else ''}"])
            
            # Helper to add summary lines with correct format
            def add_line(label, value):
                if is_md: summary_lines.append(f"- **{label}**: {value}")
                else: summary_lines.append(f"<li>{label}: {value}</li>")

            if not is_md: summary_lines.append("<ul>")
            add_line("Total VINs Analyzed", len(final_output))
            add_line("VINs missing in Source file", len(missing_in_source))
            add_line("VINs missing in Target file", len(missing_in_target))
            if compare_model: add_line("VINs with True Mismatched Model", len(mismatched_model))
            if compare_sw: 
                add_line("VINs with True Mismatched SW", len(mismatched_sw))
                add_line("VINs with Matched SW", len(final_output) - len(mismatched_sw) - len(missing_in_source) - len(missing_in_target))
            if compare_vdn:
                add_line("VINs with True Mismatched VDNs", len(mismatched_vdns))
                add_line("VINs with Matched VDNs", len(final_output) - len(mismatched_vdns) - len(missing_in_source) - len(missing_in_target))
            if not is_md: summary_lines.append("</ul>")
            summary_lines.append("")

            if is_html:
                # Add data-grid optimized HTML boilerplate
                html_style = "<style>body{font-family:-apple-system,BlinkMacSystemFont,\"Segoe UI\",Roboto,Arial,sans-serif;font-size:14px;line-height:1.5;color:#333;margin:20px;background:#fff}h1,h2{color:#2c3e50;border-bottom:2px solid #eee;padding-bottom:5px;margin-top:20px}h1:first-child,h2:first-child{margin-top:0}table{border-collapse:collapse;margin-bottom:30px;font-size:13px;width:auto;min-width:50%;max-width:none}th,td{border:1px solid #dcdcdc;padding:6px 10px;text-align:left;vertical-align:top;white-space:nowrap}th{background:#34495e;color:#fff;font-weight:600;white-space:nowrap;position:sticky;top:0}tr:nth-child(even){background:#f8f9fa}tr:hover{background:#e9ecef}.mismatch{color:#e74c3c;font-weight:bold}ul{margin-bottom:20px}li{margin-bottom:5px}.toc{background:#fdfdfd;border:1px solid #eee;padding:15px;border-radius:5px;display:inline-block;min-width:300px}.toc h2{margin-top:0}.toc ul{margin-bottom:0}.back-to-top{position:fixed;bottom:20px;right:20px;background:#34495e;color:#fff;padding:10px 15px;border-radius:5px;text-decoration:none;font-weight:600;font-size:12px;box-shadow:0 2px 5px rgba(0,0,0,0.2);z-index:1000}.back-to-top:hover{background:#2c3e50}</style>"
                html_head = f"<!DOCTYPE html>\n<html>\n<head>\n<meta charset='utf-8'>\n<title>VDN Comparison Report</title>\n{html_style}\n</head>\n<body>\n<a href='#' class='back-to-top'>TOP &uarr;</a>"
                summary_lines.insert(0, html_head)
        else:
            summary_lines = [
                "COMPARISON METADATA", "-"*40,
                f"Source File: {os.path.basename(args.source)}",
                f"Target File: {os.path.basename(args.target)}",
                f"Full Report: {os.path.basename(full_report_path)}",
                f"Mismatches: {os.path.basename(m_path)}",
                "\nCOMPARISON RESULTS", "="*80,
                f"Total VINs Analyzed: {len(final_output)}",
                f"VINs missing in Source file: {len(missing_in_source)}",
                f"VINs missing in Target file: {len(missing_in_target)}",
                f"VINs with True Mismatched Model: {len(mismatched_model)}" if compare_model else None,
                f"VINs with True Mismatched SW: {len(mismatched_sw)}" if compare_sw else None,
                f"VINs with Matched SW: {len(final_output) - len(mismatched_sw) - len(missing_in_source) - len(missing_in_target)}" if compare_sw else None,
                f"VINs with True Mismatched VDNs: {len(mismatched_vdns)}" if compare_vdn else None,
                f"VINs with Matched VDNs: {len(final_output) - len(mismatched_vdns) - len(missing_in_source) - len(missing_in_target)}" if compare_vdn else None,
                ""
            ]

        summary_lines = [s for s in summary_lines if s is not None]

        if compare_sw and not mismatched_sw.empty:
            matrix_df = pd.crosstab(mismatched_sw['source_sw'], mismatched_sw['target_sw'], margins=True, margins_name='TOTAL')
            header_text = "SW VERSION MISMATCH MATRIX (Source vs Target)"
            anchor_id = "sw-mismatch-matrix"
            if is_md or is_html:
                sub_prefix = f"## {header_text}" if is_md else f"<h2 id=\"{anchor_id}\">{header_text}</h2>"
                summary_lines.append(f"\n{sub_prefix}\n")
                # Color code figures
                disp_matrix = matrix_df.map(lambda x: f'<span class="mismatch" style="color:red">{x}</span>' if str(x).isdigit() and int(x) > 0 else str(x))
                matrix_styled = disp_matrix.reset_index().rename(columns={'source_sw': 'Source SW(row)\\Target SW(col)'})
                matrix_styled.columns.name = None # Remove the 'target_sw' ghost header
                if is_md:
                    summary_lines.append(matrix_styled.to_markdown(index=False))
                else:
                    summary_lines.append(matrix_styled.to_html(index=False, escape=False))
            elif fmt == 'rich' and has_rich:
                from io import StringIO
                capture_console = Console(file=StringIO(), force_terminal=False, width=250)
                matrix_df_reset = matrix_df.reset_index().rename(columns={'source_sw': 'Source SW(row)\\Target SW(col)'})
                table = Table(show_header=True, header_style="bold magenta", show_lines=True, box=box.ASCII)
                for i, col in enumerate(matrix_df_reset.columns): table.add_column(str(col), overflow="fold", style="bold magenta" if i == 0 else None)
                for _, row in matrix_df_reset.iterrows(): table.add_row(*[f"[bold red]{val}[/bold red]" if str(val).isdigit() and int(val) > 0 else str(val) for val in row.values])
                capture_console.print(table)
                summary_lines.append(f"\n{header_text}:")
                summary_lines.append(capture_console.file.getvalue())
            else:
                summary_lines.append(f"\n{header_text}:\n" + matrix_df.to_string())

            # Detailed Table View
            header_list = "DETAILED SW MISMATCH TALLY"
            anchor_id = "detailed-sw-mismatch-tally"
            sw_counts = mismatched_sw.groupby(['source_sw', 'target_sw']).size().reset_index(name='count')
            sw_counts = sw_counts.sort_values('count', ascending=False)
            
            if is_md or is_html:
                sub_prefix = f"## {header_list}" if is_md else f"<h2 id=\"{anchor_id}\">{header_list}</h2>"
                summary_lines.append(f"\n{sub_prefix}\n")
                if is_md:
                    summary_lines.append(sw_counts.to_markdown(index=False))
                else:
                    summary_lines.append(sw_counts.to_html(index=False, escape=False))
            else:
                summary_lines.append(f"\n{header_list}:")
                summary_lines.append(sw_counts.to_string(index=False))


        def save_sample_section(df_sample, title, style_color, anchor_id=None):
            # Create a URL-safe anchor ID if not provided
            if not anchor_id:
                import re
                anchor_id = re.sub(r'[^a-zA-Z0-9]+', '-', title.lower()).strip('-')
            
            if is_md or is_html:
                sub_prefix = f"## {title}" if is_md else f"<h2 id=\"{anchor_id}\">{title}</h2>"
                summary_lines.append(f"\n{sub_prefix}\n")
                md_s = df_sample.copy()
                # Bold/Bold-ish formatting
                if not md_s.empty:
                    if is_md:
                        md_s.iloc[:, 0] = md_s.iloc[:, 0].apply(lambda x: f"**{x}**")
                    else:
                        md_s.iloc[:, 0] = md_s.iloc[:, 0].apply(lambda x: f"{x}")
                
                # Truncation logic (use the same logic we have for console to avoid double logic)
                for col in md_s.columns:
                    md_s[col] = md_s[col].apply(lambda x: str(x)[:37] + "..." if len(str(x)) > 40 else str(x))
                
                red_indicators = ['MISMATCH', 'NOK']
                md_s = md_s.map(lambda x: f'<span style="color:red" class="mismatch">{x}</span>' if str(x).upper() in red_indicators else str(x))
                if is_md:
                    summary_lines.append(md_s.to_markdown(index=False))
                else:
                    summary_lines.append(md_s.to_html(index=False, escape=False))
            elif fmt == 'rich' and has_rich:
                from io import StringIO
                capture_con = Console(file=StringIO(), force_terminal=False, width=250)
                tbl = Table(show_header=True, header_style=style_color, show_lines=True, box=box.ASCII)
                for idx, c in enumerate(df_sample.columns):
                    tbl.add_column(
                        str(c), 
                        overflow="fold", 
                        style="bold white" if idx == 0 else None
                    )
                for _, r in df_sample.iterrows():
                    # Truncate long strings manually for the saved report content
                    display_vals = []
                    for v in r.values:
                        v_s = str(v)
                        if len(v_s) > 40:
                            v_s = v_s[:37] + "..."
                        display_vals.append(v_s)
                    
                    styled_vals = [f"[bold red]{val}[/bold red]" if val.upper() in ['MISMATCH', 'NOK'] else val for val in display_vals]
                    tbl.add_row(*styled_vals)
                capture_con.print(tbl)
                summary_lines.append(f"\n{title}:")
                summary_lines.append(capture_con.file.getvalue())
            elif fmt == 'csv':
                summary_lines.append(f"\n{title}:\n" + df_sample.to_csv(index=False))
            else:
                summary_lines.append(f"\n{title}:\n" + df_sample.to_string(index=False))

        if compare_model and not mismatched_model.empty:
            df_mm = mismatched_model.head(sample_limit) if sample_limit else mismatched_model
            cols = ['vin', 'source_model', 'target_model', 'model_match']
            title_m = f"{'SAMPLES: ' if sample_limit else ''}MODEL MISMATCHES ({len(df_mm)} entries out of total {len(mismatched_model)} findings)"
            save_sample_section(df_mm[cols], title_m, "bold magenta", anchor_id="samples-model-mismatches")
            
            # Detailed Table View for Models
            header_list_m = "DETAILED MODEL MISMATCH TALLY"
            anchor_id = "detailed-model-mismatch-tally"
            m_counts = mismatched_model.groupby(['source_model', 'target_model']).size().reset_index(name='count')
            m_counts = m_counts.sort_values('count', ascending=False)
            
            if is_md or is_html:
                sub_prefix = f"## {header_list_m}" if is_md else f"<h2 id=\"{anchor_id}\">{header_list_m}</h2>"
                summary_lines.append(f"\n{sub_prefix}\n")
                if is_md:
                    summary_lines.append(m_counts.to_markdown(index=False))
                else:
                    summary_lines.append(m_counts.to_html(index=False, escape=False))
            else:
                summary_lines.append(f"\n{header_list_m}:")
                summary_lines.append(m_counts.to_string(index=False))

        if not true_mismatches.empty:
            df_s = true_mismatches.head(sample_limit) if sample_limit else true_mismatches
            title_all = f"{'SAMPLES: ' if sample_limit else ''}VDN DATA MISMATCHES ({len(df_s)} entries out of total {len(true_mismatches)} findings)"
            cols = ['vin']
            if compare_model: cols.extend(['source_model', 'target_model', 'model_match'])
            if compare_sw: cols.extend(['source_sw', 'target_sw', 'sw_match'])
            cols.append('Result')
            if compare_vdn: cols.extend(['vdn_match', 'Only in Target (missing in Source)', 'Only in Source (missing in Target)'])
            save_sample_section(df_s[cols], title_all, "bold cyan", anchor_id="samples-all-data-mismatches")

        if not missing_in_source.empty:
            df_ms = missing_in_source.head(sample_limit) if sample_limit else missing_in_source
            title_ms = f"{'SAMPLES: ' if sample_limit else ''}VINs MISSING IN SOURCE ({len(df_ms)} entries out of total {len(missing_in_source)} findings)"
            cols = ['vin']
            if compare_model: cols.append('target_model')
            if compare_sw: cols.append('target_sw')
            save_sample_section(df_ms[cols], title_ms, "bold yellow", anchor_id="samples-missing-in-source")
            
        if not missing_in_target.empty:
            df_mt = missing_in_target.head(sample_limit) if sample_limit else missing_in_target
            title_mt = f"{'SAMPLES: ' if sample_limit else ''}VINs MISSING IN TARGET ({len(df_mt)} entries out of total {len(missing_in_target)} findings)"
            cols = ['vin']
            if compare_model: cols.append('source_model')
            if compare_sw: cols.append('source_sw')
            save_sample_section(df_mt[cols], title_mt, "bold yellow", anchor_id="samples-missing-in-target")

        if mismatches.empty and missing_in_source.empty and missing_in_target.empty:
            summary_lines.append("\nNo Differences or Missing VINs Found!")

        if is_html:
            summary_lines.append("</body></html>")

        # Save the file
        summary_text = "\n".join(summary_lines)
        with open(curr_summary_path, 'w', encoding='utf-8') as f: f.write(summary_text)
        cprint(f"[green]Saved Summary ({fmt}) to[/green] [bold white]{curr_summary_path}[/bold white]")

    duration = time.time() - start_time
    cprint(f"\n[bold green]Processing completed in {duration:.2f} seconds.[/bold green]")
    input("\nPress Enter to exit...")

if __name__ == '__main__':
    main()
