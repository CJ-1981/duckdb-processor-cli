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
        # Let pandas auto-detect the separator by using engine='python' and sep=None
        return pd.read_csv(file_path, sep=None, engine='python')
    else:
        raise ValueError(f"Unsupported file format: {ext}")

def main():
    parser = argparse.ArgumentParser(description="Compare Source and Target VDN reports.")
    parser.add_argument('--source', help="Source file", default="input/DB.csv")
    parser.add_argument('--target', help="Target PIE export file", default="input/PIE.csv")
    parser.add_argument('--mismatch-only', nargs='?', const='default', help="Optional output file for Mismatches only")
    parser.add_argument('--summary', nargs='?', const='default', help="Optional output file for the Summary and SW version matrix")
    parser.add_argument('--format', choices=['csv', 'markdown', 'md', 'rich'], default='rich', help="Format for summary output")
    parser.add_argument('--sort-vin', choices=['none', 'asc', 'desc'], default='none', help="Sort the output records by VIN (default: none, respects input order)")
    parser.add_argument('--samples', default='10', help="Number of samples to show in summary (integer or 'all', default: 10)")
    parser.add_argument('--pager', action='store_true', help="Use a pager to display long console tables")
    parser.add_argument('--use-default-input', action='store_true', help="Load default DB.csv and PIE.csv from /input without dialog")
    args = parser.parse_args()

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
    common_map = {
        'vin': 'VIN',
        'DB_SW': 'CONSUMER_SW_VERSION',
        'DB_targetVdns': 'VDN_LIST'
    }
    
    df_source.rename(columns=common_map, inplace=True)
    df_target.rename(columns=common_map, inplace=True)
    
    # Verify standard columns exist
    for col in ['VIN', 'CONSUMER_SW_VERSION', 'VDN_LIST']:
        if col not in df_source.columns:
            cprint(f"[bold yellow]Warning:[/bold yellow] Expected column '{col}' not found in Source file (resolved to {df_source.columns.tolist()})")
        if col not in df_target.columns:
            cprint(f"[bold yellow]Warning:[/bold yellow] Expected column '{col}' not found in Target file (resolved to {df_target.columns.tolist()})")

    # 2. Trim whitespaces and quotes from the data cells
    for df in (df_source, df_target):
        for col in df.columns:
            if df[col].dtype == object or df[col].dtype == pd.StringDtype:
                df[col] = df[col].astype(str).str.strip().str.replace(r'^"|"$', '', regex=True)

    # 3. Parse VDN_LIST smartly
    def parse_vdn(val):
        if pd.isna(val) or str(val).strip() in ('nan', ''): return []
        val_str = str(val).strip()
        
        # Check if it's a JSON array
        if val_str.startswith('[') and val_str.endswith(']'):
            try:
                parsed = json.loads(val_str.replace("'", '"'))
                if isinstance(parsed, list):
                    return sorted(str(v).strip() for v in parsed)
            except Exception:
                pass
                
        # If not, assume it's concatenated 4-char chunks
        chunks = [val_str[i:i+4] for i in range(0, len(val_str), 4)]
        return sorted(c for c in chunks if c.strip())

    for df in (df_source, df_target):
        if 'VDN_LIST' in df.columns:
            if has_tqdm:
                cprint(f"[cyan]Parsing VDNs for {df.columns[0]}...[/cyan]")
                df['VDN_LIST_PARSED'] = df['VDN_LIST'].progress_apply(parse_vdn) if has_tqdm else df['VDN_LIST'].apply(parse_vdn)
            else:
                df['VDN_LIST_PARSED'] = df['VDN_LIST'].apply(parse_vdn)
            df['VDN_LIST_CLEAN'] = df['VDN_LIST_PARSED'].apply(json.dumps)

    # 4. Compare with DuckDB
    con = duckdb.connect()
    con.register('source_db', df_source)
    con.register('target_db', df_target)

    sort_clause = f"ORDER BY vin {args.sort_vin.upper()}" if args.sort_vin != 'none' else ""
    compare_query = f"""
    WITH source_data AS (
        SELECT 
            VIN as vin, 
            CONSUMER_SW_VERSION as source_sw, 
            VDN_LIST_CLEAN as source_vdns_json
        FROM source_db
        WHERE VIN IS NOT NULL AND VIN != 'nan'
    ),
    target_data AS (
        SELECT 
            VIN as vin, 
            CONSUMER_SW_VERSION as target_sw, 
            VDN_LIST_CLEAN as target_vdns_json
        FROM target_db
        WHERE VIN IS NOT NULL AND VIN != 'nan'
    ),
    joined AS (
        SELECT
            COALESCE(s.vin, t.vin) as vin,
            CASE 
                WHEN s.vin IS NULL THEN 'VIN not found in Source'
                ELSE COALESCE(s.source_sw, 'N/A')
            END as source_sw_display,
            CASE 
                WHEN t.vin IS NULL THEN 'VIN not found in Target'
                ELSE COALESCE(t.target_sw, 'N/A')
            END as target_sw_display,
            CASE 
                WHEN s.vin IS NULL OR t.vin IS NULL THEN 'MISMATCH'
                WHEN s.source_sw = t.target_sw THEN 'MATCH' 
                ELSE 'MISMATCH' 
            END as sw_match,
            CASE 
                WHEN s.vin IS NULL OR t.vin IS NULL THEN 'MISMATCH'
                WHEN s.source_vdns_json = t.target_vdns_json THEN 'MATCH' 
                ELSE 'MISMATCH' 
            END as vdn_match
        FROM source_data s
        FULL OUTER JOIN target_data t ON s.vin = t.vin
    )
    SELECT
        vin,
        source_sw_display as source_sw,
        target_sw_display as target_sw,
        sw_match,
        vdn_match,
        CASE WHEN sw_match = 'MISMATCH' OR vdn_match = 'MISMATCH' THEN 'NOK' ELSE 'OK' END as Result
    FROM joined
    {sort_clause}
    """
    
    result_df = con.execute(compare_query).df()
    
    # Advanced logic for Python-side extraction of VDN differences
    source_dict = df_source.set_index('VIN')['VDN_LIST_PARSED'].to_dict() if 'VIN' in df_source.columns and 'VDN_LIST_PARSED' in df_source.columns else {}
    target_dict = df_target.set_index('VIN')['VDN_LIST_PARSED'].to_dict() if 'VIN' in df_target.columns and 'VDN_LIST_PARSED' in df_target.columns else {}

    diff_list = []
    iterable = result_df['vin']
    if has_tqdm:
        cprint("[cyan]Extracting VDN differences...[/cyan]")
        iterable = tqdm(iterable, desc="Comparing VINs")
        
    for vin in iterable:
        s_vdns = set(source_dict.get(vin, []))
        t_vdns = set(target_dict.get(vin, []))
        
        only_in_source = s_vdns - t_vdns
        only_in_target = t_vdns - s_vdns
        
        added = ", ".join(sorted(only_in_target)) if only_in_target else ""
        removed = ", ".join(sorted(only_in_source)) if only_in_source else ""
        
        diff_list.append({
            "Only in Target (missing in Source)": added, 
            "Only in Source (missing in Target)": removed
        })
        
    diff_df = pd.DataFrame(diff_list)
    final_output = pd.concat([result_df, diff_df], axis=1)

    # Print Console Header
    # cprint(f"\n[bold magenta]{'='*80}[/bold magenta]")
    # cprint("[bold magenta]COMPARISON RESULTS:[/bold magenta]")
    # cprint(f"[bold magenta]{'='*80}[/bold magenta]")
    
    # Overall differences summary
    mismatched_sw = final_output[final_output['sw_match'] == 'MISMATCH']
    mismatched_vdns = final_output[final_output['vdn_match'] == 'MISMATCH']
    
    # Prepare output paths
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs('output', exist_ok=True)
    full_report_path = f"output/vdn_comparison_results_{timestamp}.csv"
    
    # Calculate Summary Path early
    summary_path = args.summary
    if not summary_path or summary_path == 'default':
        sum_ext = ".md" if args.format in ['markdown', 'md'] else (".txt" if args.format == 'rich' else ".csv")
        summary_path = f"output/summary_{timestamp}{sum_ext}"
    if (args.format == 'markdown' or args.format == 'md') and not (summary_path.endswith('.md') or summary_path.endswith('.markdown')):
        summary_path = os.path.splitext(summary_path)[0] + ".md"

    # Calculate Mismatch Path early
    m_path = args.mismatch_only
    if not m_path or m_path == 'default':
        m_path = f"output/mismatch-only_{timestamp}.csv"
    if not m_path.endswith('.csv'):
         m_path = os.path.splitext(m_path)[0] + ".csv"

    # Prepare summary content based on format
    is_md = args.format in ['markdown', 'md']
    
    if is_md:
        summary_lines = [
            "# VDN Comparison Summary",
            "",
            "## Comparison Metadata",
            f"- **Source File**: `{os.path.abspath(args.source)}`",
            f"- **Target File**: `{os.path.abspath(args.target)}`",
            f"- **Full Report**: [{os.path.basename(full_report_path)}](file:///{os.path.abspath(full_report_path).replace('\\', '/')})",
            f"- **Summary**: [{os.path.basename(summary_path)}](file:///{os.path.abspath(summary_path).replace('\\', '/')})",
            f"- **Mismatches Only**: [{os.path.basename(m_path)}](file:///{os.path.abspath(m_path).replace('\\', '/')})",
            "",
            "## Comparison Results",
            f"- **Total VINs Analyzed**: {len(final_output)}",
            f"- **VINs with Mismatched SW**: {len(mismatched_sw)}",
            f"- **VINs with Matched SW**: {len(final_output) - len(mismatched_sw)}",
            f"- **VINs with Mismatched VDNs**: {len(mismatched_vdns)}",
            f"- **VINs with Matched VDNs**: {len(final_output) - len(mismatched_vdns)}",
            ""
        ]
    else:
        summary_lines = [
            "COMPARISON METADATA",
            "-"*40,
            f"Source File: {os.path.abspath(args.source)}",
            f"Target File: {os.path.abspath(args.target)}",
            f"Full Report: {os.path.abspath(full_report_path)}",
            f"Summary File: {os.path.abspath(summary_path)}",
            f"Mismatches: {os.path.abspath(m_path)}",
            "\nCOMPARISON RESULTS",
            "="*80,
            f"Total VINs Analyzed: {len(final_output)}",
            f"VINs with Mismatched SW: {len(mismatched_sw)}",
            f"VINs with Matched SW: {len(final_output) - len(mismatched_sw)}",
            f"VINs with Mismatched VDNs: {len(mismatched_vdns)}",
            f"VINs with Matched VDNs: {len(final_output) - len(mismatched_vdns)}",
            ""
        ]
    
    # Print basic stats to console
    for line in (summary_lines[7:] if not is_md else summary_lines[8:]): 
        if line.strip(): cprint(f"[bold cyan]{line.replace('- **', '').replace('**', '')}[/bold cyan]")
    
    if not mismatched_sw.empty:
        matrix_df = pd.crosstab(mismatched_sw['source_sw'], mismatched_sw['target_sw'])
        header_text = "SW VERSION MISMATCH MATRIX (Source vs Target)"
        
        if is_md:
            summary_lines.append(f"## {header_text}")
            summary_lines.append("")
            try:
                summary_lines.append(matrix_df.reset_index().rename(columns={'source_sw': 'Source SW(column) \\ Target SW(row)'}).to_markdown(index=False))
            except ImportError:
                summary_lines.append(matrix_df.to_string())
            summary_lines.append("")
        elif args.format == 'rich' and has_rich:
            from io import StringIO
            summary_lines.append(f"\n{header_text}:")
            # Capture Rich table output to string (high width to prevent truncation)
            capture_console = Console(file=StringIO(), force_terminal=False, width=250)
            matrix_df_reset = matrix_df.reset_index().rename(columns={'source_sw': 'Source\\Target'})
            table = Table(show_header=True, header_style="bold magenta", show_lines=True, box=box.SQUARE)
            for col in matrix_df_reset.columns: table.add_column(str(col), overflow="fold")
            for _, row in matrix_df_reset.iterrows(): table.add_row(*[f"[bold red]{val}[/bold red]" if str(val).isdigit() and int(val) > 0 else str(val) for val in row.values])
            capture_console.print(table)
            summary_lines.append(capture_console.file.getvalue())
        elif args.format == 'csv' or args.format == 'rich': # Fallback to CSV for rich if needed
            summary_lines.append(f"\n{header_text}:")
            summary_lines.append(matrix_df.to_csv())
        else:
            summary_lines.append(f"\n{header_text}:")
            summary_lines.append(matrix_df.to_string())

        # Matrix for Console
        cprint(f"\n[bold magenta]{header_text}:[/bold magenta]")
        if has_rich:
            # Use a console with explicit settings for pager compatibility
            console = Console(force_terminal=True, soft_wrap=False)
            matrix_df_reset = matrix_df.reset_index().rename(columns={'source_sw': 'Source\\Target'})
            # Use ASCII box on Windows for pager compatibility, SQUARE otherwise
            table_box = box.ASCII if os.name == 'nt' and args.pager else box.SQUARE
            table = Table(show_header=True, header_style="bold magenta", show_lines=True, box=table_box)
            for col in matrix_df_reset.columns: table.add_column(str(col), overflow="fold")
            for _, row in matrix_df_reset.iterrows(): table.add_row(*[f"[bold red]{val}[/bold red]" if str(val).isdigit() and int(val) > 0 else str(val) for val in row.values])
            
            if args.pager:
                with console.pager(styles=True):
                    console.print(table)
            else:
                console.print(table)
        else:
            cprint(matrix_df.to_string())

    if not mismatched_vdns.empty:
        # Determine sample size
        if args.samples.lower() == 'all':
            sample_df = mismatched_vdns
            display_count = "ALL"
        else:
            try:
                n = int(args.samples)
                sample_df = mismatched_vdns.head(n)
                display_count = str(len(sample_df))
            except ValueError:
                sample_df = mismatched_vdns.head(10)
                display_count = "10"

        # Select columns for display
        cols = ['vin', 'source_sw', 'target_sw', 'Result', 'sw_match', 'vdn_match', 'Only in Target (missing in Source)', 'Only in Source (missing in Target)']
        sample = sample_df[cols]
        actual_n = len(sample)
        
        sample_header = f"SAMPLE VDN MISMATCHES ({actual_n} entries)"
        
        # Add Header to Summary
        if is_md:
            summary_lines.append(f"## {sample_header}")
            summary_lines.append("")
        else:
            summary_lines.append(f"\n{sample_header}:")
            
        # Print Header to Console
        cprint(f"\n[bold cyan]{sample_header}:[/bold cyan]")
        
        # Sample for Summary File
        if is_md:
            try:
                summary_lines.append(sample.to_markdown(index=False))
            except ImportError:
                summary_lines.append(sample.to_string(index=False))
            summary_lines.append("")
        elif args.format == 'rich' and has_rich:
            from io import StringIO
            # Capture Rich table output for sample (high width to prevent truncation)
            capture_console = Console(file=StringIO(), force_terminal=False, width=250)
            table = Table(show_header=True, header_style="bold cyan", show_lines=True, box=box.SQUARE)
            for col in sample.columns: table.add_column(str(col), overflow="fold")
            for _, row in sample.iterrows(): table.add_row(*[str(val) for val in row.values])
            capture_console.print(table)
            summary_lines.append(capture_console.file.getvalue())
        elif args.format == 'csv' or args.format == 'rich':
            summary_lines.append(sample.to_csv(index=False))
        else:
            summary_lines.append(sample.to_string(index=False))

        # Sample for Console
        if has_rich:
            console = Console(force_terminal=True, soft_wrap=False)
            table_box = box.ASCII if os.name == 'nt' and args.pager else box.SQUARE
            table = Table(show_header=True, header_style="bold cyan", show_lines=True, box=table_box)
            for col in sample.columns: table.add_column(str(col), overflow="fold")
            for _, row in sample.iterrows(): table.add_row(*[str(val) for val in row.values])
            
            if args.pager:
                with console.pager(styles=True):
                    console.print(table)
            else:
                console.print(table)
        else:
            cprint(sample.to_string(index=False))
    else:
        msg = "No VDN Mismatches Found!"
        summary_lines.append(msg)
        cprint(f"[bold green]{msg}[/bold green]")
        
    # Always save full report as CSV (Scalability/Performance)
    def save_dataframe(df, file_path, fmt):
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

    # Enforce CSV for the 220k+ row full output
    save_dataframe(final_output, full_report_path, 'csv')
    
    summary_text = "\n".join(summary_lines)
    
    # Save Summary
    out_dir = os.path.dirname(summary_path)
    if out_dir: os.makedirs(out_dir, exist_ok=True)
    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(summary_text)
        cprint(f"[green]Saved Summary to[/green] [bold white]{summary_path}[/bold white]")
    except PermissionError:
        cprint(f"\n[bold red][ERROR][/bold red] Permission denied: '{summary_path}'.")
        
    # Handle Mismatches logic (CSV only)
    mismatches_only = final_output[final_output['Result'] == 'NOK']
    save_dataframe(mismatches_only, m_path, 'csv')

if __name__ == '__main__':
    main()
