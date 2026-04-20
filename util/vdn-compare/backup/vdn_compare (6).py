import os
import sys
import argparse
import duckdb
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import tkinter as tk
from tkinter import filedialog

__version__ = "1.2.1"

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
    try:
        path_obj = Path(file_path)
        ext = path_obj.suffix.lower()
        if ext in ('.xlsx', '.xls'):
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
    except PermissionError:
        cprint(f"\n[bold red]PERMISSION DENIED:[/bold red] Could not access [bold white]{Path(file_path).name}[/bold white]")
        cprint("[yellow]Please make sure the file is CLOSED in Excel or other programs and try again.[/yellow]")
        sys.exit(1)
    except Exception as e:
        cprint(f"\n[bold red]ERROR LOADING FILE:[/bold red] {e}")
        sys.exit(1)

def save_dataframe(df, file_path, fmt):
    """Helper to save a dataframe with error handling and directory creation."""
    path_obj = Path(file_path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
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

    cprint(f"[bold cyan]VDN Compare[/bold cyan] [dim]v{__version__}[/dim]")
    
    parser = argparse.ArgumentParser(description="Compare Source 1 and Source 2 files.")
    parser.add_argument('-s1', '--source1', help="Source 1 file", default=str(Path("input/DB.csv")))
    parser.add_argument('-s2', '--source2', help="Source 2 file", default=str(Path("input/PIE.csv")))
    parser.add_argument('--format', nargs='+', choices=['csv', 'markdown', 'md', 'rich', 'html'], default=['rich', 'md', 'html'], help="Format(s) for summary output (can select multiple)")
    parser.add_argument('--sort-vin', choices=['none', 'asc', 'desc'], default='asc', help="Sort the output records by VIN (default: none, respects input order)")
    parser.add_argument('--samples', default='10', help="Number of samples to show in summary (integer or 'all', default: 10)")
    parser.add_argument('--pager', action='store_true', help="Use a pager to display long console tables")
    parser.add_argument('--use-default-input', action='store_true', help="Load default DB.csv and PIE.csv from /input without file select GUI dialog")
    parser.add_argument('--compare', nargs='+', default=['sw', 'vdn', 'model'], help='List of columns to compare. Options: sw, vdn, model, vin.')
    parser.add_argument('--normalize-models', nargs='+', default=['EX30,V216', 'EX30 CC,V216-CC'], help='Groups of equivalent models, comma-separated (e.g. "EX30,V216" "PS4,P417")')
    parser.add_argument('--normalize-sw', nargs='+', default=['MY27 J1,27 J1'], help='Groups of equivalent SW versions, comma-separated (e.g. "MY27 J1,27 J1" "1.8.0,1.8.0-hotfix")')
    parser.add_argument('--normalize-custom', default="{}", help='Custom normalization rules in JSON format mapping column names to lists of equivalent groups. Best configured via config.json.')
    parser.add_argument('--skip-filter', default="{}", help='Values to skip/exclude, in JSON format: {"ColumnName": ["Value1", "Value2"]}. Rows matching any of these will be dropped.')
    parser.add_argument('--skip-nodata', action='store_true', help='Skip rows with missing data in any compared column')
    parser.add_argument('--config', help="Path to a JSON file for custom configuration and column mapping", default="config.json")
    
    # 1. Parse known args first to find the config path
    temp_args, _ = parser.parse_known_args()
    
    config_data = {}
    config_path = Path(temp_args.config)
    if config_path.exists():
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            # Apply all top-level keys as default arguments (except the nested column map)
            parser_defaults = {k: v for k, v in config_data.items() if k != 'column_map'}
            if parser_defaults:
                parser.set_defaults(**parser_defaults)
            cprint(f"[cyan]Loaded configuration from: {config_path}[/cyan]")
        except Exception as e:
            cprint(f"[yellow]Warning: Could not parse config file {config_path}: {e}[/yellow]")
            config_data = {}

    # 2. Final parse, allowing CLI arguments to override the JSON defaults
    args = parser.parse_args()

    if isinstance(args.skip_filter, str):
        try:
            skip_filters = json.loads(args.skip_filter)
        except Exception as e:
            cprint(f"[bold red]Error: Could not parse --skip-filter JSON: {e}[/bold red]")
            cprint(f"[yellow]Filter was: {args.skip_filter}[/yellow]")
            skip_filters = {}
    else:
        skip_filters = args.skip_filter if args.skip_filter else {}

    if isinstance(args.normalize_custom, str):
        try:
            custom_norms = json.loads(args.normalize_custom)
        except Exception:
            custom_norms = {}
    else:
        custom_norms = args.normalize_custom if args.normalize_custom else {}

    comp_flags = [c.lower() for c in args.compare]
    compare_sw = 'sw' in comp_flags
    compare_vdn = 'vdn' in comp_flags
    compare_model = 'model' in comp_flags

    # Determine if we should show the file dialog
    # Default behavior: show dialog UNLESS --use-default-input is used 
    # OR the user explicitly provided --source1/--source2 via CLI
    manual_input = Path(args.source1) != Path("input/DB.csv") or Path(args.source2) != Path("input/PIE.csv")
    
    if not args.use_default_input and not manual_input:
        root = tk.Tk()
        root.withdraw()  # Hide the main tkinter window
        root.attributes('-topmost', True) # Bring dialog to front

        cprint("[cyan]Please select Source 1 file...[/cyan]")
        s1_file = filedialog.askopenfilename(
            title="Select Source 1 file",
            filetypes=[("CSV/Excel files", "*.csv *.xlsx *.xls"), ("All files", "*.*")]
        )
        if not s1_file:
            cprint("[bold red]No Source 1 file selected. Exiting.[/bold red]")
            sys.exit(0)
        args.source1 = s1_file

        cprint("[cyan]Please select Source 2 file...[/cyan]")
        s2_file = filedialog.askopenfilename(
            title="Select Source 2 file",
            filetypes=[("CSV/Excel files", "*.csv *.xlsx *.xls"), ("All files", "*.*")]
        )
        if not s2_file:
            cprint("[bold red]No Source 2 file selected. Exiting.[/bold red]")
            sys.exit(0)
        args.source2 = s2_file
        
        root.destroy()

    # Start the timer here to exclude file dialog interaction time
    start_time = time.time()
    cprint(f"[cyan]Loading Source 1:[/cyan] [bold white]{args.source1}[/bold white]")
    df_s1 = load_file(args.source1)
    
    cprint(f"[cyan]Loading Source 2:[/cyan] [bold white]{args.source2}[/bold white]")
    df_s2 = load_file(args.source2)

    # NOTE: The values (right-hand side) are RESERVED INTERNAL HEADERS.
    # To support new columns, map your name (left) to one of these 4 reserved names.
    common_map = {
        'vin': 'VIN',
        'DB_SW': 'CONSUMER_SW_VERSION',
        'DB_targetVdns': 'VDN_LIST',
        'model': 'MODEL',
        "region": "REGION"
    }

    # Load custom mapping (Merge all sources into a unified pool for order-independence)
    shared_map = config_data.get('column_map', {})
    s1_map = config_data.get('s1_map', config_data.get('source_map', {}))
    s2_map = config_data.get('s2_map', config_data.get('target_map', {}))
    
    common_map.update(shared_map)
    common_map.update(s1_map)
    common_map.update(s2_map)
    
    # We apply the same map to both: order protection
    df_s1.rename(columns=common_map, inplace=True)
    df_s2.rename(columns=common_map, inplace=True)

    # 1. Identify Comparison Targets
    # All columns effectively renamed to something that isn't VIN are potential targets
    s_cols = [c for c in set(common_map.values()) if c != 'VIN']
    
    # Check what actually exists in both dataframes
    all_existing = [c for c in s_cols if c in df_s1.columns and c in df_s2.columns]
    
    # Final filter: Respect the --compare flag (comp_flags)
    # Map command keywords (sw, model, vdn) to internal column names
    reserved_map = {'sw': 'CONSUMER_SW_VERSION', 'vdn': 'VDN_LIST', 'model': 'MODEL'}
    
    requested_names = []
    for flag in comp_flags:
        flag_l = flag.lower()
        if flag_l in reserved_map:
            requested_names.append(reserved_map[flag_l])
        else:
            # Check if flags matches an internal column name directly 
            # OR matches a Source 1 header that was mapped to that column
            target_name = None
            if flag_l.upper() in [c.upper() for c in s_cols]:
                 target_name = next(c for c in s_cols if c.upper() == flag_l.upper())
            else:
                 target_name = next((v for k, v in common_map.items() if k.lower() == flag_l), None)
            
            if target_name:
                requested_names.append(target_name)
    
    # Ensure existing_targets follows the specific order requested by the user/default flags
    existing_targets = [c for c in requested_names if c in all_existing]
    
    # Identify specials and handle auto-downgrade
    compare_sw = 'CONSUMER_SW_VERSION' in existing_targets
    compare_model = 'MODEL' in existing_targets
    compare_vdn = 'VDN_LIST' in existing_targets
    
    if not compare_vdn and 'vdn' in comp_flags:
        cprint("[yellow]Auto-disabling VDN comparison: VDN_LIST header missing.[/yellow]")
    
    if 'VIN' not in df_s1.columns or 'VIN' not in df_s2.columns:
        cprint(f"[bold red]CRITICAL ERROR: 'VIN' column not found in one or both files.[/bold red]")
        sys.exit(1)

    # 2. Trim whitespaces and quotes, and normalize null-like values
    def preprocess_df(df, label):
        """Unified cleanup, filtering, and normalization for comparison targets."""
        # 2a. Aggressive column/value cleanup
        df.columns = [str(c).strip().replace('"', '') for c in df.columns]
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.replace(r'^"|"$', '', regex=True)
            df[col] = df[col].replace({'nan': None, 'NaN': None, 'None': None, '': None})
        
        # 2b. Apply Skip Filters (Exclude certain rows early)
        if skip_filters:
            for f_col, f_vals in skip_filters.items():
                # Case-insensitive column matching
                target_col = next((c for c in df.columns if c.lower() == f_col.lower()), None)
                
                if target_col:
                    if not isinstance(f_vals, list): f_vals = [f_vals]
                    # Case-insensitive value matching
                    f_vals_set = set(str(v).strip().upper() for v in f_vals)
                    mask = df[target_col].astype(str).str.strip().str.upper().isin(f_vals_set)
                    skip_count = mask.sum()
                    if skip_count > 0:
                        df = df[~mask].copy()
                        cprint(f"[yellow]Filtered {skip_count} rows from {label} where '{target_col}' matched {f_vals_set}[/yellow]")
        
        # 2c. Custom business logic normalization for Model comparison
        if compare_model and 'MODEL' in df.columns:
            df['MODEL_NORM'] = df['MODEL']
            df['MODEL_DISPLAY'] = df['MODEL']
            for group in args.normalize_models:
                models = [m.strip() for m in group.split(',')]
                if len(models) > 1:
                    primary = models[0]
                    for alias in models[1:]:
                        df.loc[df['MODEL'] == alias, 'MODEL_NORM'] = primary
                        df.loc[df['MODEL'] == alias, 'MODEL_DISPLAY'] = f"{primary}({alias})"

        # 2d. Custom business logic normalization for SW comparison
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

        # 2e. Generic Custom Normalization for any extra columns specified in config
        if custom_norms:
            for norm_col, groups in custom_norms.items():
                if norm_col in df.columns:
                    col_norm = f"{norm_col}_NORM"
                    col_disp = f"{norm_col}_DISPLAY"
                    df[col_norm] = df[norm_col]
                    df[col_disp] = df[norm_col]
                    if isinstance(groups, str): groups = [groups]
                    for group in groups:
                        items = [str(x).strip() for x in group.split(',')]
                        if len(items) > 1:
                            primary = items[0]
                            for alias in items[1:]:
                                df.loc[df[norm_col] == alias, col_norm] = primary
                                df.loc[df[norm_col] == alias, col_disp] = f"{primary}({alias})"
        return df

    df_s1 = preprocess_df(df_s1, "Source 1")
    df_s2 = preprocess_df(df_s2, "Source 2")


    # 3. Parse VDN_LIST smartly
    def parse_vdn(val):
        if pd.isna(val) or str(val).strip() in ('nan', ''): return []
        val_str = str(val).strip()
        
        # Check if it's a JSON array
        if val_str.startswith('[') and val_str.endswith(']'):
            try:
                parsed = json.loads(val_str.replace("'", '"'))
                if isinstance(parsed, list):
                    result = sorted(str(v).strip() for v in parsed if str(v).strip())
                    return result if result else []
            except Exception:
                pass
                
        # If not, assume it's concatenated 4-char chunks
        chunks = [val_str[i:i+4] for i in range(0, len(val_str), 4)]
        result = sorted(c for c in chunks if c.strip())
        return result if result else []

    for df in (df_s1, df_s2):
        if compare_vdn and 'VDN_LIST' in df.columns:
            if has_tqdm:
                cprint(f"[cyan]Parsing VDNs for {df.columns[0]}...[/cyan]")
                df['VDN_LIST_CLEAN'] = df['VDN_LIST'].progress_apply(lambda x: json.dumps(parse_vdn(x)))
            else:
                df['VDN_LIST_CLEAN'] = df['VDN_LIST'].apply(lambda x: json.dumps(parse_vdn(x)))
            # Free up memory containing original heavy string values early
            df.drop(columns=['VDN_LIST'], inplace=True)

    # 3.2. EXTRA FILTER: Skip rows with missing/empty data if requested
    skipped_nodata = {'s1': 0, 's2': 0}
    if args.skip_nodata:
        for label, df_ref in [('Source 1', df_s1), ('Source 2', df_s2)]:
            nodata_mask = pd.Series(False, index=df_ref.index)
            for col in existing_targets:
                # Resolve the column to check (original or cleaned)
                target_col = col if col in df_ref.columns else ('VDN_LIST_CLEAN' if col == 'VDN_LIST' else None)
                if not target_col: continue
                
                # Rows where data is null OR empty VDN list
                mask = df_ref[target_col].isna() | (df_ref[target_col] == '[]')
                nodata_mask |= mask
            
            skip_count = nodata_mask.sum()
            if skip_count > 0:
                if label == 'Source 1': 
                    df_s1 = df_s1[~nodata_mask].copy()
                    skipped_nodata['s1'] = skip_count
                else: 
                    df_s2 = df_s2[~nodata_mask].copy()
                    skipped_nodata['s2'] = skip_count
                cprint(f"[yellow]Skipped {skip_count} rows from {label} due to missing data (--skip-nodata)[/yellow]")

    # 3.5. AUDITING STEP: Data Integrity & Business Logic Checks
    def find_vdn_prefix_conflicts(vdn_json):
        if not vdn_json or vdn_json == '[]': return None
        try:
            vdns = json.loads(vdn_json)
            prefixes = {}
            for v in vdns:
                p = str(v)[:2].upper()
                prefixes.setdefault(p, []).append(v)
            conflicts = {p: v_list for p, v_list in prefixes.items() if len(v_list) > 1}
            if conflicts:
                return ", ".join(f"{p}({'/'.join(v)})" for p, v in conflicts.items())
        except: pass
        return None

    audit_results = {'s1': {}, 's2': {}}
    for label, df in [('s1', df_s1), ('s2', df_s2)]:
        # Part 1: Duplicate VINs
        dup_mask = df.duplicated(subset=['VIN'], keep=False)
        dup_vins = sorted(df[dup_mask]['VIN'].unique().tolist())
        
        # Part 2: VDN Prefix Conflicts
        prefix_conflicts = []
        if 'VDN_LIST_CLEAN' in df.columns:
            # We use a temporary series to avoid modifying the dataframe for this audit
            conflict_series = df['VDN_LIST_CLEAN'].apply(find_vdn_prefix_conflicts)
            conflict_mask = conflict_series.notna()
            for vin, conflict_desc in zip(df[conflict_mask]['VIN'], conflict_series[conflict_mask]):
                prefix_conflicts.append(f"{vin} [Conflicts: {conflict_desc}]")
        
        # Part 3: Empty Data Rows
        empty_data = {}        # Column -> [VINs]
        vin_empty_map = {}     # VIN -> [Column Labels]
        empty_mask_combined = pd.Series(False, index=df.index)
        
        for col in existing_targets:
            # Check if column exists (handling the case where VDN_LIST was renamed/cleaned)
            audit_col = col if col in df.columns else ('VDN_LIST_CLEAN' if col == 'VDN_LIST' and 'VDN_LIST_CLEAN' in df.columns else None)
            if not audit_col: continue
            
            mask = df[audit_col].isna() | (df[audit_col] == '[]')
            if mask.any():
                v_list = sorted(df[mask]['VIN'].unique().tolist())
                # Identify which data_label to show (e.g. SW instead of CONSUMER_SW_VERSION)
                data_label = col
                if col == 'CONSUMER_SW_VERSION': data_label = 'SW'
                elif col == 'VDN_LIST': data_label = 'VDN'
                elif col == 'MODEL': data_label = 'MODEL'
                
                empty_data[data_label] = v_list
                empty_mask_combined |= mask
                
                for v in v_list:
                    vin_empty_map.setdefault(v, []).append(data_label)
        
        u_empty_vins = df[empty_mask_combined]['VIN'].nunique()
        
        audit_results[label] = {
            'dup_vins': dup_vins,
            'extra_rows': len(df) - df['VIN'].nunique(),
            'prefix_conflicts': sorted(prefix_conflicts),
            'empty_data': empty_data,
            'vin_empty_map': vin_empty_map,
            'u_empty_vins': u_empty_vins
        }

    # Console Warnings (Immediate)
    for label in ['s1', 's2']:
        res = audit_results[label]
        l_disp = "Source 1" if label == 's1' else "Source 2"
        if res['dup_vins']:
            cprint(f"[bold yellow]DATA AUDIT WARNING: {len(res['dup_vins'])} duplicate VINs found in {l_disp} file (affecting {res['extra_rows']} redundant rows).[/bold yellow]")
        if res['prefix_conflicts']:
            cprint(f"[bold yellow]DATA AUDIT WARNING: {len(res['prefix_conflicts'])} VINs with VDN Prefix Conflicts found in {l_disp} file.[/bold yellow]")
        if res['u_empty_vins']:
            cprint(f"[bold yellow]DATA AUDIT WARNING: {res['u_empty_vins']} VINs with missing/empty data in compared columns found in {l_disp} file.[/bold yellow]")
    con = duckdb.connect()
    con.register('s1_db', df_s1)
    con.register('s2_db', df_s2)

    sort_clause = f"ORDER BY vin {args.sort_vin.upper()}" if args.sort_vin != 'none' else ""
    
    s_selects = ["VIN as vin"]
    t_selects = ["VIN as vin"]
    for col in existing_targets:
        if col == 'CONSUMER_SW_VERSION':
            s_selects.extend(["SW_NORM as s1_sw", "SW_DISPLAY as s1_sw_disp"])
            t_selects.extend(["SW_NORM as s2_sw", "SW_DISPLAY as s2_sw_disp"])
        elif col == 'MODEL':
            s_selects.extend(["MODEL_NORM as s1_model", "MODEL_DISPLAY as s1_model_disp"])
            t_selects.extend(["MODEL_NORM as s2_model", "MODEL_DISPLAY as s2_model_disp"])
        elif col == 'VDN_LIST':
            s_selects.append("VDN_LIST_CLEAN as s1_vdns_json")
            t_selects.append("VDN_LIST_CLEAN as s2_vdns_json")
        else:
            # Generic Column
            if custom_norms and col in custom_norms:
                s_selects.extend([f"{col}_NORM as s1_{col}_norm", f"{col}_DISPLAY as s1_{col}_disp"])
                t_selects.extend([f"{col}_NORM as s2_{col}_norm", f"{col}_DISPLAY as s2_{col}_disp"])
            else:
                s_selects.append(f"{col} as s1_{col}")
                t_selects.append(f"{col} as s2_{col}")
        
    s_selects_str = ",\n            ".join(s_selects)
    t_selects_str = ",\n            ".join(t_selects)
    
    joined_selects = ["COALESCE(s.vin, t.vin) as vin"]
    joined_selects.append("CASE WHEN s.vin IS NOT NULL THEN 1 ELSE 0 END as VIN_in_s1")
    joined_selects.append("CASE WHEN t.vin IS NOT NULL THEN 1 ELSE 0 END as VIN_in_s2")
    
    for col in existing_targets:
        if col == 'CONSUMER_SW_VERSION':
            joined_selects.append("CASE WHEN s.vin IS NULL THEN 'N/A' ELSE COALESCE(s.s1_sw_disp, 'NO DATA') END as s1_sw_display")
            joined_selects.append("CASE WHEN t.vin IS NULL THEN 'N/A' ELSE COALESCE(t.s2_sw_disp, 'NO DATA') END as s2_sw_display")
            joined_selects.append("CASE WHEN s.vin IS NULL OR t.vin IS NULL THEN 'MISMATCH' WHEN s.s1_sw IS NOT DISTINCT FROM t.s2_sw THEN 'MATCH' ELSE 'MISMATCH' END as sw_match")
        elif col == 'MODEL':
            joined_selects.append("CASE WHEN s.vin IS NULL THEN 'N/A' ELSE COALESCE(s.s1_model_disp, 'NO DATA') END as s1_model_display")
            joined_selects.append("CASE WHEN t.vin IS NULL THEN 'N/A' ELSE COALESCE(t.s2_model_disp, 'NO DATA') END as s2_model_display")
            joined_selects.append("CASE WHEN s.vin IS NULL OR t.vin IS NULL THEN 'MISMATCH' WHEN s.s1_model IS NOT DISTINCT FROM t.s2_model THEN 'MATCH' ELSE 'MISMATCH' END as model_match")
        elif col == 'VDN_LIST':
            joined_selects.append("CASE WHEN s.vin IS NULL OR t.vin IS NULL THEN 'MISMATCH' WHEN s.s1_vdns_json IS NOT DISTINCT FROM t.s2_vdns_json THEN 'MATCH' ELSE 'MISMATCH' END as vdn_match")
            joined_selects.append("s.s1_vdns_json as s1_json")
            joined_selects.append("t.s2_vdns_json as s2_json")
        else:
            # Generic logic
            m_col = f"{col.lower()}_match"
            if custom_norms and col in custom_norms:
                joined_selects.append(f"CASE WHEN s.vin IS NULL THEN 'N/A' ELSE COALESCE(s.s1_{col}_disp, 'NO DATA') END as s1_{col.lower()}_display")
                joined_selects.append(f"CASE WHEN t.vin IS NULL THEN 'N/A' ELSE COALESCE(t.s2_{col}_disp, 'NO DATA') END as s2_{col.lower()}_display")
                joined_selects.append(f"CASE WHEN s.vin IS NULL OR t.vin IS NULL THEN 'MISMATCH' WHEN s.s1_{col}_norm IS NOT DISTINCT FROM t.s2_{col}_norm THEN 'MATCH' ELSE 'MISMATCH' END as {m_col}")
            else:
                joined_selects.append(f"CASE WHEN s.vin IS NULL THEN 'N/A' ELSE COALESCE(CAST(s.s1_{col} as VARCHAR), 'NO DATA') END as s1_{col.lower()}_display")
                joined_selects.append(f"CASE WHEN t.vin IS NULL THEN 'N/A' ELSE COALESCE(CAST(t.s2_{col} as VARCHAR), 'NO DATA') END as s2_{col.lower()}_display")
                joined_selects.append(f"CASE WHEN s.vin IS NULL OR t.vin IS NULL THEN 'MISMATCH' WHEN s.s1_{col} IS NOT DISTINCT FROM t.s2_{col} THEN 'MATCH' ELSE 'MISMATCH' END as {m_col}")
        
    joined_selects_str = ",\n            ".join(joined_selects)
    
    final_selects = ["vin", "VIN_in_s1", "VIN_in_s2"]
    mismatch_conditions = []
    
    for col in existing_targets:
        if col == 'CONSUMER_SW_VERSION':
            final_selects.extend(["s1_sw_display as s1_sw", "s2_sw_display as s2_sw", "sw_match"])
            mismatch_conditions.append("sw_match = 'MISMATCH'")
        elif col == 'MODEL':
            final_selects.extend(["s1_model_display as s1_model", "s2_model_display as s2_model", "model_match"])
            mismatch_conditions.append("model_match = 'MISMATCH'")
        elif col == 'VDN_LIST':
            final_selects.extend(["vdn_match", "s1_json as s1_vdns_json", "s2_json as s2_vdns_json"])
            mismatch_conditions.append("vdn_match = 'MISMATCH'")
        else:
            m_col = f"{col.lower()}_match"
            final_selects.extend([f"s1_{col.lower()}_display as s1_{col.lower()}", f"s2_{col.lower()}_display as s2_{col.lower()}", m_col])
            mismatch_conditions.append(f"{m_col} = 'MISMATCH'")

    if mismatch_conditions:
        result_cond = " OR ".join(mismatch_conditions)
        final_selects.append(f"CASE WHEN {result_cond} THEN 'NOK' ELSE 'OK' END as Result")
    else:
        final_selects.append("'OK' as Result")
        
    final_selects_str = ",\n        ".join(final_selects)

    compare_query = f"""
    WITH s1_data AS (
        SELECT {s_selects_str}
        FROM s1_db
        WHERE VIN IS NOT NULL AND VIN != 'nan'
    ),
    s2_data AS (
        SELECT {t_selects_str}
        FROM s2_db
        WHERE VIN IS NOT NULL AND VIN != 'nan'
    ),
    joined AS (
        SELECT
            {joined_selects_str}
        FROM s1_data s
        FULL OUTER JOIN s2_data t ON s.vin = t.vin
    )
    SELECT
        {final_selects_str}
    FROM joined
    {sort_clause}
    """
    
    result_df = con.execute(compare_query).df()
    
    import gc
    # Free up heavy dataframe hashes immediately
    del df_s1
    del df_s2
    con.close()
    gc.collect()

    # Advanced logic for Python-side extraction of VDN differences (Optimized)
    if compare_vdn:
        if has_tqdm:
            cprint("[cyan]Extracting VDN differences...[/cyan]")
            
        vdn_diff_pairs = []
        def compute_diff(row):
            s_vdns_str = row.get('s1_vdns_json')
            t_vdns_str = row.get('s2_vdns_json')
            
            s_vdns = set(json.loads(s_vdns_str)) if pd.notna(s_vdns_str) and s_vdns_str else set()
            t_vdns = set(json.loads(t_vdns_str)) if pd.notna(t_vdns_str) and t_vdns_str else set()
            
            only_in_t = t_vdns - s_vdns
            only_in_s = s_vdns - t_vdns
            
            added = ", ".join(sorted(only_in_t)) if only_in_t else ""
            removed = ", ".join(sorted(only_in_s)) if only_in_s else ""
            
            # Detailed Tally logic (only for true discrepancies where VIN exists in both)
            if row['vdn_match'] == 'MISMATCH' and row['VIN_in_s1'] == 1 and row['VIN_in_s2'] == 1:
                s_groups = {}
                for v in only_in_s: s_groups.setdefault(v[:2], []).append(v)
                t_groups = {}
                for v in only_in_t: t_groups.setdefault(v[:2], []).append(v)
                
                all_prefixes = set(s_groups.keys()) | set(t_groups.keys())
                for pref in sorted(all_prefixes):
                    s_list = sorted(s_groups.get(pref, []))
                    t_list = sorted(t_groups.get(pref, []))
                    for i in range(max(len(s_list), len(t_list))):
                        sv = s_list[i] if i < len(s_list) else "No Match"
                        tv = t_list[i] if i < len(t_list) else "No Match"
                        vdn_diff_pairs.append((row['vin'], sv, tv))

            return added, removed

        if has_tqdm:
            tqdm.pandas(desc="Computing Diffs")
            diffs = result_df.progress_apply(compute_diff, axis=1)
        else:
            diffs = result_df.apply(compute_diff, axis=1)

        result_df['Only in S1'] = diffs.map(lambda x: x[1])
        result_df['Only in S2'] = diffs.map(lambda x: x[0])
        
        # Create VDN Tally DataFrame
        vdn_tally_df = pd.DataFrame(vdn_diff_pairs, columns=['VIN', 'VDN in S1', 'VDN in S2'])
        if not vdn_tally_df.empty:
            # Aggregate "No Match" cases to show unique VIN counts
            none_in_s = vdn_tally_df[vdn_tally_df['VDN in S1'] == 'No Match']
            none_in_t = vdn_tally_df[vdn_tally_df['VDN in S2'] == 'No Match']
            both_codes = vdn_tally_df[(vdn_tally_df['VDN in S1'] != 'No Match') & (vdn_tally_df['VDN in S2'] != 'No Match')]
            
            summaries = []
            if not none_in_s.empty:
                summaries.append({'VDN in S1': 'No Match', 'VDN in S2': '(Various VDNs)', 'Count': none_in_s['VIN'].nunique()})
            if not none_in_t.empty:
                summaries.append({'VDN in S1': '(Various VDNs)', 'VDN in S2': 'No Match', 'Count': none_in_t['VIN'].nunique()})
                
            if not both_codes.empty:
                # Count unique VINs for each specific pairwise mismatch pattern
                true_tally = both_codes.groupby(['VDN in S1', 'VDN in S2'])['VIN'].nunique().reset_index(name='Count')
                vdn_tally_df = pd.concat([pd.DataFrame(summaries), true_tally.sort_values('Count', ascending=False)], ignore_index=True)
            else:
                vdn_tally_df = pd.DataFrame(summaries)
        
        final_output = result_df.drop(columns=['s1_vdns_json', 's2_json' if 's2_json' in result_df.columns else 's2_vdns_json'], errors='ignore')
    else:
        final_output = result_df
    
    # Overall differences summary
    # 1. Identify VINs completely missing from one side
    missing_in_s1 = final_output[final_output['VIN_in_s1'] == 0]
    missing_in_s2 = final_output[final_output['VIN_in_s2'] == 0]
    
    # 2. Identify "True" mismatches for all compared columns
    column_meta = {
        'CONSUMER_SW_VERSION': {'label': 'SW', 'match': 'sw_match', 's': 's1_sw', 't': 's2_sw'},
        'MODEL': {'label': 'Model', 'match': 'model_match', 's': 's1_model', 't': 's2_model'},
        'VDN_LIST': {'label': 'VDN', 'match': 'vdn_match', 's': 'Only in S1', 't': 'Only in S2'}
    }
    
    mismatched_data = {}
    for col in existing_targets:
        meta = column_meta.get(col, {
            'label': col, 
            'match': f"{col.lower()}_match", 
            's': f"s1_{col.lower()}", 
            't': f"s2_{col.lower()}"
        })
        mismatched_df = final_output[
            (final_output[meta['match']] == 'MISMATCH') & 
            (final_output['VIN_in_s1'] == 1) & 
            (final_output['VIN_in_s2'] == 1)
        ]
        mismatched_data[col] = {
            'df': mismatched_df,
            'label': meta['label'],
            'u_count': mismatched_df['vin'].nunique(),
            'match_col': meta['match'],
            's_col': meta['s'],
            't_col': meta['t']
        }

    # Extract specialized dataframes for backward compatibility with complex reporting sections
    mismatched_sw = mismatched_data.get('CONSUMER_SW_VERSION', {}).get('df', pd.DataFrame())
    mismatched_vdns = mismatched_data.get('VDN_LIST', {}).get('df', pd.DataFrame())

    # Calculate UNIQUE counts for reporting
    u_total = final_output['vin'].nunique()
    u_only_s2 = missing_in_s1['vin'].nunique()
    u_only_s1 = missing_in_s2['vin'].nunique()
    u_s1 = u_total - u_only_s2
    u_s2 = u_total - u_only_s1
    for col in existing_targets:
        mismatched_data[col]['u_count'] = mismatched_data[col]['df']['vin'].nunique()
    
    # Prepare output paths
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    
    full_report_path = output_dir / f"full_comparison_results_{timestamp}.csv"
    m_path = output_dir / f"mismatch-only_{timestamp}.csv"
    
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
        f"Source 1 File: {Path(args.source1).name}",
        f"Source 2 File: {Path(args.source2).name}",
        f"Full Report: {full_report_path.name}",
        f"Mismatches: {m_path.name}",
        f"Source 1 Dups: {len(audit_results['s1']['dup_vins'])} VINs",
        f"Source 2 Dups: {len(audit_results['s2']['dup_vins'])} VINs",
    ]
    if compare_vdn:
        summary_lines_console.extend([
            f"Source 1 VDN Prefix Conflicts: {len(audit_results['s1']['prefix_conflicts'])} VINs",
            f"Source 2 VDN Prefix Conflicts: {len(audit_results['s2']['prefix_conflicts'])} VINs",
        ])
    summary_lines_console.extend([
        "\nCOMPARISON RESULTS",
        "="*80,
        f"Total Unique VINs Analyzed: {u_total} (Source 1: {u_s1}, Source 2: {u_s2})",
        f"Unique VINs found only in Source 1: {u_only_s1}",
        f"Unique VINs found only in Source 2: {u_only_s2}"
    ])
    for col in existing_targets:
        md = mismatched_data[col]
        label = md['label']
        u_count = md['u_count']
        summary_lines_console.append(f"Unique VINs with {label} Discrepancy (VIN exists in both): {u_count}")
    
    summary_lines_console.append("")
    
    # ---------------------------------------------------------
    # AUDIT DETAILS (Console)
    # ---------------------------------------------------------
    has_audit_errors = any(audit_results[l]['dup_vins'] or audit_results[l]['prefix_conflicts'] or audit_results[l]['empty_data'] for l in ['s1', 's2'])
    if has_audit_errors:
        summary_lines_console.append("")
        summary_lines_console.append("-" * 40)
        summary_lines_console.append("AUDIT DETAILS & DATA INTEGRITY")
        summary_lines_console.append("-" * 40)
        for label in ['s1', 's2']:
            res = audit_results[label]
            l_disp = "Source 1" if label == 's1' else "Source 2"
            if res['dup_vins']:
                v_list = res['dup_vins']
                summary_lines_console.append(f"Duplicate VINs in {l_disp}:")
                for v in v_list[:20]:
                    summary_lines_console.append(f"  - {v}")
                if len(v_list) > 20:
                    summary_lines_console.append(f"  ... (+{len(v_list)-20} more in report file)")
            
            if res['empty_data']:
                breakdown = ", ".join(f"{c}({len(v)})" for c, v in res['empty_data'].items())
                summary_lines_console.append(f"Incomplete Data in {l_disp} (Breakdown: {breakdown}):")
            if res['prefix_conflicts']:
                v_list = res['prefix_conflicts']
                summary_lines_console.append(f"VDN Prefix Conflicts in {l_disp}:")
                for v in v_list[:10]:
                    summary_lines_console.append(f"  - {v}")
                if len(v_list) > 10:
                    summary_lines_console.append(f"  ... (+{len(v_list)-10} more in report file)")
        summary_lines_console.append("")
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
        header_text = "SW VERSION MISMATCH MATRIX (Source 1 vs Source 2)"
        cprint(f"\n[bold magenta]{header_text}:[/bold magenta]")
        matrix_df = pd.crosstab(mismatched_sw['s1_sw'], mismatched_sw['s2_sw'], margins=True, margins_name='TOTAL')
        
        # Matrix View
        if has_rich:
            console = Console(force_terminal=True)
            matrix_df_reset = matrix_df.reset_index().rename(columns={'s1_sw': 'Source 1 SW(row)\\Source 2 SW(col)'})
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

        # Detailed Tally View
        sw_counts = mismatched_sw.groupby(['s1_sw', 's2_sw']).size().reset_index(name='Count')
        sw_counts = sw_counts.sort_values('Count', ascending=False)
        render_console_table(sw_counts, "DETAILED SW MISMATCH TALLY", header_style="bold magenta")

    # 1.2 Other Column Tallies (Generic Handling)
    for col in existing_targets:
        if col in ['CONSUMER_SW_VERSION', 'VDN_LIST']: continue
        md = mismatched_data[col]
        if md['df'].empty: continue
        
        t_df = md['df'].groupby([md['s_col'], md['t_col']]).size().reset_index(name='Count')
        t_df = t_df.sort_values('Count', ascending=False)
        render_console_table(t_df, f"DETAILED {md['label'].upper()} MISMATCH TALLY", header_style="bold magenta")

    # 1.3. VDN Mismatch Detailed Tally (Console)
    if compare_vdn and not vdn_tally_df.empty:
        render_console_table(vdn_tally_df, "DETAILED VDN MISMATCHES (Pairwise)", header_style="bold cyan")


    # 2. Sample Data for Console (Mismatches and Missing VINs)
    sample_limit = None if args.samples.lower() == 'all' else int(args.samples) if args.samples.isdigit() else 10
    
    # 2. Sample Data for Console (Dynamic Loop)
    for col in existing_targets:
        if col == 'VDN_LIST': continue # Handled specially below
        md = mismatched_data[col]
        if md['df'].empty: continue
        
        df_sample = md['df'].head(sample_limit) if sample_limit else md['df']
        cols = ['vin', md['s_col'], md['t_col'], md['match_col']]
        title = f"{'SAMPLES: ' if sample_limit else ''}{md['label']} MISMATCHES ({len(df_sample)} shown of {md['u_count']} unique VINs)"
        render_console_table(df_sample[cols], title, header_style="bold magenta")

    # Missing in Source 1 Samples
    if not missing_in_s1.empty:
        df_ms = missing_in_s1.head(sample_limit) if sample_limit else missing_in_s1
        cols = ['vin']
        if compare_model: cols.append('s2_model')
        if compare_sw: cols.append('s2_sw')
        title_ms = f"{'SAMPLES: ' if sample_limit else ''}VINs FOUND ONLY IN SOURCE 2 ({len(df_ms)} entries shown of {u_only_s2} unique VINs)"
        render_console_table(df_ms[cols], title_ms, header_style="bold yellow")
        
    # Missing in Source 2 Samples
    if not missing_in_s2.empty:
        df_mt = missing_in_s2.head(sample_limit) if sample_limit else missing_in_s2
        cols = ['vin']
        if compare_model: cols.append('s1_model')
        if compare_sw: cols.append('s1_sw')
        title_mt = f"{'SAMPLES: ' if sample_limit else ''}VINs FOUND ONLY IN SOURCE 1 ({len(df_mt)} entries shown of {u_only_s1} unique VINs)"
        render_console_table(df_mt[cols], title_mt, header_style="bold yellow")

    # (Already handled above)

    # VDN MISMATCHES (Detailed list of differences for records with VDN errors)
    if compare_vdn and not mismatched_vdns.empty:
        sample_df = mismatched_vdns.head(sample_limit) if sample_limit else mismatched_vdns
        cols = ['vin', 'vdn_match', 'Only in S1', 'Only in S2']
            
        title_vdn = f"{'SAMPLES: ' if sample_limit else ''}VDN MISMATCHES ({len(sample_df)} entries shown of {mismatched_data['VDN_LIST']['u_count']} unique VINs)"
        render_console_table(sample_df[cols], title_vdn, header_style="bold cyan")

    # ---------------------------------------------------------
    # PART B: GENERATE FILES
    # ---------------------------------------------------------
    for fmt in req_formats:
        is_md = fmt in ['markdown', 'md']
        is_html = fmt == 'html'
        
        sum_ext = ".html" if is_html else (".md" if is_md else (".txt" if fmt == 'rich' else ".csv"))
        curr_summary_path = output_dir / f"summary_{timestamp}{sum_ext}"
        
        if is_md or is_html:
            title_prefix = "# " if is_md else "<h1>"
            title_suffix = "" if is_md else "</h1>"
            meta_prefix = "## " if is_md else "<h2>"
            res_prefix = "## " if is_md else "<h2>"
            
            summary_lines = [
                f"{title_prefix}Comparison Report{title_suffix}"
            ]
            
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
                    
                    # Truncation logic
                    for col in md_s.columns:
                        md_s[col] = md_s[col].apply(lambda x: str(x)[:37] + "..." if len(str(x)) > 40 else str(x))
                    
                    red_indicators = ['MISMATCH', 'NOK']
                    md_s = md_s.map(lambda x: f'<span style="color:red" class="mismatch">{x}</span>' if str(x).upper() in red_indicators else str(x))
                    if is_md:
                        summary_lines.append(md_s.to_markdown(index=False))
                    else:
                        summary_lines.append(f"<details><summary>View Data Samples ({len(df_sample)} shown)</summary>")
                        summary_lines.append(md_s.to_html(index=False, escape=False))
                        summary_lines.append("</details>")
                elif fmt == 'rich' and has_rich:
                    from io import StringIO
                    capture_con = Console(file=StringIO(), force_terminal=False, width=250)
                    tbl = Table(show_header=True, header_style=style_color, show_lines=True, box=box.ASCII)
                    for idx, c_name in enumerate(df_sample.columns):
                        tbl.add_column(str(c_name), overflow="fold", style="bold white" if idx == 0 else None)
                    for _, r in df_sample.iterrows():
                        display_vals = [str(v)[:37] + "..." if len(str(v)) > 40 else str(v) for v in r.values]
                        styled_vals = [f"[bold red]{val}[/bold red]" if val.upper() in ['MISMATCH', 'NOK'] else val for val in display_vals]
                        tbl.add_row(*styled_vals)
                    capture_con.print(tbl)
                    summary_lines.append(f"\n{title}:")
                    summary_lines.append(capture_con.file.getvalue())
                elif fmt == 'csv':
                    summary_lines.append(f"\n{title}:\n" + df_sample.to_csv(index=False))
                else:
                    summary_lines.append(f"\n{title}:\n" + df_sample.to_string(index=False))
            # --- TABLE OF CONTENTS ---
            sample_prefix = "" if sample_limit is None else "Samples: "
            toc_lines = []
            if is_md:
                toc_lines.append("## Table of Contents")
                toc_lines.append("- [Comparison Metadata](#comparison-metadata)")
                if has_audit_errors:
                    toc_lines.append("- [Audit Details](#audit-details)")
                toc_lines.append("- [Comparison Results](#comparison-results)")
                
                # Dynamic Mismatch Sections
                for col in existing_targets:
                    md = mismatched_data[col]
                    if md['df'].empty: continue
                    anchor_base = md['label'].lower().replace(' ', '-')
                    if col == 'CONSUMER_SW_VERSION':
                        toc_lines.append(f"- [{md['label']} Mismatch Matrix](#sw-mismatch-matrix)")
                    
                    toc_lines.append(f"- [Detailed {md['label']} Mismatch Tally](#tally-{anchor_base})")
                    toc_lines.append(f"- [{sample_prefix}{md['label']} Mismatches](#samples-{anchor_base})")
                
                if not missing_in_s1.empty: toc_lines.append(f"- [{sample_prefix}VINs Found Only in Source 2](#samples-missing-in-source1)")
                if not missing_in_s2.empty: toc_lines.append(f"- [{sample_prefix}VINs Found Only in Source 1](#samples-missing-in-source2)")
                toc_lines.append("")
            elif is_html:
                summary_lines.append("<div class='toc'><h2>Table of Contents</h2><ul>")
                summary_lines.append("<li><a href='#comparison-metadata'>Comparison Metadata</a></li>")
                if has_audit_errors:
                    summary_lines.append("<li><a href='#audit-details'>Audit Details</a></li>")
                summary_lines.append("<li><a href='#comparison-results'>Comparison Results</a></li>")
                
                # Dynamic Mismatch Sections (HTML)
                for col in existing_targets:
                    md = mismatched_data[col]
                    if md['df'].empty: continue
                    anchor_base = md['label'].lower().replace(' ', '-')
                    
                    if col == 'CONSUMER_SW_VERSION':
                        summary_lines.append("<li><a href='#sw-mismatch-matrix'>SW Mismatch Matrix</a></li>")
                    
                    summary_lines.append(f"<li><a href='#tally-{anchor_base}'>Detailed {md['label']} Mismatch Tally</a></li>")
                    summary_lines.append(f"<li><a href='#samples-{anchor_base}'>{sample_prefix}{md['label']} Mismatches</a></li>")
                
                if not missing_in_s1.empty: summary_lines.append(f"<li><a href='#samples-missing-in-source1'>{sample_prefix}VINs Found Only in Source 2</a></li>")
                if not missing_in_s2.empty: summary_lines.append(f"<li><a href='#samples-missing-in-source2'>{sample_prefix}VINs Found Only in Source 1</a></li>")
                summary_lines.append("</ul></div>")

            summary_lines.extend(toc_lines)
            
            # --- METADATA ---
            summary_lines.append(f"{meta_prefix if is_md else '<h2 id=\"comparison-metadata\">'}Comparison Metadata{'</h2>' if is_html else ''}")
            if is_html: summary_lines.append("<ul>")
            summary_lines.extend([
                f"- **Source 1 File**: `{Path(args.source1).name}`" if is_md else f"<li>Source 1 File: {Path(args.source1).name}</li>",
                f"- **Source 2 File**: `{Path(args.source2).name}`" if is_md else f"<li>Source 2 File: {Path(args.source2).name}</li>",
                f"- **Source 1 Duplicates**: {len(audit_results['s1']['dup_vins'])} unique VINs" if is_md else f"<li>Source 1 Duplicates: {len(audit_results['s1']['dup_vins'])} unique VINs</li>",
                f"- **Source 2 Duplicates**: {len(audit_results['s2']['dup_vins'])} unique VINs" if is_md else f"<li>Source 2 Duplicates: {len(audit_results['s2']['dup_vins'])} unique VINs</li>",
            ])
            if compare_vdn:
                summary_lines.extend([
                    f"- **Source 1 VDN Prefix Conflicts**: {len(audit_results['s1']['prefix_conflicts'])} unique VINs" if is_md else f"<li>Source 1 VDN Prefix Conflicts: {len(audit_results['s1']['prefix_conflicts'])} unique VINs</li>",
                    f"- **Source 2 VDN Prefix Conflicts**: {len(audit_results['s2']['prefix_conflicts'])} unique VINs" if is_md else f"<li>Source 2 VDN Prefix Conflicts: {len(audit_results['s2']['prefix_conflicts'])} unique VINs</li>",
                ])
            summary_lines.extend([
                f"- **Source 1 Incomplete Data**: {audit_results['s1']['u_empty_vins']} unique VINs" if is_md else f"<li>Source 1 Incomplete Data: {audit_results['s1']['u_empty_vins']} unique VINs</li>",
                f"- **Source 2 Incomplete Data**: {audit_results['s2']['u_empty_vins']} unique VINs" if is_md else f"<li>Source 2 Incomplete Data: {audit_results['s2']['u_empty_vins']} unique VINs</li>",
            ])
            if args.skip_nodata:
                summary_lines.extend([
                    f"- **Source 1 Skipped (No-Data)**: {skipped_nodata['s1']} VINs" if is_md else f"<li>Source 1 Skipped (No-Data): {skipped_nodata['s1']} VINs</li>",
                    f"- **Source 2 Skipped (No-Data)**: {skipped_nodata['s2']} VINs" if is_md else f"<li>Source 2 Skipped (No-Data): {skipped_nodata['s2']} VINs</li>",
                ])
            summary_lines.extend([
                f"- **Full Report**: `{full_report_path.name}`" if is_md else f"<li>Full Report: {full_report_path.name}</li>",
                f"- **Mismatches Only**: `{m_path.name}`" if is_md else f"<li>Mismatches Only: {m_path.name}</li>"
            ])
            if is_html: summary_lines.append("</ul>")

            # --- AUDIT DETAILS (File) ---
            if has_audit_errors:
                summary_lines.append(f"\n{'## Audit Details' if is_md else '<h2 id=\"audit-details\">Audit Details</h2>'}")
                if is_html: summary_lines.append("<ul>")
                for label in ['s1', 's2']:
                    res = audit_results[label]
                    l_disp = "Source 1" if label == 's1' else "Source 2"
                    
                    if res['dup_vins']:
                        if is_html:
                            summary_lines.append(f"<li><b>Duplicate VINs in {l_disp}</b>:<ul>")
                            for v in res['dup_vins']: summary_lines.append(f"<li>{v}</li>")
                            summary_lines.append("</ul></li>")
                        else:
                            summary_lines.append(f"- **Duplicate VINs in {l_disp}**:")
                            for v in res['dup_vins']: summary_lines.append(f"  - {v}")
                            
                    if res['prefix_conflicts']:
                        if is_html:
                            summary_lines.append(f"<li><b>VDN Prefix Conflicts in {l_disp}</b>:<ul>")
                            for v in res['prefix_conflicts']: summary_lines.append(f"<li>{v}</li>")
                            summary_lines.append("</ul></li>")
                        else:
                            summary_lines.append(f"- **VDN Prefix Conflicts in {l_disp}**:")
                            for v in res['prefix_conflicts']: summary_lines.append(f"  - {v}")
                    
                    if res['vin_empty_map']:
                        breakdown = ", ".join(f"{c} ({len(v)})" for c, v in res['empty_data'].items())
                        if is_html:
                            summary_lines.append(f"<li><b>Incomplete Data in {l_disp}</b> (Breakdown: {breakdown}):<details><summary>View Affected VINs ({res['u_empty_vins']} unique)</summary><ul>")
                            for v, cols in sorted(res['vin_empty_map'].items()):
                                summary_lines.append(f"<li>{v} (Missing {', '.join(cols)})</li>")
                            summary_lines.append("</ul></details></li>")
                        else:
                            summary_lines.append(f"- **Incomplete Data in {l_disp}** (Breakdown: {breakdown}):")
                            sample_vins = sorted(res['vin_empty_map'].keys())[:20]
                            for v in sample_vins:
                                cols = res['vin_empty_map'][v]
                                summary_lines.append(f"  - {v} (Missing {', '.join(cols)})")
                            if len(res['vin_empty_map']) > 20:
                                summary_lines.append(f"  ... (+{len(res['vin_empty_map'])-20} more in report file)")
                
                if is_html: summary_lines.append("</ul>")
                summary_lines.append("")

            summary_lines.extend(["", f"{res_prefix if is_md else '<h2 id=\"comparison-results\">'}Comparison Results{'</h2>' if is_html else ''}"])
            
            # Helper to add summary lines with correct format
            def add_line(label, value):
                if is_md: summary_lines.append(f"- **{label}**: {value}")
                else: summary_lines.append(f"<li>{label}: {value}</li>")

            if not is_md: summary_lines.append("<ul>")
            add_line("Total Unique VINs Analyzed", f"{u_total} (Source 1: {u_s1}, Source 2: {u_s2})")
            add_line("Unique VINs found only in Source 1", u_only_s1)
            add_line("Unique VINs found only in Source 2", u_only_s2)
            
            for col in existing_targets:
                md = mismatched_data[col]
                add_line(f"Unique VINs with {md['label']} Discrepancy (VIN exists in both)", md['u_count'])
            
            if not is_md: summary_lines.append("</ul>")
            summary_lines.append("")

            if is_html:
                # Add data-grid optimized HTML boilerplate
                html_style = "<style>body{font-family:-apple-system,BlinkMacSystemFont,\"Segoe UI\",Roboto,Arial,sans-serif;font-size:14px;line-height:1.5;color:#333;margin:20px;background:#fff}h1,h2{color:#2c3e50;border-bottom:2px solid #eee;padding-bottom:5px;margin-top:20px}h1:first-child,h2:first-child{margin-top:0}table{border-collapse:collapse;margin-bottom:30px;font-size:13px;width:auto;min-width:50%;max-width:none}th,td{border:1px solid #dcdcdc;padding:6px 10px;text-align:left;vertical-align:top;white-space:nowrap}th{background:#34495e;color:#fff;font-weight:600;white-space:nowrap;position:sticky;top:0}tr:nth-child(even){background:#f8f9fa}tr:hover{background:#e9ecef}.mismatch{color:#e74c3c;font-weight:bold}ul{margin-bottom:20px}li{margin-bottom:5px}.toc{background:#fdfdfd;border:1px solid #eee;padding:15px;border-radius:5px;display:inline-block;min-width:300px}.toc h2{margin-top:0}.toc ul{margin-bottom:0}.back-to-top{position:fixed;bottom:20px;right:20px;background:#34495e;color:#fff;padding:10px 15px;border-radius:5px;text-decoration:none;font-weight:600;font-size:12px;box-shadow:0 2px 5px rgba(0,0,0,0.2);z-index:1000}.back-to-top:hover{background:#2c3e50}details{margin:10px 0;border:1px solid #eee;border-radius:4px;padding:5px}summary{cursor:pointer;font-weight:600;color:#3498db}summary:hover{text-decoration:underline}details[open]{background:#fafafa}</style>"
                html_head = f"<!DOCTYPE html>\n<html>\n<head>\n<meta charset='utf-8'>\n<title>VDN Comparison Report</title>\n{html_style}\n</head>\n<body>\n<a href='#' class='back-to-top'>TOP &uarr;</a>"
                summary_lines.insert(0, html_head)
        else:
            summary_lines = [
                "COMPARISON METADATA", "-"*40,
                f"Source 1 File: {Path(args.source1).name}",
                f"Source 2 File: {Path(args.source2).name}",
                f"Full Report: {full_report_path.name}",
                f"Mismatches: {m_path.name}",
                f"Source 1 Dups: {len(audit_results['s1']['dup_vins'])} VINs",
                f"Source 2 Dups: {len(audit_results['s2']['dup_vins'])} VINs",
            ]
            if compare_vdn:
                summary_lines.extend([
                    f"Source 1 VDN Conflicts: {len(audit_results['s1']['prefix_conflicts'])} VINs",
                    f"Source 2 VDN Conflicts: {len(audit_results['s2']['prefix_conflicts'])} VINs",
                ])
            summary_lines.extend([
                f"Source 1 Incomplete: {audit_results['s1']['u_empty_vins']} VINs",
                f"Source 2 Incomplete: {audit_results['s2']['u_empty_vins']} VINs",
            ])
            if args.skip_nodata:
                summary_lines.extend([
                    f"Source 1 Skipped (No-Data): {skipped_nodata['s1']} VINs",
                    f"Source 2 Skipped (No-Data): {skipped_nodata['s2']} VINs",
                ])
            summary_lines.extend([
                "\nCOMPARISON RESULTS", "="*80,
                f"Total Unique VINs Analyzed: {u_total} (Source 1: {u_s1}, Source 2: {u_s2})",
                f"Unique VINs found only in Source 1: {u_only_s1}",
                f"Unique VINs found only in Source 2: {u_only_s2}"
            ])
            
            for col in existing_targets:
                md = mismatched_data[col]
                u_count_legacy = md['u_count']
                summary_lines.append(f"Unique VINs with {md['label']} Discrepancy (VIN exists in both): {u_count_legacy}")
            summary_lines.append("")

        summary_lines = [s for s in summary_lines if s is not None]

        # 3. Dynamic Mismatch Sections (Files)
        for col in existing_targets:
            md = mismatched_data[col]
            if md['df'].empty: continue
            
            anchor_base = md['label'].lower().replace(' ', '-')
            
            # A. Special handling for SW Matrix
            if col == 'CONSUMER_SW_VERSION':
                matrix_df = pd.crosstab(md['df']['s1_sw'], md['df']['s2_sw'], margins=True, margins_name='TOTAL')
                header_text = "SW VERSION MISMATCH MATRIX (Source 1 vs Source 2)"
                anchor_id = "sw-mismatch-matrix"
                if is_md or is_html:
                    sub_prefix = f"## {header_text}" if is_md else f"<h2 id=\"{anchor_id}\">{header_text}</h2>"
                    summary_lines.append(f"\n{sub_prefix}\n")
                    disp_matrix = matrix_df.map(lambda x: f'<span class="mismatch" style="color:red">{x}</span>' if str(x).isdigit() and int(x) > 0 else str(x))
                    matrix_styled = disp_matrix.reset_index().rename(columns={'s1_sw': 'Source 1 SW(row)\\Source 2 SW(col)'})
                    matrix_styled.columns.name = None
                    if is_md: summary_lines.append(matrix_styled.to_markdown(index=False))
                    else: summary_lines.append(matrix_styled.to_html(index=False, escape=False))
                elif fmt == 'rich' and has_rich:
                    from io import StringIO
                    capture_console = Console(file=StringIO(), force_terminal=False, width=250)
                    matrix_df_reset = matrix_df.reset_index().rename(columns={'s1_sw': 'Source 1 SW(row)\\Source 2 SW(col)'})
                    table = Table(show_header=True, header_style="bold magenta", show_lines=True, box=box.ASCII)
                    for i, c_name in enumerate(matrix_df_reset.columns): table.add_column(str(c_name), overflow="fold", style="bold magenta" if i == 0 else None)
                    for _, row in matrix_df_reset.iterrows(): table.add_row(*[f"[bold red]{val}[/bold red]" if str(val).isdigit() and int(val) > 0 else str(val) for val in row.values])
                    capture_console.print(table)
                    summary_lines.append(f"\n{header_text}:")
                    summary_lines.append(capture_console.file.getvalue())

            # B. Tally Section
            t_title = f"DETAILED {md['label'].upper()} MISMATCH TALLY"
            t_anchor = f"tally-{anchor_base}"
            
            # VDN has a custom pre-computed tally
            if col == 'VDN_LIST':
                t_df = vdn_tally_df
            else:
                t_df = md['df'].groupby([md['s_col'], md['t_col']]).size().reset_index(name='Count')
                t_df = t_df.sort_values('Count', ascending=False)
            
            if is_md or is_html:
                sub_prefix = f"## {t_title}" if is_md else f"<h2 id=\"{t_anchor}\">{t_title}</h2>"
                summary_lines.append(f"\n{sub_prefix}\n")
                if is_md: summary_lines.append(t_df.to_markdown(index=False))
                else: summary_lines.append(t_df.to_html(index=False, escape=False))
            else:
                summary_lines.append(f"\n{t_title}:")
                summary_lines.append(t_df.to_string(index=False))

            # C. Samples Section
            df_sampled = md['df'].head(sample_limit) if sample_limit else md['df']
            s_title = f"{'SAMPLES: ' if sample_limit else ''}{md['label']} MISMATCHES ({len(df_sampled)} entries out of total {len(md['df'])} findings)"
            s_anchor = f"samples-{anchor_base}"
            
            if col == 'VDN_LIST':
                display_cols = ['vin', 'vdn_match', 'Only in S1', 'Only in S2']
            else:
                display_cols = ['vin', md['s_col'], md['t_col'], md['match_col']]
            
            save_sample_section(df_sampled[display_cols], s_title, "bold magenta", anchor_id=s_anchor)

        # 4. Missing VIN Sections
        if not missing_in_s1.empty:
            df_ms = missing_in_s1.head(sample_limit) if sample_limit else missing_in_s1
            title_ms = f"{'SAMPLES: ' if sample_limit else ''}VINs MISSING IN SOURCE 1 ({len(df_ms)} entries out of total {len(missing_in_s1)} findings)"
            report_cols_ms = ['vin']
            for col in existing_targets:
                target_name = 'sw' if col == 'CONSUMER_SW_VERSION' else ('model' if col == 'MODEL' else col.lower())
                report_cols_ms.append(f's2_{target_name}_display')
            save_sample_section(df_ms[[c for c in report_cols_ms if c in df_ms.columns]], title_ms, "bold yellow", anchor_id="samples-missing-in-source1")
            
        if not missing_in_s2.empty:
            df_mt = missing_in_s2.head(sample_limit) if sample_limit else missing_in_s2
            title_mt = f"{'SAMPLES: ' if sample_limit else ''}VINs MISSING IN SOURCE 2 ({len(df_mt)} entries out of total {len(missing_in_s2)} findings)"
            report_cols_mt = ['vin']
            for col in existing_targets:
                target_name = 'sw' if col == 'CONSUMER_SW_VERSION' else ('model' if col == 'MODEL' else col.lower())
                report_cols_mt.append(f's1_{target_name}_display')
            save_sample_section(df_mt[[c for c in report_cols_mt if c in df_mt.columns]], title_mt, "bold yellow", anchor_id="samples-missing-in-source2")

        if mismatches_only.empty and missing_in_s1.empty and missing_in_s2.empty:
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
