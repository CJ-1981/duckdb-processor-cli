"""VDN Compare — duckdb-processor-cli analyst plugin.

Compares Source (DB) vs Target (PIE) VDN data loaded from CSV/Excel files
and produces a rich console summary plus CSV/Markdown/HTML output reports.

Usage (CLI):
    # Auto-prompt for files via Tkinter dialog:
    python main.py --run vdn_compare

    # Provide files directly:
    python main.py source.csv target.csv --run vdn_compare

    # With dedicated source/target names (if you loaded with table mapping):
    python main.py source.csv:source_db target.csv:target_db --run vdn_compare

Configuration:
    Edit analysts_plugins/vdn_compare/config.json to customise column
    mappings, normalisation rules, samples count, and output formats.
"""
from __future__ import annotations

import gc
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from duckdb_processor.analyzer import BaseAnalyzer, register

# ── Optional rich / tqdm ─────────────────────────────────────────────────────

try:
    from rich.console import Console
    from rich.table import Table
    from rich import box as _rich_box
    _has_rich = True
except ImportError:
    _has_rich = False

try:
    from tqdm import tqdm as _tqdm
    _has_tqdm = True
    _tqdm.pandas()
except ImportError:
    _has_tqdm = False

if _has_rich:
    _console = Console()
    def _cprint(msg, *args, **kwargs):
        _console.print(msg, *args, **kwargs)
else:
    def _cprint(msg, *args, **kwargs):
        print(msg)

# ── Plugin directory (for config.json resolution) ────────────────────────────

_PLUGIN_DIR = Path(__file__).parent


# ── Helpers (ported from vdn_compare.py) ─────────────────────────────────────

def _load_file(file_path: str) -> pd.DataFrame:
    """Load a CSV or Excel file into a pandas DataFrame."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext in ('.xlsx', '.xls'):
        return pd.read_excel(file_path)
    elif ext == '.csv':
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                first_line = f.readline()
            if ';' in first_line:
                sep = ';'
            elif '\t' in first_line:
                sep = '\t'
            else:
                sep = ','
        except Exception:
            sep = ','
        return pd.read_csv(file_path, sep=sep, encoding='utf-8-sig')
    else:
        raise ValueError(f"Unsupported file format: {ext}")


def _save_dataframe(df: pd.DataFrame, file_path: str, fmt: str) -> None:
    """Save dataframe to CSV or Markdown with error handling."""
    os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)
    try:
        if fmt == 'csv':
            df.to_csv(file_path, index=False)
        else:
            try:
                if len(df) > 5000:
                    _cprint(f"[bold yellow]Warning:[/bold yellow] {file_path} is large ({len(df)} rows).")
                df.to_markdown(file_path, index=False)
            except ImportError:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write('| ' + ' | '.join(df.columns) + ' |\n')
                    f.write('| ' + ' | '.join(['---'] * len(df.columns)) + ' |\n')
                    for _, row in df.iterrows():
                        f.write('| ' + ' | '.join(str(v).replace('\n', '<br>') for v in row.values) + ' |\n')
        _cprint(f"[green]Saved report to[/green] [bold white]{file_path}[/bold white]")
    except PermissionError:
        _cprint(f"\n[bold red][ERROR][/bold red] Permission denied: '{file_path}'. "
                "Please ensure the file is not open in another program.")


def _render_console_table(df: pd.DataFrame, title: str, header_style: str = "bold cyan",
                           first_col_style: str = "bold white", pager: bool = False) -> None:
    """Print a pretty Rich table to the console."""
    if df.empty:
        return
    _cprint(f"\n[{header_style}]{title}[/{header_style}]")
    if _has_rich:
        console = Console(force_terminal=True)
        table_box = _rich_box.ASCII if os.name == 'nt' and pager else _rich_box.SQUARE
        table = Table(show_header=True, header_style=header_style, show_lines=True, box=table_box)
        for i, col in enumerate(df.columns):
            table.add_column(str(col), overflow="fold", style=first_col_style if i == 0 else None)
        for _, row in df.iterrows():
            display_row = []
            for val in row.values:
                v_str = str(val)
                if len(v_str) > 40:
                    v_str = v_str[:37] + "..."
                display_row.append(v_str)
            styled_row = [
                f"[bold red]{v}[/bold red]" if v.upper() in ['MISMATCH', 'NOK'] else v
                for v in display_row
            ]
            table.add_row(*styled_row)
        if pager:
            with console.pager(styles=True):
                console.print(table)
        else:
            console.print(table)
    else:
        _cprint(df.to_string(index=False))


def _parse_vdn(val) -> list:
    """Parse a VDN list value from various raw formats."""
    if pd.isna(val) or str(val).strip() in ('nan', ''):
        return ['NO DATA']
    val_str = str(val).strip()
    if val_str.startswith('[') and val_str.endswith(']'):
        try:
            parsed = json.loads(val_str.replace("'", '"'))
            if isinstance(parsed, list):
                result = sorted(str(v).strip() for v in parsed if str(v).strip())
                return result if result else ['NO DATA']
        except Exception:
            pass
    chunks = [val_str[i:i+4] for i in range(0, len(val_str), 4)]
    result = sorted(c for c in chunks if c.strip())
    return result if result else ['NO DATA']


# ── Plugin ────────────────────────────────────────────────────────────────────

@register
class VdnComparePlugin(BaseAnalyzer):
    """Compares Source (DB) and Target (PIE) VDN/SW/Model data."""

    name = "vdn_compare"
    description = "Compare Source and Target VDN data (Load both CSVs in Data Preview tab first)"

    # ── Configuration loading ─────────────────────────────────

    def _load_config(self) -> dict:
        """Load config.json from the plugin directory (if present)."""
        cfg_path = _PLUGIN_DIR / "config.json"
        if cfg_path.exists():
            try:
                with open(cfg_path, 'r', encoding='utf-8') as f:
                    raw = json.load(f)
                # Strip comment keys
                return {k: v for k, v in raw.items() if not k.startswith('_comment')}
            except Exception as e:
                _cprint(f"[yellow]Warning: Could not parse vdn_compare/config.json: {e}[/yellow]")
        return {}

    # ── File acquisition ─────────────────────────────────────

    def _acquire_files(self, p) -> Optional[tuple[pd.DataFrame, pd.DataFrame, str, str]]:
        """Return (df_source, df_target, source_label, target_label).

        Strategy:
        1. Use 'source_db' and 'target_db' if they exist.
        2. Use the first two loaded tables if available.
        3. If exactly one table is loaded, use it as 'source' and ONLY ask for 'target'.
        4. If no tables are loaded and we are in an interactive CLI, ask for both.
        5. If in Gradio or non-interactive CLI, error out if tables are missing.
        """
        tables = p.get_tables() if hasattr(p, 'get_tables') else []
        if not tables and hasattr(p, 'table'):
            tables = [p.table]

        # 1. Named tables take precedence
        if 'source_db' in tables and 'target_db' in tables:
            _cprint("[cyan]Using pre-loaded tables 'source_db' and 'target_db'.[/cyan]")
            df_src = p.con.execute('SELECT * FROM "source_db"').df()
            df_tgt = p.con.execute('SELECT * FROM "target_db"').df()
            return df_src, df_tgt, "source_db", "target_db"

        # 2. Exactly two tables (or more)
        if len(tables) >= 2:
            _cprint(f"[cyan]Using first two loaded tables: '{tables[0]}' (source) and '{tables[1]}' (target).[/cyan]")
            df_src = p.con.execute(f'SELECT * FROM "{tables[0]}"').df()
            df_tgt = p.con.execute(f'SELECT * FROM "{tables[1]}"').df()
            return df_src, df_tgt, tables[0], tables[1]

        # 3. Environment check
        is_interactive = sys.stdin.isatty() and not 'gradio' in sys.modules
        
        # 4. Fallback or Error
        if not is_interactive:
            msg = ""
            if len(tables) == 1:
                msg = f"Only one table loaded ('{tables[0]}'). vdn_compare needs TWO tables to compare."
            else:
                msg = "No data tables loaded. vdn_compare needs TWO tables to compare."
            
            _cprint(f"[bold red]Error:[/bold red] {msg}")
            _cprint("[bold yellow]In Gradio, upload BOTH files together in 'Data Preview' and click 'Load Data'.[/bold yellow]")
            _cprint("[cyan]Tip: Use 'source_db, target_db' in Table Mapping field for clarity.[/cyan]")
            raise RuntimeError(msg)

        # 5. Interactive Tkinter dialog (CLI fallback only)
        try:
            import tkinter as tk
            from tkinter import filedialog

            root = tk.Tk()
            root.withdraw()
            root.attributes('-topmost', True)

            df_src, src_label = None, ""
            df_tgt, tgt_label = None, ""

            # Use existing table as source if available
            if len(tables) == 1:
                _cprint(f"[cyan]Using existing table '{tables[0]}' as Source.[/cyan]")
                df_src = p.con.execute(f'SELECT * FROM "{tables[0]}"').df()
                src_label = tables[0]
            else:
                _cprint("[cyan]Please select the Source file (DB)...[/cyan]")
                path = filedialog.askopenfilename(title="Select Source file (DB)", 
                                                   filetypes=[("CSV/Excel", "*.csv *.xlsx *.xls")])
                if not path: return None
                df_src, src_label = _load_file(path), os.path.basename(path)

            _cprint("[cyan]Please select the Target file (PIE)...[/cyan]")
            path = filedialog.askopenfilename(title="Select Target file (PIE)", 
                                               filetypes=[("CSV/Excel", "*.csv *.xlsx *.xls")])
            if not path: return None
            df_tgt, tgt_label = _load_file(path), os.path.basename(path)

            root.destroy()
            return df_src, df_tgt, src_label, tgt_label

        except Exception as exc:
            _cprint(f"[bold red]Error acquiring files: {exc}[/bold red]")
            return None

    # ── Main run ─────────────────────────────────────────────

    def run(self, p) -> None:  # noqa: C901 (acceptable complexity for a full analysis plugin)
        import time
        start_time = time.time()

        cfg = self._load_config()

        # ── Settings (config.json → hardcoded defaults) ───────
        sort_vin      = cfg.get('sort_vin', 'asc')
        samples_raw   = str(cfg.get('samples', '10'))
        comp_flags    = [c.lower() for c in cfg.get('compare', ['sw', 'vdn', 'model'])]
        req_formats   = list(set(f.lower() for f in cfg.get('format', ['rich', 'md', 'html'])))
        norm_models   = cfg.get('normalize_models', ['EX30,V216', 'EX30 CC,V216-CC'])
        norm_sw       = cfg.get('normalize_sw', ['MY27 J1,27 J1'])
        shared_map    = cfg.get('column_map', {})
        s_specific    = cfg.get('source_map', {})
        t_specific    = cfg.get('target_map', {})

        sample_limit  = None if samples_raw.lower() == 'all' else (int(samples_raw) if samples_raw.isdigit() else 10)

        # ── 1. Acquire DataFrames ─────────────────────────────
        result = self._acquire_files(p)
        if result is None:
            return
        df_source, df_target, source_label, target_label = result

        # ── 2. Column mapping ─────────────────────────────────
        common_map = {
            'vin': 'VIN',
            'DB_SW': 'CONSUMER_SW_VERSION',
            'DB_targetVdns': 'VDN_LIST',
            'model': 'MODEL',
        }
        common_map.update(shared_map)
        common_map.update(s_specific)
        common_map.update(t_specific)

        df_source.rename(columns=common_map, inplace=True)
        df_target.rename(columns=common_map, inplace=True)

        target_cols     = [c for c in set(common_map.values()) if c != 'VIN']
        existing_targets = [c for c in target_cols if c in df_source.columns and c in df_target.columns]

        compare_sw    = 'CONSUMER_SW_VERSION' in existing_targets
        compare_model = 'MODEL' in existing_targets
        compare_vdn   = 'VDN_LIST' in existing_targets

        if not compare_vdn and 'vdn' in comp_flags:
            _cprint("[yellow]Auto-disabling VDN comparison: VDN_LIST header missing.[/yellow]")

        if 'VIN' not in df_source.columns or 'VIN' not in df_target.columns:
            _cprint("[bold red]CRITICAL: 'VIN' column not found in one or both files.[/bold red]")
            return

        # ── 3. Trim / normalise ───────────────────────────────
        for df in (df_source, df_target):
            df.columns = [str(c).strip().replace('"', '') for c in df.columns]
            for col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.replace(r'^"|"$', '', regex=True)
                df[col] = df[col].replace({'nan': None, 'NaN': None, 'None': None, '': None})

            if compare_model and 'MODEL' in df.columns:
                df['MODEL_NORM']    = df['MODEL']
                df['MODEL_DISPLAY'] = df['MODEL']
                for group in norm_models:
                    models = [m.strip() for m in group.split(',')]
                    if len(models) > 1:
                        primary = models[0]
                        for alias in models[1:]:
                            df.loc[df['MODEL'] == alias, 'MODEL_NORM']    = primary
                            df.loc[df['MODEL'] == alias, 'MODEL_DISPLAY'] = f"{primary}({alias})"

            if compare_sw and 'CONSUMER_SW_VERSION' in df.columns:
                df['SW_NORM']    = df['CONSUMER_SW_VERSION']
                df['SW_DISPLAY'] = df['CONSUMER_SW_VERSION']
                if norm_sw:
                    for group in norm_sw:
                        versions = [v.strip() for v in group.split(',')]
                        if len(versions) > 1:
                            primary = versions[0]
                            for alias in versions[1:]:
                                df.loc[df['CONSUMER_SW_VERSION'] == alias, 'SW_NORM']    = primary
                                df.loc[df['CONSUMER_SW_VERSION'] == alias, 'SW_DISPLAY'] = f"{primary}({alias})"

        # ── 4. Parse VDN_LIST ─────────────────────────────────
        for df in (df_source, df_target):
            if compare_vdn and 'VDN_LIST' in df.columns:
                if _has_tqdm:
                    df['VDN_LIST_CLEAN'] = df['VDN_LIST'].progress_apply(
                        lambda x: json.dumps(_parse_vdn(x))
                    )
                else:
                    df['VDN_LIST_CLEAN'] = df['VDN_LIST'].apply(
                        lambda x: json.dumps(_parse_vdn(x))
                    )
                df.drop(columns=['VDN_LIST'], inplace=True)

        # ── 5. DuckDB comparison via the Processor's connection ─
        con = p.con
        # Register dataframes as in-memory views (avoids collisions with existing tables)
        con.register('_vdncmp_source', df_source)
        con.register('_vdncmp_target', df_target)

        sort_clause = f"ORDER BY vin {sort_vin.upper()}" if sort_vin != 'none' else ""

        s_selects = ["VIN as vin"]
        t_selects = ["VIN as vin"]
        for col in existing_targets:
            if col == 'CONSUMER_SW_VERSION':
                s_selects.extend(["SW_NORM as source_sw", "SW_DISPLAY as source_sw_disp"])
                t_selects.extend(["SW_NORM as target_sw", "SW_DISPLAY as target_sw_disp"])
            elif col == 'MODEL':
                s_selects.extend(["MODEL_NORM as source_model", "MODEL_DISPLAY as source_model_disp"])
                t_selects.extend(["MODEL_NORM as target_model", "MODEL_DISPLAY as target_model_disp"])
            elif col == 'VDN_LIST':
                s_selects.append("VDN_LIST_CLEAN as source_vdns_json")
                t_selects.append("VDN_LIST_CLEAN as target_vdns_json")
            else:
                s_selects.append(f"{col} as source_{col}")
                t_selects.append(f"{col} as target_{col}")

        s_selects_str = ",\n            ".join(s_selects)
        t_selects_str = ",\n            ".join(t_selects)

        joined_selects = ["COALESCE(s.vin, t.vin) as vin"]
        joined_selects.append("CASE WHEN s.vin IS NOT NULL THEN 1 ELSE 0 END as source_exists")
        joined_selects.append("CASE WHEN t.vin IS NOT NULL THEN 1 ELSE 0 END as target_exists")

        for col in existing_targets:
            if col == 'CONSUMER_SW_VERSION':
                joined_selects.append("CASE WHEN s.vin IS NULL THEN 'N/A' ELSE COALESCE(s.source_sw_disp, 'NO DATA') END as source_sw_display")
                joined_selects.append("CASE WHEN t.vin IS NULL THEN 'N/A' ELSE COALESCE(t.target_sw_disp, 'NO DATA') END as target_sw_display")
                joined_selects.append("CASE WHEN s.vin IS NULL OR t.vin IS NULL THEN 'MISMATCH' WHEN s.source_sw IS NOT DISTINCT FROM t.target_sw THEN 'MATCH' ELSE 'MISMATCH' END as sw_match")
            elif col == 'MODEL':
                joined_selects.append("CASE WHEN s.vin IS NULL THEN 'N/A' ELSE COALESCE(s.source_model_disp, 'NO DATA') END as source_model_display")
                joined_selects.append("CASE WHEN t.vin IS NULL THEN 'N/A' ELSE COALESCE(t.target_model_disp, 'NO DATA') END as target_model_display")
                joined_selects.append("CASE WHEN s.vin IS NULL OR t.vin IS NULL THEN 'MISMATCH' WHEN s.source_model IS NOT DISTINCT FROM t.target_model THEN 'MATCH' ELSE 'MISMATCH' END as model_match")
            elif col == 'VDN_LIST':
                joined_selects.append("CASE WHEN s.vin IS NULL OR t.vin IS NULL THEN 'MISMATCH' WHEN s.source_vdns_json IS NOT DISTINCT FROM t.target_vdns_json THEN 'MATCH' ELSE 'MISMATCH' END as vdn_match")
                joined_selects.append("s.source_vdns_json as s_json")
                joined_selects.append("t.target_vdns_json as t_json")
            else:
                m_col = f"{col.lower()}_match"
                joined_selects.append(f"CASE WHEN s.vin IS NULL THEN 'N/A' ELSE COALESCE(CAST(s.source_{col} as VARCHAR), 'NO DATA') END as source_{col.lower()}_display")
                joined_selects.append(f"CASE WHEN t.vin IS NULL THEN 'N/A' ELSE COALESCE(CAST(t.target_{col} as VARCHAR), 'NO DATA') END as target_{col.lower()}_display")
                joined_selects.append(f"CASE WHEN s.vin IS NULL OR t.vin IS NULL THEN 'MISMATCH' WHEN s.source_{col} IS NOT DISTINCT FROM t.target_{col} THEN 'MATCH' ELSE 'MISMATCH' END as {m_col}")

        joined_selects_str = ",\n            ".join(joined_selects)

        final_selects = ["vin", "source_exists", "target_exists"]
        mismatch_conditions = []

        for col in existing_targets:
            if col == 'CONSUMER_SW_VERSION':
                final_selects.extend(["source_sw_display as source_sw", "target_sw_display as target_sw", "sw_match"])
                mismatch_conditions.append("sw_match = 'MISMATCH'")
            elif col == 'MODEL':
                final_selects.extend(["source_model_display as source_model", "target_model_display as target_model", "model_match"])
                mismatch_conditions.append("model_match = 'MISMATCH'")
            elif col == 'VDN_LIST':
                final_selects.extend(["vdn_match", "s_json as s_vdns_json", "t_json as t_vdns_json"])
                mismatch_conditions.append("vdn_match = 'MISMATCH'")
            else:
                m_col = f"{col.lower()}_match"
                final_selects.extend([
                    f"source_{col.lower()}_display as source_{col.lower()}",
                    f"target_{col.lower()}_display as target_{col.lower()}",
                    m_col
                ])
                mismatch_conditions.append(f"{m_col} = 'MISMATCH'")

        if mismatch_conditions:
            result_cond = " OR ".join(mismatch_conditions)
            final_selects.append(f"CASE WHEN {result_cond} THEN 'NOK' ELSE 'OK' END as Result")
        else:
            final_selects.append("'OK' as Result")

        final_selects_str = ",\n        ".join(final_selects)

        compare_query = f"""
        WITH source_data AS (
            SELECT {s_selects_str}
            FROM _vdncmp_source
            WHERE VIN IS NOT NULL AND VIN != 'nan'
        ),
        target_data AS (
            SELECT {t_selects_str}
            FROM _vdncmp_target
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

        # Clean up registered views
        try:
            con.unregister('_vdncmp_source')
            con.unregister('_vdncmp_target')
        except Exception:
            pass

        del df_source, df_target
        gc.collect()

        # ── 6. VDN diff extraction ────────────────────────────
        if compare_vdn:
            def _compute_diff(row):
                if row.get('vdn_match') != 'MISMATCH' or pd.isna(row.get('vdn_match')):
                    return "", ""
                s_vdns_str = row.get('s_vdns_json')
                t_vdns_str = row.get('t_vdns_json')
                s_vdns = set(json.loads(s_vdns_str)) if pd.notna(s_vdns_str) else set()
                t_vdns = set(json.loads(t_vdns_str)) if pd.notna(t_vdns_str) else set()
                only_in_t = t_vdns - s_vdns
                only_in_s = s_vdns - t_vdns
                return (
                    ", ".join(sorted(only_in_t)) if only_in_t else "",
                    ", ".join(sorted(only_in_s)) if only_in_s else "",
                )

            if _has_tqdm:
                _tqdm.pandas(desc="Computing Diffs")
                diffs = result_df.progress_apply(_compute_diff, axis=1)
            else:
                diffs = result_df.apply(_compute_diff, axis=1)

            result_df['Only in Target (missing in Source)'] = diffs.map(lambda x: x[0])
            result_df['Only in Source (missing in Target)'] = diffs.map(lambda x: x[1])
            final_output = result_df.drop(columns=['s_vdns_json', 't_vdns_json'], errors='ignore')
        else:
            final_output = result_df

        # ── 7. Summary statistics ─────────────────────────────
        missing_in_source = final_output[final_output['source_exists'] == 0]
        missing_in_target = final_output[final_output['target_exists'] == 0]

        mismatched_sw = final_output[
            (final_output['sw_match'] == 'MISMATCH') &
            ~final_output['vin'].isin(missing_in_source['vin']) &
            ~final_output['vin'].isin(missing_in_target['vin'])
        ] if compare_sw else pd.DataFrame()

        mismatched_model = final_output[
            (final_output['model_match'] == 'MISMATCH') &
            ~final_output['vin'].isin(missing_in_source['vin']) &
            ~final_output['vin'].isin(missing_in_target['vin'])
        ] if compare_model else pd.DataFrame()

        mismatched_vdns = final_output[
            (final_output['vdn_match'] == 'MISMATCH') &
            ~final_output['vin'].isin(missing_in_source['vin']) &
            ~final_output['vin'].isin(missing_in_target['vin'])
        ] if compare_vdn else pd.DataFrame()

        # ── 8. Save CSV outputs ───────────────────────────────
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs('output', exist_ok=True)
        full_report_path = f"output/full_comparison_results_{timestamp}.csv"
        m_path           = f"output/mismatch-only_{timestamp}.csv"

        _save_dataframe(final_output, full_report_path, 'csv')
        mismatches_only = final_output[final_output['Result'] == 'NOK']
        _save_dataframe(mismatches_only, m_path, 'csv')

        # ── 9. Console output ─────────────────────────────────
        summary_stats = [
            f"Total VINs Analyzed: {len(final_output)}",
            f"VINs missing in Source: {len(missing_in_source)}",
            f"VINs missing in Target: {len(missing_in_target)}",
        ]
        if compare_model: summary_stats.append(f"VINs with True Mismatched Model: {len(mismatched_model)}")
        if compare_sw:
            summary_stats.append(f"VINs with True Mismatched SW: {len(mismatched_sw)}")
            summary_stats.append(f"VINs with Matched SW: {len(final_output) - len(mismatched_sw) - len(missing_in_source) - len(missing_in_target)}")
        if compare_vdn:
            summary_stats.append(f"VINs with True Mismatched VDNs: {len(mismatched_vdns)}")
            summary_stats.append(f"VINs with Matched VDNs: {len(final_output) - len(mismatched_vdns) - len(missing_in_source) - len(missing_in_target)}")

        _cprint(f"\n[bold cyan]{'='*60}[/bold cyan]")
        _cprint(f"[bold cyan]  VDN COMPARE: {source_label} vs {target_label}[/bold cyan]")
        _cprint(f"[bold cyan]{'='*60}[/bold cyan]")
        for line in summary_stats:
            _cprint(f"[bold cyan]{line}[/bold cyan]")

        # SW mismatch matrix
        if compare_sw and not mismatched_sw.empty:
            _cprint("\n[bold magenta]SW VERSION MISMATCH MATRIX (Source vs Target):[/bold magenta]")
            matrix_df = pd.crosstab(mismatched_sw['source_sw'], mismatched_sw['target_sw'], margins=True, margins_name='TOTAL')
            if _has_rich:
                console = Console(force_terminal=True)
                matrix_df_reset = matrix_df.reset_index().rename(columns={'source_sw': 'Source SW(row)\\Target SW(col)'})
                table = Table(show_header=True, header_style="bold magenta", show_lines=True, box=_rich_box.SQUARE)
                for i, col in enumerate(matrix_df_reset.columns):
                    table.add_column(str(col), overflow="fold", style="bold magenta" if i == 0 else None)
                for _, row in matrix_df_reset.iterrows():
                    table.add_row(*[f"[bold red]{val}[/bold red]" if str(val).isdigit() and int(val) > 0 else str(val) for val in row.values])
                console.print(table)
            else:
                _cprint(matrix_df.to_string())

            sw_counts = mismatched_sw.groupby(['source_sw', 'target_sw']).size().reset_index(name='count')
            _render_console_table(sw_counts.sort_values('count', ascending=False), "DETAILED SW MISMATCH TALLY", header_style="bold magenta")

        # Sample tables
        if compare_model and not mismatched_model.empty:
            df_mm = mismatched_model.head(sample_limit) if sample_limit else mismatched_model
            _render_console_table(df_mm[['vin', 'source_model', 'target_model', 'model_match']],
                                   f"{'SAMPLES: ' if sample_limit else ''}MODEL MISMATCHES ({len(df_mm)} of {len(mismatched_model)})",
                                   header_style="bold magenta")

        if not missing_in_source.empty:
            cols = ['vin'] + (['target_model'] if compare_model else []) + (['target_sw'] if compare_sw else [])
            df_ms = missing_in_source.head(sample_limit) if sample_limit else missing_in_source
            _render_console_table(df_ms[[c for c in cols if c in df_ms.columns]],
                                   f"{'SAMPLES: ' if sample_limit else ''}VINs MISSING IN SOURCE ({len(df_ms)} of {len(missing_in_source)})",
                                   header_style="bold yellow")

        if not missing_in_target.empty:
            cols = ['vin'] + (['source_model'] if compare_model else []) + (['source_sw'] if compare_sw else [])
            df_mt = missing_in_target.head(sample_limit) if sample_limit else missing_in_target
            _render_console_table(df_mt[[c for c in cols if c in df_mt.columns]],
                                   f"{'SAMPLES: ' if sample_limit else ''}VINs MISSING IN TARGET ({len(df_mt)} of {len(missing_in_target)})",
                                   header_style="bold yellow")

        true_mismatches = final_output[
            (final_output['Result'] == 'NOK') &
            ~final_output['vin'].isin(missing_in_source['vin']) &
            ~final_output['vin'].isin(missing_in_target['vin'])
        ]
        if not true_mismatches.empty:
            sample_df = true_mismatches.head(sample_limit) if sample_limit else true_mismatches
            cols = ['vin']
            if compare_model: cols.extend(['source_model', 'target_model', 'model_match'])
            if compare_sw:    cols.extend(['source_sw', 'target_sw', 'sw_match'])
            cols.append('Result')
            if compare_vdn:   cols.extend(['vdn_match', 'Only in Target (missing in Source)', 'Only in Source (missing in Target)'])
            _render_console_table(sample_df[[c for c in cols if c in sample_df.columns]],
                                   f"{'SAMPLES: ' if sample_limit else ''}VDN DATA MISMATCHES ({len(sample_df)} of {len(true_mismatches)})",
                                   header_style="bold cyan")

        # ── 10. File reports (md / html / rich / csv) ─────────
        # (Identical report generation logic from original vdn_compare.py)
        for fmt in req_formats:
            is_md   = fmt in ['markdown', 'md']
            is_html = fmt == 'html'
            sum_ext = ".html" if is_html else (".md" if is_md else (".txt" if fmt == 'rich' else ".csv"))
            curr_summary_path = f"output/summary_{timestamp}{sum_ext}"

            if is_md or is_html:
                title_prefix = "# " if is_md else "<h1>"
                title_suffix = "" if is_md else "</h1>"
                meta_prefix  = "## " if is_md else "<h2>"

                summary_lines = [f"{title_prefix}Comparison Report{title_suffix}"]

                if is_html:
                    html_style = "<style>body{font-family:-apple-system,BlinkMacSystemFont,\"Segoe UI\",Roboto,Arial,sans-serif;font-size:14px;line-height:1.5;color:#333;margin:20px;background:#fff}h1,h2{color:#2c3e50;border-bottom:2px solid #eee;padding-bottom:5px;margin-top:20px}table{border-collapse:collapse;margin-bottom:30px;font-size:13px;width:auto}th,td{border:1px solid #dcdcdc;padding:6px 10px;text-align:left;vertical-align:top;white-space:nowrap}th{background:#34495e;color:#fff;font-weight:600;position:sticky;top:0}tr:nth-child(even){background:#f8f9fa}tr:hover{background:#e9ecef}.mismatch{color:#e74c3c;font-weight:bold}ul{margin-bottom:20px}li{margin-bottom:5px}.toc{background:#fdfdfd;border:1px solid #eee;padding:15px;border-radius:5px;display:inline-block;min-width:300px}.back-to-top{position:fixed;bottom:20px;right:20px;background:#34495e;color:#fff;padding:10px 15px;border-radius:5px;text-decoration:none;font-weight:600;font-size:12px;box-shadow:0 2px 5px rgba(0,0,0,0.2)}</style>"
                    html_head  = f"<!DOCTYPE html>\n<html>\n<head>\n<meta charset='utf-8'>\n<title>VDN Comparison Report</title>\n{html_style}\n</head>\n<body>\n<a href='#' class='back-to-top'>TOP &uarr;</a>"
                    summary_lines.insert(0, html_head)

                # Metadata section
                summary_lines.append(f"\n{meta_prefix}Comparison Metadata{'</h2>' if is_html else ''}")
                if is_html: summary_lines.append("<ul>")
                def _ml(label, value):
                    if is_md: summary_lines.append(f"- **{label}**: {value}")
                    else:     summary_lines.append(f"<li>{label}: {value}</li>")
                _ml("Source File", source_label)
                _ml("Target File", target_label)
                _ml("Full Report", os.path.basename(full_report_path))
                _ml("Mismatches Only", os.path.basename(m_path))
                if is_html: summary_lines.append("</ul>")

                # Results section
                summary_lines.append(f"\n{meta_prefix}Comparison Results{'</h2>' if is_html else ''}")
                if is_html: summary_lines.append("<ul>")
                _ml("Total VINs Analyzed", len(final_output))
                _ml("VINs missing in Source", len(missing_in_source))
                _ml("VINs missing in Target", len(missing_in_target))
                if compare_model: _ml("VINs with True Mismatched Model", len(mismatched_model))
                if compare_sw:
                    _ml("VINs with True Mismatched SW", len(mismatched_sw))
                    _ml("VINs with Matched SW", len(final_output) - len(mismatched_sw) - len(missing_in_source) - len(missing_in_target))
                if compare_vdn:
                    _ml("VINs with True Mismatched VDNs", len(mismatched_vdns))
                    _ml("VINs with Matched VDNs", len(final_output) - len(mismatched_vdns) - len(missing_in_source) - len(missing_in_target))
                if is_html: summary_lines.append("</ul>")

                # SW Matrix
                if compare_sw and not mismatched_sw.empty:
                    matrix_df = pd.crosstab(mismatched_sw['source_sw'], mismatched_sw['target_sw'], margins=True, margins_name='TOTAL')
                    hdr = "SW VERSION MISMATCH MATRIX"
                    sub = f"## {hdr}" if is_md else f"<h2>{hdr}</h2>"
                    summary_lines.append(f"\n{sub}\n")
                    matrix_df_reset = matrix_df.reset_index().rename(columns={'source_sw': 'Source SW(row)\\Target SW(col)'})
                    matrix_df_reset.columns.name = None
                    if is_md:
                        summary_lines.append(matrix_df_reset.to_markdown(index=False))
                    else:
                        summary_lines.append(matrix_df_reset.to_html(index=False, escape=False))

                    sw_counts = mismatched_sw.groupby(['source_sw', 'target_sw']).size().reset_index(name='count').sort_values('count', ascending=False)
                    hdr2 = "DETAILED SW MISMATCH TALLY"
                    sub2 = f"## {hdr2}" if is_md else f"<h2>{hdr2}</h2>"
                    summary_lines.append(f"\n{sub2}\n")
                    summary_lines.append(sw_counts.to_markdown(index=False) if is_md else sw_counts.to_html(index=False, escape=False))

                def _save_section(df_section, title):
                    if df_section.empty:
                        return
                    sub = f"## {title}" if is_md else f"<h2>{title}</h2>"
                    summary_lines.append(f"\n{sub}\n")
                    md_s = df_section.copy()
                    for col in md_s.columns:
                        md_s[col] = md_s[col].apply(lambda x: str(x)[:37] + "..." if len(str(x)) > 40 else str(x))
                    red = ['MISMATCH', 'NOK']
                    md_s = md_s.map(lambda x: f'<span style="color:red" class="mismatch">{x}</span>' if str(x).upper() in red else str(x))
                    summary_lines.append(md_s.to_markdown(index=False) if is_md else md_s.to_html(index=False, escape=False))

                if compare_model and not mismatched_model.empty:
                    df_mm = mismatched_model.head(sample_limit) if sample_limit else mismatched_model
                    _save_section(df_mm[['vin', 'source_model', 'target_model', 'model_match']],
                                   f"{'SAMPLES: ' if sample_limit else ''}MODEL MISMATCHES ({len(df_mm)} of {len(mismatched_model)})")

                if not true_mismatches.empty:
                    df_s2 = true_mismatches.head(sample_limit) if sample_limit else true_mismatches
                    _save_section(df_s2[[c for c in cols if c in df_s2.columns]],
                                   f"{'SAMPLES: ' if sample_limit else ''}VDN DATA MISMATCHES ({len(df_s2)} of {len(true_mismatches)})")

                if not missing_in_source.empty:
                    df_ms = missing_in_source.head(sample_limit) if sample_limit else missing_in_source
                    ms_cols = ['vin'] + [c for c in ['target_model', 'target_sw'] if c in df_ms.columns]
                    _save_section(df_ms[ms_cols], f"{'SAMPLES: ' if sample_limit else ''}VINs MISSING IN SOURCE ({len(df_ms)} of {len(missing_in_source)})")

                if not missing_in_target.empty:
                    df_mt2 = missing_in_target.head(sample_limit) if sample_limit else missing_in_target
                    mt_cols = ['vin'] + [c for c in ['source_model', 'source_sw'] if c in df_mt2.columns]
                    _save_section(df_mt2[mt_cols], f"{'SAMPLES: ' if sample_limit else ''}VINs MISSING IN TARGET ({len(df_mt2)} of {len(missing_in_target)})")

                if is_html:
                    summary_lines.append("</body></html>")

            else:
                summary_lines = [
                    "COMPARISON METADATA", "-"*40,
                    f"Source File: {source_label}",
                    f"Target File: {target_label}",
                    f"Full Report: {os.path.basename(full_report_path)}",
                    f"Mismatches: {os.path.basename(m_path)}",
                    "\nCOMPARISON RESULTS", "="*80,
                    f"Total VINs Analyzed: {len(final_output)}",
                    f"VINs missing in Source: {len(missing_in_source)}",
                    f"VINs missing in Target: {len(missing_in_target)}",
                ]
                if compare_model: summary_lines.append(f"VINs with True Mismatched Model: {len(mismatched_model)}")
                if compare_sw:    summary_lines.extend([f"VINs with True Mismatched SW: {len(mismatched_sw)}", f"VINs with Matched SW: {len(final_output) - len(mismatched_sw) - len(missing_in_source) - len(missing_in_target)}"])
                if compare_vdn:   summary_lines.extend([f"VINs with True Mismatched VDNs: {len(mismatched_vdns)}", f"VINs with Matched VDNs: {len(final_output) - len(mismatched_vdns) - len(missing_in_source) - len(missing_in_target)}"])

            summary_text = "\n".join(s for s in summary_lines if s is not None)
            with open(curr_summary_path, 'w', encoding='utf-8') as f:
                f.write(summary_text)
            _cprint(f"[green]Saved Summary ({fmt}) to[/green] [bold white]{curr_summary_path}[/bold white]")

        # ── 11. Expose result to CLI pipeline ─────────────────
        p.last_result  = final_output
        p.last_action  = "vdn_compare"

        duration = time.time() - start_time
        _cprint(f"\n[bold green]vdn_compare completed in {duration:.2f} seconds.[/bold green]")
