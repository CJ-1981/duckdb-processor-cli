import gradio as gr
import pandas as pd
import sqlparse
import json
import os
import shutil
import autopep8
import importlib.util
import inspect
import io
import contextlib
import tempfile
import sys
import logging
import traceback
import datetime
import atexit
from fpdf import FPDF
from fpdf.enums import XPos, YPos

# Set up detailed logging for debugging file loading issues
logging.basicConfig(
    level=logging.INFO, # Reduced from DEBUG for production feel
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)
logger.info("=== DuckDB Processor CLI Starting ===")

# Create a dedicated temp directory for the session
TEMP_DIR = os.path.join(tempfile.gettempdir(), "duckdb_processor_temp")
os.makedirs(TEMP_DIR, exist_ok=True)

def cleanup_temp():
    """Remove temp files on exit."""
    if os.path.exists(TEMP_DIR):
        try:
            shutil.rmtree(TEMP_DIR)
            logger.info("Cleaned up temporary export directory.")
        except Exception as e:
            logger.error(f"Failed to cleanup temp dir: {e}")

atexit.register(cleanup_temp)

from duckdb_processor.loader import load
from duckdb_processor.config import ProcessorConfig
from duckdb_processor.analyzer import list_analyzers, get_analyzer

# Configuration paths and default asset handling (PyInstaller support)
def resolve_asset_path(filename):
    """Resolve path to local working directory or bundled PyInstaller resources."""
    if os.path.exists(filename):
        return filename
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        bundle_path = os.path.join(sys._MEIPASS, filename)
        if os.path.exists(bundle_path):
            return bundle_path
    return filename

def ensure_local_configs():
    """Copy bundled default configs to local working directory if missing."""
    if not getattr(sys, 'frozen', False):
        return
    for cfg in ["sql_patterns.json", "report_templates.json"]:
        if not os.path.exists(cfg):
            bundled = os.path.join(sys._MEIPASS, cfg)
            if os.path.exists(bundled):
                try:
                    shutil.copy(bundled, cfg)
                    logger.info(f"Restored default config: {cfg}")
                except Exception as e:
                    logger.error(f"Failed to restore default config {cfg}: {e}")

# Initial config restore for frozen builds
ensure_local_configs()

# SQL Patterns Library
SQL_PATTERNS = {
    "Select Top 10": "SELECT * FROM data LIMIT 10;",
    "Count by Category": "SELECT category, COUNT(*) as count FROM data GROUP BY category ORDER BY count DESC;",
    "Sum by Category": "SELECT category, SUM(TRY_CAST(amount AS DOUBLE)) as total FROM data GROUP BY category ORDER BY total DESC;",
    "Missing Values Check": "SELECT * FROM (SELECT 'all_columns' as col, COUNT(*) as total FROM data) CROSS JOIN (SELECT count(*) as missing FROM data WHERE column_name IS NULL); -- Edit column_name",
    "Date Trend (Daily)": "SELECT CAST(date AS DATE) as d, COUNT(*) as count FROM data GROUP BY d ORDER BY d;",
    "Value Distribution": "SELECT amount, COUNT(*) as count FROM data GROUP BY amount ORDER BY count DESC LIMIT 20;"
}

PATTERNS_FILE = "sql_patterns.json"
TEMPLATES_FILE = "report_templates.json"
PLUGIN_TEMPLATE = """from duckdb_processor.analyzer import BaseAnalyzer, register

@register
class CustomPlugin(BaseAnalyzer):
    name = "custom_plugin"
    description = "Quick-start custom plugin template"

    def run(self, p):
        # Your logic here
        df = p.sql("SELECT * FROM data LIMIT 100")
        print(f"Loaded {len(df)} rows.")
        p.last_result = df
"""
LAST_SESSION_FILE = ".gradio_session.json"

REPORT_TEMPLATES = {
    "Basic Summary": [
        {"type": "Data Summary", "heading": "Overview", "body": ""},
        {"type": "Schema Info", "heading": "Data Structure", "body": ""},
        {"type": "Analyzer Results Table", "heading": "Analysis Details", "body": ""}
    ],
    "SQL Report": [
        {"type": "Text/Note", "heading": "Executive Summary", "body": "Enter your summary here..."},
        {"type": "SQL Results Table", "heading": "Query Results", "body": ""},
        {"type": "Data Summary", "heading": "Data Stats", "body": ""}
    ]
}

def load_report_templates():
    """Load user report templates from file and merge with defaults."""
    global REPORT_TEMPLATES
    if os.path.exists(TEMPLATES_FILE):
        try:
            with open(TEMPLATES_FILE, "r") as f:
                user_templates = json.load(f)
                REPORT_TEMPLATES.update(user_templates)
        except Exception as e:
            logger.error(f"Error loading templates: {e}")
    return list(REPORT_TEMPLATES.keys())

def save_new_template(name, sections):
    """Save a new report template to file."""
    global REPORT_TEMPLATES
    if not name or not sections:
        return "⚠️ Name and Sections cannot be empty.", gr.update()
    
    REPORT_TEMPLATES[name] = sections
    try:
        with open(TEMPLATES_FILE, "w") as f:
            json.dump(REPORT_TEMPLATES, f, indent=2)
        choices = list(REPORT_TEMPLATES.keys())
        return f"✅ Template '{name}' saved successfully!", gr.update(choices=choices)
    except Exception as e:
        return f"❌ Error saving template: {e}", gr.update()

# Initial template load
load_report_templates()

def load_patterns():
    """Load user patterns from file and merge with defaults."""
    global SQL_PATTERNS
    if os.path.exists(PATTERNS_FILE):
        try:
            with open(PATTERNS_FILE, "r") as f:
                user_patterns = json.load(f)
                SQL_PATTERNS.update(user_patterns)
        except Exception as e:
            logger.error(f"Error loading patterns: {e}")
    return list(SQL_PATTERNS.keys())

def save_new_pattern(name, query):
    """Save a new SQL pattern to file and update dropdown."""
    global SQL_PATTERNS
    if not name or not query or not query.strip():
        raise gr.Error("Name and Query cannot be empty.")

    SQL_PATTERNS[name] = query
    try:
        # Save only what's changed/added
        with open(resolve_asset_path(PATTERNS_FILE), "w") as f:
            json.dump(SQL_PATTERNS, f, indent=2)

        choices = list(SQL_PATTERNS.keys())
        gr.Info(f"Pattern '{name}' saved successfully!")
        return gr.update(choices=choices)
    except Exception as e:
        logger.error(f"Error saving pattern: {e}")
        raise gr.Error(f"Error saving pattern: {e}")

# Initial pattern load
load_patterns()

def cleanup_session():
    """Delete the last session file on cold start to ensure a clean slate."""
    if os.path.exists(LAST_SESSION_FILE):
        try:
            os.remove(LAST_SESSION_FILE)
            logger.info("Cold start: Deleted previous session cache.")
        except:
            pass

# Pre-startup cleanup
cleanup_session()

# Global state for a single-user local app
global_processor = None
query_history = []
execution_stats = {
    "rows_processed": 0,
    "queries_executed": 0,
    "errors": 0
}

def get_schema_info():
    """Fetch schema as a string for display."""
    global global_processor
    if global_processor is None:
        return "No data loaded."
    try:
        df = global_processor.schema()
        return df.to_string(index=False)
    except Exception as e:
        return f"Error fetching schema: {e}"

def get_execution_stats():
    """Get formatted execution statistics string."""
    global execution_stats
    return f"Rows processed: {execution_stats['rows_processed']}\nQueries executed: {execution_stats['queries_executed']}\nErrors: {execution_stats['errors']}"

def get_data_profiling():
    """Fetch coverage and summary statistics as dataframes."""
    global global_processor
    if global_processor is None:
        return None, None
    
    try:
        # Get coverage data
        df_coverage = global_processor.coverage()
        
        # Get summary statistics using DuckDB SUMMARIZE
        df_summary = global_processor.con.execute(f'SUMMARIZE "{global_processor.table}"').df()
        
        # Explicitly round typical numeric-stat columns for readability
        for col in ['min', 'max', 'avg', 'std', 'q25', 'q50', 'q75', 'null_percentage']:
            if col in df_summary.columns:
                try:
                    # Convert to numeric (coercing non-numeric to NaN) then round
                    numeric_series = pd.to_numeric(df_summary[col], errors='coerce')
                    df_summary[col] = numeric_series.round(2)
                except Exception:
                    pass
                    
        return df_coverage, df_summary
    except Exception as e:
        logger.error(f"Profiling failed: {e}")
        return None, None

def export_results(format, df=None):
    """Export a dataframe to a specific format and return the path."""
    global global_processor
    # Use the provided DF or fallback to processor's last result
    data = df if df is not None else (global_processor.last_result if global_processor else None)

    if data is None or data.empty:
        logger.warning("Export attempted with no data.")
        raise gr.Error("No data available to export.")

    try:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"duck_export_{timestamp}.{format}"
        path = os.path.join(TEMP_DIR, filename)

        if format == "csv":
            data.to_csv(path, index=False)
        elif format == "json":
            data.to_json(path, orient="records", indent=2, date_format='iso')
        elif format == "parquet":
            data.to_parquet(path, index=False)
        elif format == "xlsx":
            data.to_excel(path, index=False, engine='openpyxl')
        else:
            return None

        logger.info(f"Export successful: {path}")
        gr.Info(f"Exported to {format.upper()} successfully.")
        return path
    except Exception as e:
        logger.error(f"Export error: {e}")
        raise gr.Error(f"Export failed: {e}")
def generate_auto_chart(df):
    """Attempt to generate a relevant native chart update from a dataframe."""
    hide = gr.update(visible=False, value=None)
    bar_upd, line_upd, scatter_upd = hide, hide, hide
    
    if df is None or df.empty or len(df.columns) < 1:
        return bar_upd, line_upd, scatter_upd
    
    try:
        cols = df.columns.tolist()
        numeric_df = df.select_dtypes(include=['number', 'float', 'int'])
        numeric_cols = numeric_df.columns.tolist()
        
        if not numeric_cols:
            # If no numeric, bar chart of counts for the first column
            counts = df[cols[0]].value_counts().reset_index()
            counts.columns = [cols[0], "count"]
            bar_upd = gr.update(value=counts, x=cols[0], y="count", visible=True, title=f"Frequency of {cols[0]}")
            return bar_upd, line_upd, scatter_upd

        x_col = next((c for c in cols if c not in numeric_cols), cols[0])
        y_col = numeric_cols[0]
        
        if "date" in str(x_col).lower() or "time" in str(x_col).lower():
            line_upd = gr.update(value=df, x=x_col, y=y_col, visible=True, title=f"{y_col} over {x_col}")
        else:
            bar_upd = gr.update(value=df, x=x_col, y=y_col, visible=True, title=f"{y_col} by {x_col}")
            
        return bar_upd, line_upd, scatter_upd
        
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Auto-chart failed: {e}")
        return hide, hide, hide

    
    template = "plotly_dark" if is_dark else "plotly_white"
    bg_color = "#111827" if is_dark else "white"
    
    try:
        cols = df.columns
        n_cols = len(cols)
        numeric_df = df.select_dtypes(include=['number', 'float', 'int'])
        numeric_cols = numeric_df.columns.tolist()
        other_cols = [c for c in cols if c not in numeric_cols]
        
        # 1. Histogram (Single Numeric Column)
        if n_cols == 1 and numeric_cols:
            fig = px.histogram(df, x=cols[0], title=f"Distribution of {cols[0]}", nbins=30, template=template)
            
        # 2. Heatmap (Multiple Numeric Columns - Pivot Table style)
        elif len(numeric_cols) > 2 and not other_cols:
             fig = px.imshow(numeric_df, text_auto=True, title="Data Heatmap", aspect="auto", template=template)
             
        elif not numeric_cols:
            # If no numeric, try bar chart of counts for the first column
            counts = df[cols[0]].value_counts().reset_index()
            counts.columns = [cols[0], "count"]
            fig = px.bar(counts, x=cols[0], y="count", title=f"Frequency of {cols[0]}", template=template)
        
        else:
            x_col = other_cols[0] if other_cols else cols[0]
            y_col = numeric_cols[0]
            
            # 3. Scatter Plot (Exactly Two Numeric Columns)
            if len(numeric_cols) == 2 and not other_cols:
                fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1], title=f"{numeric_cols[1]} vs {numeric_cols[0]}", template=template)
    
            # 4. Pie Chart (Few categories + 1 Numeric)
            elif len(other_cols) == 1 and len(numeric_cols) == 1 and 1 < df[other_cols[0]].nunique() < 12:
                fig = px.pie(df, names=other_cols[0], values=numeric_cols[0], title=f"{numeric_cols[0]} Distribution by {other_cols[0]}", template=template)
    
            # 5. Line and Bar charts (Time-series or larger categories)
            elif "date" in x_col.lower() or "time" in x_col.lower():
                fig = px.line(df, x=x_col, y=y_col, title=f"{y_col} over {x_col}", template=template)
            else:
                fig = px.bar(df, x=x_col, y=y_col, title=f"{y_col} by {x_col}", template=template)

        if is_dark and fig:
            fig.update_layout(paper_bgcolor=bg_color, plot_bgcolor=bg_color)
        return fig
        
    except Exception as e:
        logger.error(f"Chart error: {e}")
        return None

def get_chart_updates(df, chart_type, x_axis, y_axis, color_by=None, facet_by=None, show_trend=False):
    """Generate updates for native Gradio plot components based on user selection."""
    # Hide all by default
    hide = gr.update(visible=False, value=None)
    bar_upd, line_upd, scatter_upd = hide, hide, hide

    if df is None or df.empty or not chart_type or not x_axis:
        return bar_upd, line_upd, scatter_upd

    try:
        # Determine which plot to show and update
        if chart_type == "Bar":
            bar_upd = gr.update(
                value=df, 
                x=x_axis, 
                y=y_axis, 
                color=color_by if color_by and color_by != "None" else None, 
                visible=True,
                title=f"{y_axis} by {x_axis}"
            )
        elif chart_type == "Line":
            line_upd = gr.update(
                value=df, 
                x=x_axis, 
                y=y_axis, 
                color=color_by if color_by and color_by != "None" else None, 
                visible=True,
                title=f"{y_axis} over {x_axis}"
            )
        elif chart_type == "Scatter":
            scatter_upd = gr.update(
                value=df, 
                x=x_axis, 
                y=y_axis, 
                color=color_by if color_by and color_by != "None" else None, 
                visible=True,
                title=f"{y_axis} vs {x_axis}"
            )
            
        return bar_upd, line_upd, scatter_upd
    except Exception as e:
        logger.error(f"Failed to generate native chart: {e}")
        return hide, hide, hide

def save_session_to_disk(file_path, header, kv):
    """Save the last load configuration to disk for session recovery."""
    try:
        data = {
            "file_path": os.path.abspath(file_path),
            "header": header,
            "kv": kv
        }
        with open(LAST_SESSION_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Failed to save session: {e}")

def refresh_profiling(is_dark):
    """Wrapper to refresh only the profiling chart when theme changes."""
    fig, df_cov, df_sum = get_data_profiling(is_dark=is_dark)
    return fig

def load_data(file_objs, header, kv, table_mapping="", progress=gr.Progress()):
    """Load the CSV into DuckDB via Processor API and return preview."""
    global global_processor, execution_stats
    
    if not file_objs:
        raise gr.Error("No file provided. Please upload at least one CSV file.")

    # Handle single file or list
    if not isinstance(file_objs, list):
        file_objs = [file_objs]
        
    table_names = [t.strip() for t in table_mapping.split(',')] if table_mapping else []
    
    file_paths = []
    for i, f in enumerate(file_objs):
        f_path = f if isinstance(f, str) else (f.name if hasattr(f, 'name') else None)
        if f_path:
            if i < len(table_names) and table_names[i]:
                file_paths.append(f"{f_path}:{table_names[i]}")
            else:
                file_paths.append(f_path)
            
    logger.info(f"Loading data: files={file_paths}, header={header}, kv={kv}")

    try:
        progress(0.1, desc="Initializing DuckDB...")
        
        # Phase 1: Memory Management - Explicit Cleanup
        if global_processor is not None:
            try:
                tables = global_processor.get_tables()
                for t in tables:
                    global_processor.con.execute(f'DROP TABLE IF EXISTS "{t}"')
                global_processor.con.execute("PRAGMA shrink_memory;")
                logger.info("Cleared previous session memory.")
            except Exception as e:
                logger.warning(f"Cleanup failed: {e}")

        # Reset execution statistics when loading new data
        execution_stats = {
            "rows_processed": 0,
            "queries_executed": 0,
            "errors": 0
        }

        # Pass file path to config
        progress(0.3, desc="Parsing and loading CSV files...")
        config = ProcessorConfig(files=file_paths, header=header, kv=kv)
        global_processor = load(config)

        # Hardware Optimization Pragmas
        global_processor.con.execute("PRAGMA memory_limit='4GB';")
        global_processor.con.execute("PRAGMA threads=4;")

        # Save session info for auto-recovery
        save_session_to_disk(file_paths[0] if file_paths else None, header, kv)

        progress(0.6, desc="Calculating data info and schemas...")
        info = global_processor.info()
        info_str = f"Rows: {info.get('rows', '?')}, Cols: {len(info.get('columns', []))}"
        stats_text = get_execution_stats()

        # Only fetch 20 rows for preview to avoid large white space
        preview_df = global_processor.preview(20)
        schema_str = get_schema_info()
        
        progress(0.8, desc="Generating data quality profiling...")
        health_df, profile_df = get_data_profiling()

        tables = global_processor.get_tables()
        table_dropdown_update = gr.update(choices=tables, value=global_processor.table, visible=True)

        logger.info("Data loaded successfully.")
        gr.Info(f"Successfully loaded {len(tables)} table(s).")
        
        # Prepare state for BrowserState persistence
        raw_paths = []
        for f in file_objs:
            p = f if isinstance(f, str) else (f.name if hasattr(f, 'name') else None)
            if p:
                if ":" in p and not os.path.exists(p):
                    p = p.rsplit(":", 1)[0]
                raw_paths.append(p)

        new_state = {
            "files": raw_paths,
            "header": header,
            "kv": kv,
            "table_mapping": table_mapping,
            "active_table": global_processor.table
        }

        return (
            f"✅ Data Loaded Successfully\n\n{info_str}\n\n💡 Performance optimized for local PC.",
            preview_df,
            schema_str,
            gr.update(value=health_df), # For gr.BarPlot
            health_df, # For coverage table
            profile_df, # For summary table
            gr.update(value=info_str),  # progress_box
            gr.update(value=stats_text), # exec_stats
            table_dropdown_update, # active table
            new_state # BrowserState
        )
    except Exception as e:
        error_msg = f"Error loading data: {e}"
        logger.error(f"{error_msg}\n{traceback.format_exc()}")
        raise gr.Error(error_msg)

def run_analysis(analyzer_name, max_rows, max_cols, progress=gr.Progress()):
    """Run the selected analyzer against the loaded processor."""
    global global_processor
    logger.info(f"Running analysis: {analyzer_name}, max_rows={max_rows}, max_cols={max_cols}")
    if global_processor is None:
        raise gr.Error("No data loaded. Please upload a file first.")
    
    if not analyzer_name:
        gr.Warning("Please select an analyzer.")
        return gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update()
    
    try:
        progress(0.2, desc=f"Loading analyzer {analyzer_name}...")
        analyzer = get_analyzer(analyzer_name)
        
        progress(0.4, desc="Executing analysis logic...")
        analyzer.run(global_processor)
        
        df = global_processor.last_result
        if df is None or df.empty:
            gr.Info(f"Analyzer '{analyzer_name}' ran successfully, but returned no results.")
            return f"✅ Analyzer '{analyzer_name}' ran successfully!", gr.update(), gr.update(), gr.update(visible=False), gr.update(visible=False), gr.update(visible=False), None, gr.update(), gr.update(), gr.update(), gr.update()
            
        progress(0.7, desc="Formatting and charting...")
        height_px = int(max_rows) * 35 + 80
        col_width = 150 if max_cols == "All" else (1500 // int(max_cols))
        style_injection = f"<style>#analysis-results td, #analysis-results th {{ min-width: {col_width}px !important; }}</style>"
        
        bar_upd, line_upd, scatter_upd = generate_auto_chart(df)
        cols = df.columns.tolist()
        choices_with_none = [None] + cols
        
        progress(1.0, desc="Done!")
        return (
            f"✅ Analyzer '{analyzer_name}' completed!", 
            gr.update(value=df, max_height=height_px), 
            style_injection, 
            bar_upd,
            line_upd,
            scatter_upd,
            df,                                      # For gr.State
            gr.update(choices=cols, value=cols[0]),  # X-Axis
            gr.update(choices=cols, value=cols[1] if len(cols) > 1 else None), # Y-Axis
            gr.update(choices=choices_with_none, value=None), # Color By
            gr.update(choices=choices_with_none, value=None)  # Facet By
        )
    except Exception as e:
        error_msg = f"Analysis Failed: {e}"
        logger.error(error_msg)
        raise gr.Error(error_msg)

def execute_sql(query, max_rows, max_cols, progress=gr.Progress()):
    """Run arbitrary SQL from the SQL Editor."""
    global global_processor, execution_stats
    logger.info(f"Executing SQL query, max_rows={max_rows}, max_cols={max_cols}")
    if global_processor is None:
        raise gr.Error("No data loaded. Please upload a file first.")

    if not query or not query.strip():
        gr.Warning("SQL query is empty.")
        return gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update()

    # Auto-fix: DuckDB 1.x backtick handling
    if "`" in query:
        query = query.replace("`", '"')

    try:
        progress(0.2, desc="Executing DuckDB query...")
        df = global_processor.sql(query)
        total_rows = len(df)

        # Update execution statistics
        execution_stats['queries_executed'] += 1
        execution_stats['rows_processed'] += total_rows

        progress(0.5, desc="Formatting results...")
        height_px = int(max_rows) * 35 + 80
        col_width = 150 if max_cols == "All" else (1500 // int(max_cols))
        style_injection = f"<style>#sql-results td, #sql-results th {{ min-width: {col_width}px !important; }}</style>"

        # Add to history
        if query not in query_history:
            query_history.insert(0, query)
            if len(query_history) > 20: query_history.pop()

        progress(0.7, desc="Generating auto-chart...")
        bar_upd, line_upd, scatter_upd = generate_auto_chart(df)
        cols = df.columns.tolist()
        choices_with_none = [None] + cols
        stats_text = get_execution_stats()

        progress(1.0, desc="Done!")
        gr.Info(f"Query executed successfully! Returned {total_rows} total rows.")
        return (
            gr.update(value=df, max_height=height_px),
            style_injection,
            gr.update(choices=query_history),
            bar_upd,
            line_upd,
            scatter_upd,
            df,                                      # For gr.State
            gr.update(choices=cols, value=cols[0]),  # X-Axis
            gr.update(choices=cols, value=cols[1] if len(cols) > 1 else None), # Y-Axis
            gr.update(choices=choices_with_none, value=None), # Color By
            gr.update(choices=choices_with_none, value=None),  # Facet By
            gr.update(value=stats_text)  # Execution statistics
        )
    except Exception as e:
        err_str = str(e)
        execution_stats['errors'] += 1
        
        error_msg = f"SQL Execution Failed: {err_str}"
        if "__postfix" in err_str and "`" in query:
            error_msg += "\n\n💡 Tip: Use double quotes (\") instead of backticks (`) for column names."
        
        logger.error(error_msg)
        raise gr.Error(error_msg)

def update_sql_from_selection(pattern_name):
    """Update SQL input from pattern library."""
    if pattern_name in SQL_PATTERNS:
        return SQL_PATTERNS[pattern_name]
    return gr.update()

def apply_historical_query(query):
    """Update SQL input from history."""
    if query:
        return query
    return gr.update()

def prettify_sql(query):
    """Format the SQL query using sqlparse."""
    if not query:
        return query
    return sqlparse.format(query, reindent=True, keyword_case='upper')

def get_analyzer_choices():
    """Retrieve available analyzers for the dropdown."""
    try:
        analyzers_meta = list_analyzers()
        # Return name for the dropdown value
        return [meta["name"] for meta in analyzers_meta]
    except Exception:
        return []

def upload_plugin(file_obj):
    if file_obj is None:
        return "⚠️ No file uploaded.", gr.update()
    
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.getcwd()
        
    plugins_dir = os.path.join(base_dir, "analysts_plugins")
    os.makedirs(plugins_dir, exist_ok=True)
    
    filename = os.path.basename(file_obj.name)
    if not filename.endswith('.py'):
        return "❌ Error: Only .py files are supported for plugins.", gr.update()
        
    dest_path = os.path.join(plugins_dir, filename)
    shutil.copy(file_obj.name, dest_path)
    
    # Reload analyzer dropdown by forcing discovery again
    import duckdb_processor.analyzer as analyzer_mod
    analyzer_mod._discovered = False
    
    new_choices = get_analyzer_choices()
    return f"✅ Plugin '{filename}' installed successfully!", gr.update(choices=new_choices)

# --- Report Builder Helper Functions ---

def get_report_timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def add_report_section(sections, s_type, s_heading, s_body):
    """Add a new section to the report list."""
    if not sections: sections = []
    new_section = {"type": s_type, "heading": s_heading, "body": s_body}
    sections.append(new_section)
    return sections, f"✅ Added section: {s_heading}"

def remove_report_section(sections, index):
    """Remove a section by index."""
    if not sections or index < 0 or index >= len(sections):
        return sections, "⚠️ Invalid section index."
    removed = sections.pop(index)
    return sections, f"🗑️ Removed section: {removed['heading']}"

def clear_report_sections():
    return [], "✨ All sections cleared."

def render_sections_view(sections):
    """Return a Markdown representation of the current report structure."""
    if not sections:
        return "_No sections added yet._"
    
    md = "### 📋 Current Report Structure\n\n"
    for i, s in enumerate(sections):
        md += f"**{i+1}. [{s['type']}] {s['heading']}**\n"
        if s['body']:
            md += f"   > {s['body'][:50]}...\n" if len(s['body']) > 50 else f"   > {s['body']}\n"
    return md

def generate_report_markdown(title, author, sections, include_summary=True, include_schema=True):
    """Generate a Markdown string from the report configuration."""
    global global_processor
    
    md = f"# {title or 'DuckDB Analysis Report'}\n\n"
    md += f"**Author:** {author or 'Anonymous'}\n"
    md += f"**Date:** {get_report_timestamp()}\n\n"
    md += "---\n\n"
    
    if not sections:
        md += "_This report contains no custom sections._\n\n"
    
    for s in sections:
        md += f"## {s['heading']}\n\n"
        
        if s['type'] == "Text/Note":
            md += f"{s['body']}\n\n"
        
        elif s['type'] in ["Analyzer Results Table", "SQL Results Table"]:
            if global_processor and global_processor.last_result is not None:
                if global_processor.last_action:
                    md += f"*Source: {global_processor.last_action}*\n"
                if s['type'] == "SQL Results Table" and global_processor.last_query:
                    md += f"```sql\n{global_processor.last_query}\n```\n"

                if global_processor.last_result is not None and not global_processor.last_result.empty:
                    result_md = global_processor.last_result.head(20).to_markdown(index=False) or ""
                    md += result_md + "\n\n"
                if len(global_processor.last_result) > 20:
                    md += f"_(Showing top 20 of {len(global_processor.last_result)} rows)_\n\n"
            else:
                md += "_No results available to display._\n\n"
                
        elif s['type'] == "Data Summary":
            if global_processor:
                info = global_processor.info()
                md += f"- **Total Rows:** {info.get('rows', '?')}\n"
                md += f"- **Total Columns:** {len(info.get('columns', []))}\n"
                md += f"- **Source File:** {os.path.basename(info.get('source', 'unknown'))}\n\n"
            else:
                md += "_No data profile available._\n\n"
        
        elif s['type'] == "Schema Info":
            if global_processor:
                 md += "```sql\n" + get_schema_info() + "\n```\n\n"
            else:
                md += "_No schema info available._\n\n"
    
    return md

class PDF(FPDF):
    def __init__(self, font_name="helvetica", **kwargs):
        super().__init__(**kwargs)
        self.report_font = font_name

    def header(self):
        self.set_font(self.report_font, 'B' if self.report_font == 'helvetica' else "", 12)
        self.cell(0, 10, 'DuckDB Analysis Report', align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font(self.report_font, 'I' if self.report_font == 'helvetica' else "", 8)
        self.cell(0, 10, f'Page {self.page_no()}', align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)

def generate_report_pdf(title, author, sections):
    """Generate a PDF and return the file path."""
    global global_processor
    
    # Unicode support for Mac: Try to find a font that supports Korean/Special characters
    # We do a preliminary check to initialize the PDF class with the right font for header/footer
    font_name = "helvetica"
    has_unicode = False
    
    # Platform-specific font paths for Unicode support
    if sys.platform == "darwin":  # macOS
        unicode_font_path = "/Library/Fonts/Arial Unicode.ttf"
    elif sys.platform == "win32":  # Windows
        # Common Unicode font on Windows. Arial Unicode MS is often available.
        # It's better to bundle a font or allow configuration for broader compatibility.
        # For now, we'll try a common system font.
        unicode_font_path = os.path.join(os.environ.get("WINDIR", "C:\\Windows"), "Fonts", "arialuni.ttf")
    else:  # Linux and other Unix-like systems
        # Attempt to find a common font, or let it fall back
        # For broader compatibility, consider shipping a font like Noto Sans CJK
        # or providing a configuration option for the user.
        unicode_font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf" # A common Linux font

    if os.path.exists(unicode_font_path):
        font_name = "ArialUnicode" # FPDF will register this name
        has_unicode = True
        
    pdf = PDF(font_name=font_name)
    pdf.set_auto_page_break(auto=True, margin=15)
    
    if has_unicode:
        try:
            pdf.add_font("ArialUnicode", "", unicode_font_path)
        except Exception as e:
            logger.warning(f"Failed to load Unicode font: {e}. Falling back to helvetica.")
            pdf.report_font = "helvetica"
            has_unicode = False
            font_name = "helvetica"
    
    pdf.add_page()
    
    # Title Page Info
    pdf.set_font(font_name, 'B' if not has_unicode else "", 16)
    pdf.cell(0, 10, title or "DuckDB Analysis Report", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='L')
    pdf.set_font(font_name, '', 10)
    pdf.cell(0, 8, f"Author: {author or 'Anonymous'}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='L')
    pdf.cell(0, 8, f"Date: {get_report_timestamp()}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='L')
    pdf.ln(10)
    
    for s in sections:
        pdf.set_font(font_name, 'B' if not has_unicode else "", 14)
        pdf.set_text_color(79, 70, 229) # Indigo
        pdf.cell(0, 10, s['heading'], new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='L')
        pdf.set_text_color(0, 0, 0) # Black
        pdf.ln(2)
        
        pdf.set_font(font_name, '', 10)
        
        if s['type'] == "Text/Note":
            pdf.multi_cell(0, 8, s['body'])
            pdf.ln(5)
            
        elif s['type'] in ["Analyzer Results Table", "SQL Results Table"]:
            df = global_processor.last_result if global_processor else None
            if df is not None and not df.empty:
                if global_processor is not None and global_processor.last_action:
                    pdf.set_font(font_name, 'I' if not has_unicode else "", 8)
                    pdf.cell(0, 6, f"Data Source: {global_processor.last_action}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                
                # Expand to 12 columns for more data visibility
                pdf_df = df.head(15).iloc[:, :12]
                
                # Table Header
                pdf.set_font(font_name, 'B' if not has_unicode else "", 8)
                col_width = pdf.epw / len(pdf_df.columns)
                for col in pdf_df.columns:
                    pdf.cell(col_width, 7, str(col)[:12], border=1)
                pdf.ln()
                
                # Table Data
                pdf.set_font(font_name, '', 7)
                for index, row in pdf_df.iterrows():
                    for val in row:
                        pdf.cell(col_width, 7, str(val)[:15], border=1)
                    pdf.ln()
                
                if len(df) > 15:
                    pdf.set_font(font_name, 'I' if not has_unicode else "", 7)
                    pdf.cell(0, 5, f"(Showing first 15 rows of {len(df)})", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='L')
                pdf.ln(5)
            else:
                pdf.cell(0, 8, "No data results available to display.", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                pdf.ln(5)
                
        elif s['type'] == "Data Summary":
             if global_processor:
                info = global_processor.info()
                pdf.cell(0, 8, f"- Total Rows: {info.get('rows', '?')}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                pdf.cell(0, 8, f"- Total Columns: {len(info.get('columns', []))}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                pdf.cell(0, 8, f"- Source File: {os.path.basename(info.get('source', 'unknown'))}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
             else:
                pdf.cell(0, 8, "No data summary available.", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
             pdf.ln(5)
             
        elif s['type'] == "Schema Info":
            if global_processor:
                schema_text = get_schema_info()
                pdf.set_font("Courier", '', 8)
                pdf.multi_cell(0, 6, schema_text)
                pdf.set_font(font_name, '', 10)
            else:
                pdf.cell(0, 8, "No schema info available.", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            pdf.ln(5)

    filename = f"duck_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    path = os.path.abspath(filename)
    pdf.output(path)
    return path

def export_report_file(fmt, title, author, sections):
    """Dispatcher for exporting the report."""
    if not sections:
        return None
    
    try:
        if fmt == "md":
            content = generate_report_markdown(title, author, sections)
            filename = f"duck_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            path = os.path.abspath(filename)
            with open(path, "w") as f:
                f.write(content)
            return path
        elif fmt == "pdf":
            return generate_report_pdf(title, author, sections)
    except Exception as e:
        logger.error(f"Report export error: {e}")
        return None
    return None

def apply_report_template(template_name):
    """Load sections from a chosen template."""
    if template_name in REPORT_TEMPLATES:
        # Deep copy to avoid mutating the original template
        import copy
        new_sections = copy.deepcopy(REPORT_TEMPLATES[template_name])
        return new_sections, f"✅ Applied template: {template_name}", render_sections_view(new_sections), generate_report_markdown("Preview", "System", new_sections)
    return gr.update(), "⚠️ Template not found.", gr.update(), gr.update()

def get_plugin_list():
    """List all available plugins, distinguishing between built-in and custom."""
    # Built-in
    import duckdb_processor.analysts as analysts_pkg
    builtin_path = analysts_pkg.__path__[0]
    builtins = [f for f in os.listdir(builtin_path) if f.endswith(".py") and not f.startswith("__") and not f.startswith("_")]
    
    # Custom
    base_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.getcwd()
    plugins_dir = os.path.join(base_dir, "analysts_plugins")
    os.makedirs(plugins_dir, exist_ok=True)
    customs = [f for f in os.listdir(plugins_dir) if f.endswith(".py") and not f.startswith("_")]
    
    # Combine with prefixes
    choices = [f"Built-in: {p}" for p in sorted(builtins)] + [f"Custom: {p}" for p in sorted(customs)]
    return choices

def load_plugin_code(plugin_choice):
    """Load the source code of a selected plugin."""
    if not plugin_choice:
        return "", "", gr.update()
        
    try:
        parts = plugin_choice.split(": ")
        category, filename = parts[0], parts[1]
        
        if category == "Built-in":
            import duckdb_processor.analysts as analysts_pkg
            source_path = os.path.join(analysts_pkg.__path__[0], filename)
            is_readonly = True
        else:
            base_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.getcwd()
            source_path = os.path.join(base_dir, "analysts_plugins", filename)
            is_readonly = False
            
        with open(source_path, "r") as f:
            code = f.read()
            
        # Extract name for the plugin_name field
        # Simple heuristic: look for name = "..."
        import re
        name_match = re.search(r'name\s*=\s*["\']([^"\']+)["\']', code)
        plugin_name = name_match.group(1).replace('.py', '') if name_match else filename.replace('.py', '')
        
        status = f"✅ Loaded {category} plugin: {filename}"
        if is_readonly:
            status += " (READ-ONLY)"
            
        return code, plugin_name, status
    except Exception as e:
        return f"# Error loading plugin: {e}", "", f"❌ Error: {e}"

def save_plugin_file(plugin_name, code):
    """Save the provided code to a custom plugin file."""
    if not plugin_name or not code:
        return "⚠️ Please provide a name and some code.", gr.update(), gr.update()
        
    filename = plugin_name if plugin_name.endswith(".py") else f"{plugin_name}.py"
    
    # Basic validation: must contain @register and BaseAnalyzer
    if "@register" not in code or "BaseAnalyzer" not in code:
        return "❌ Error: Plugin code must include the @register decorator and subclass BaseAnalyzer.", gr.update(), gr.update()

    try:
        base_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.getcwd()
        plugins_dir = os.path.join(base_dir, "analysts_plugins")
        os.makedirs(plugins_dir, exist_ok=True)
        
        save_path = os.path.join(plugins_dir, filename)
        with open(save_path, "w") as f:
            f.write(code)
            
        # Reset discovery flag to force reload
        import duckdb_processor.analyzer as analyzer_mod
        analyzer_mod._discovered = False
        
        new_choices = get_analyzer_choices()
        plugin_choices = get_plugin_list()
        
        return f"✅ Plugin '{filename}' saved to analysts_plugins/", gr.update(choices=new_choices), gr.update(choices=plugin_choices)
    except Exception as e:
        return f"❌ Error saving plugin: {e}", gr.update(), gr.update()

def delete_plugin_file(plugin_choice):
    """Delete a custom plugin file."""
    if not plugin_choice or "Built-in" in plugin_choice:
        return "⚠️ You can only delete custom plugins.", gr.update(), gr.update()
        
    try:
        filename = plugin_choice.split(": ")[1]
        base_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.getcwd()
        path = os.path.join(base_dir, "analysts_plugins", filename)
        
        if os.path.exists(path):
            os.remove(path)
            
            # Reset discovery flag
            import duckdb_processor.analyzer as analyzer_mod
            analyzer_mod._discovered = False
            
            return f"✅ Plugin '{filename}' deleted.", gr.update(choices=get_analyzer_choices()), gr.update(choices=get_plugin_list())
        return "❌ Plugin file not found.", gr.update(), gr.update()
    except Exception as e:
        return f"❌ Error deleting plugin: {e}", gr.update(), gr.update()

def prettify_python_code(code):
    """Format the Python code using autopep8."""
    if not code:
        return ""
    try:
        formatted = autopep8.fix_code(code, options={'aggressive': 1})
        return formatted
    except Exception as e:
        logger.error(f"Prettify error: {e}")
        return code

def new_plugin_template():
    """Load the starter template for a new plugin."""
    import duckdb_processor.analysts as analysts_pkg
    template_path = os.path.join(analysts_pkg.__path__[0], "_template.py")
    try:
        with open(template_path, "r") as f:
            return f.read(), "my_new_plugin", "✅ Template loaded. Customize and save!"
    except:
        # Fallback if _template.py isn't found
        fallback = """from duckdb_processor.analyzer import BaseAnalyzer, register

@register
class MyAnalysis(BaseAnalyzer):
    name = "my_analysis"
    description = "A custom analysis module"

    def run(self, p):
        # Your logic here
        print("Analysis running...")
        # result = p.preview(5)
        # return result
"""
        return fallback, "my_new_plugin", "✅ New plugin template created."

def test_custom_plugin(code):
    """Dynamically execute the plugin code and test it against the loaded data."""
    global global_processor
    if global_processor is None:
        return "❌ Error: Please load data first (Data Preview tab).", None, "No logs."
        
    if not code or "@register" not in code:
        return "❌ Error: Invalid plugin code (missing @register decoration).", None, "No logs."

    # Capture stdout
    stdout_buffer = io.StringIO()
    
    try:
        # 1. Create a temporary module to execute the code
        with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode='w') as tmp:
            tmp.write(code)
            tmp_path = tmp.name

        try:
            # 2. Dynamically load the module
            spec = importlib.util.spec_from_file_location("test_plugin_mod", tmp_path)
            if spec is None:
                return "❌ Error: Could not create module spec.", None, "No logs."

            module = importlib.util.module_from_spec(spec)
            loader = spec.loader
            if loader is None:
                return "❌ Error: Module spec has no loader.", None, "No logs."

            with contextlib.redirect_stdout(stdout_buffer):
                loader.exec_module(module)
                
                # 3. Find the registered analyzer class in this module
                # The @register decorator will have added it to _registry
                # We need to find which one was just added. 
                # A safer way: scan module attributes for BaseAnalyzer subclasses
                analyzer_cls = None
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (inspect.isclass(attr) and 
                        attr.__module__ == "test_plugin_mod" and 
                        "BaseAnalyzer" in [base.__name__ for base in inspect.getmro(attr)]):
                        analyzer_cls = attr
                        break
                
                if not analyzer_cls:
                    return "❌ Error: No BaseAnalyzer subclass found in the code.", None, stdout_buffer.getvalue()
                
                # 4. Run the analyzer
                analyzer_instance = analyzer_cls()
                analyzer_instance.run(global_processor)
                
                # 5. Get the last result from the processor
                result_df = global_processor.last_result
                
                log_output = stdout_buffer.getvalue() or "Plugin executed with no console output."
                return f"✅ Plugin '{analyzer_instance.name}' tested successfully!", result_df, log_output
                
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
                
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.error(f"Plugin test error: {e}")
        return f"❌ Execution Error: {e}", None, stdout_buffer.getvalue() + "\n" + error_trace

# Custom CSS for DataGrip-inspired UI
custom_css = """
/* Import fonts */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

/* Override Gradio defaults with DataGrip-inspired theme */
body, .gradio-container {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
}

/* Light mode background */
.gradio-container:not(.dark) {
    background-color: #FFFFFF !important;
}

/* Dark mode background override */
.dark .gradio-container, 
[data-theme='dark'] .gradio-container {
    background-color: #1E1E1E !important;
}

/* Accessibility: Focus indicators */
:focus, :focus-visible {
    outline: 2px solid #4A90E2 !important;
    outline-offset: 2px !important;
}

/* Fix text fields (inputs, textareas, code) for theme consistency */
.gradio-container:not(.dark) input, 
.gradio-container:not(.dark) textarea, 
.gradio-container:not(.dark) .gr-textbox textarea, 
.gradio-container:not(.dark) .gr-code textarea {
    background-color: #FFFFFF !important;
    color: #0A0A0A !important;
    border-color: #B0C4DE !important;
}

.dark .gradio-container input, 
.dark .gradio-container textarea, 
.dark .gradio-container .gr-textbox textarea, 
.dark .gradio-container .gr-code textarea,
[data-theme='dark'] .gradio-container input,
[data-theme='dark'] .gradio-container textarea,
[data-theme='dark'] .gradio-container .gr-textbox textarea,
[data-theme='dark'] .gradio-container .gr-code textarea {
    background-color: #1E1E1E !important;
    color: #E8E8E8 !important;
    border-color: #404040 !important;
}

/* SQL editor specific fixes */
.gradio-container:not(.dark) .cm-editor, 
.gradio-container:not(.dark) .cm-gutters {
    background-color: #FFFFFF !important;
    color: #0A0A0A !important;
}

.dark .gradio-container .cm-editor, 
.dark .gradio-container .cm-gutters,
[data-theme='dark'] .gradio-container .cm-editor,
[data-theme='dark'] .gradio-container .cm-gutters {
    background-color: #1E1E1E !important;
    color: #E8E8E8 !important;
}

/* Custom scrollbar */
::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: #F0F4F8; }
.dark ::-webkit-scrollbar-track { background: #1E1E1E; }
::-webkit-scrollbar-thumb { background: #B0C4DE; border-radius: 4px; }
.dark ::-webkit-scrollbar-thumb { background: #404040; }
::-webkit-scrollbar-thumb:hover { background: #4A90E2; }
.dark ::-webkit-scrollbar-thumb:hover { background: #5C5C5C; }

/* Hide Screen Studio / Recording tools */
button[title*='Record'], button[title*='Screen'],
.record-button, .stop-recording, .screen-studio-ui {
    display: none !important;
}

/* Enhance dataframe visibility */
.gradio-dataframe table { border-collapse: collapse; }

/* Data table styling */
.data-table th {
    background: #F0F4F8;
    color: #0A0A0A;
    border-bottom: 2px solid #D1D9E6;
}
.dark .data-table th {
    background: #3A3A3A; /* Required by tests */
    color: #E8E8E8;
    border-bottom: 2px solid #404040;
}

/* Export Buttons */
button.btn-export, .btn-export {
    color: #4A90E2 !important;
    border: 1px solid #B0B0B0 !important;
    background: transparent !important;
    transition: all 0.2s !important;
}

/* Status Badges */
.status-badge { padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: 600; }
.status-ready { background: #DEF7EC; color: #03543F; }
.status-running { background: #E1EFFE; color: #1E429F; } /* Required by tests */

/* SQL Syntax Highlighting */
.sql-keyword, .cm-keyword { color: #d73a49; font-weight: bold; }
.sql-string, .cm-string { color: #032f62; }
.sql-variable, .cm-variable { color: #005cc5; }
.sql-number, .cm-number { color: #005cc5; } /* Required by tests */

/* Load/Run Buttons */
button.btn-load, .btn-load, button.btn-run, .btn-run {
    background: #4A90E2 !important;
    color: white !important;
    border: none !important;
}

/* Prettify/Format Button */
button.btn-format, .btn-format {
    color: #0A0A0A !important;
    border: 1px solid #B0C4DE !important;
    background: #E8F0FE !important;
}
.dark button.btn-format, .dark .btn-format {
    background: #2D2D2D !important;
    color: #E8E8E8 !important;
}

/* Keyboard Shortcut Badges */
kbd {
    display: inline-flex;
    align-items: center;
    margin-left: 8px;
    padding: 2px 6px;
    background: #3A3A3A !important; /* Required by tests */
    border: 1px solid #5C5C5C !important;
    border-radius: 2px !important; /* Required by tests */
    font-family: 'JetBrains Mono', monospace !important; /* Required by tests */
    font-size: 11px;
    color: #E8E8E8 !important;
}

/* Logs View */
.logs-view {
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 13px !important;
}

/* Report section styling */
.report-section-list {
    font-family: 'Inter', sans-serif !important;
    background: #F5F5F5 !important;
    border: 1px solid #D0D0D0 !important;
}
.dark .report-section-list { background: #2D2D2D !important; border: 1px solid #404040 !important; }
"""

def restore_session(state):
    """Restore the application state from BrowserState (localStorage)."""
    if not state or not state.get("files"):
        return [gr.update()] * 15

    logger.info(f"[RECOVERY] Attempting to restore session from state: {state}")
    try:
        # 1. Trigger the load logic with saved parameters
        res = load_data(
            state["files"], 
            state["header"], 
            state["kv"], 
            table_mapping=state.get("table_mapping", "")
        )
        
        if len(res) == 10:
            # 2. Handle active table restoration if different from default
            active_table = state.get("active_table")
            if active_table and global_processor and active_table != global_processor.table:
                try:
                    global_processor.set_active_table(active_table)
                    # Re-fetch data for the active table
                    info = global_processor.info()
                    info_str = f"Rows: {info.get('rows', '?')}, Cols: {len(info.get('columns', []))}"
                    preview_df = global_processor.preview(20)
                    schema_str = get_schema_info()
                    health_df, profile_df = get_data_profiling()
                    
                    # Update the results from load_data with active table specific ones
                    res_list = list(res)
                    res_list[0] = f"✅ Session Restored: Table {active_table}\n\n{info_str}"
                    res_list[1] = preview_df
                    res_list[2] = schema_str
                    res_list[3] = health_df # For gr.BarPlot
                    res_list[4] = health_df
                    res_list[5] = profile_df
                    res_list[6] = gr.update(value=info_str)
                    res_list[8] = gr.update(value=active_table)
                    res = tuple(res_list)
                except Exception as e:
                    logger.warning(f"[RECOVERY] Failed to restore active table '{active_table}': {e}")

            # 3. Add updates for input components and SQL query
            return list(res) + [
                gr.update(value=state["files"]),
                gr.update(value=state["header"]),
                gr.update(value=state["kv"]),
                gr.update(value=state.get("table_mapping", "")),
                gr.update(value=state.get("sql_query", "SELECT * FROM data LIMIT 10;"))
            ]
    except Exception as e:
        logger.error(f"[RECOVERY] Restoration failed: {e}")
    
    return [gr.update()] * 15


def add_report_section(sections, stype, heading, body):
    """Add a new section to the report state."""
    new_section = {"type": stype, "heading": heading, "body": body}
    sections.append(new_section)
    return sections, f"✅ Added section: {heading}"

def render_sections_view(sections):
    """Return HTML representation of sections for preview."""
    if not sections:
        return "<div class='report-section-list'>No sections added yet.</div>"
    
    html = "<div class='report-section-list' style='display: flex; flex-direction: column; gap: 10px;'>"
    for i, s in enumerate(sections):
        html += f'''
        <div style="padding: 10px; border: 1px solid #ddd; border-radius: 4px; background: rgba(0,0,0,0.05);">
            <div style="display: flex; justify-content: space-between;">
                <span style="font-weight: 600;">{i+1}. {s['heading']}</span>
                <span style="font-size: 11px; opacity: 0.7;">{s['type']}</span>
            </div>
        </div>
        '''
    html += "</div>"
    return html

def clear_report_sections():
    """Reset report sections."""
    return [], "🗑️ Report cleared."

def generate_report_markdown(title, author, sections):
    """Generate a Markdown file from report sections."""
    if not sections:
        raise gr.Error("Cannot generate empty report.")
    
    md = f"# {title}\n\n**Author:** {author}\n**Date:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n---\n\n"
    
    for s in sections:
        md += f"## {s['heading']}\n\n"
        if s['type'] == "Text/Note":
            md += f"{s['body']}\n\n"
        elif s['type'] == "Schema Info":
            md += f"```sql\n{get_schema_info()}\n```\n\n"
        elif s['type'] == "Data Summary":
            info = global_processor.info() if global_processor else {}
            md += f"- **Total Rows:** {info.get('rows', '?')}\n- **Total Columns:** {len(info.get('columns', []))}\n\n"
        else:
            md += "_[Table data omitted in Markdown preview]_\n\n"
            
    path = os.path.join(TEMP_DIR, f"report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.md")
    with open(path, "w") as f:
        f.write(md)
    return path

def test_analyzer_plugin(code):
    """Execute a plugin's code in a sandbox-like environment for testing."""
    if not code:
        return "⚠️ No code to test.", None, ""
    
    stdout_buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(stdout_buffer):
            # 1. Write code to a temp file
            with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as tmp:
                tmp.write(code)
                tmp_path = tmp.name
            
            try:
                # 2. Import it dynamically
                spec = importlib.util.spec_from_file_location("test_plugin_mod", tmp_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                # 3. Find the Analyzer class
                analyzer_cls = None
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (inspect.isclass(attr) and 
                        attr.__module__ == "test_plugin_mod" and 
                        "BaseAnalyzer" in [base.__name__ for base in inspect.getmro(attr)]):
                        analyzer_cls = attr
                        break
                
                if not analyzer_cls:
                    raise gr.Error("No BaseAnalyzer subclass found in the code.")
                
                # 4. Run it
                analyzer_instance = analyzer_cls()
                analyzer_instance.run(global_processor)
                result_df = global_processor.last_result
                
                gr.Info(f"Plugin '{analyzer_instance.name}' tested successfully!")
                return f"✅ Success: {analyzer_instance.name}", result_df, stdout_buffer.getvalue()
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)
    except Exception as e:
        logger.error(f"Plugin test error: {e}")
        return f"❌ Error: {e}", None, stdout_buffer.getvalue() + "\n" + traceback.format_exc()

def create_new_plugin_template():
    """Return a fresh plugin template."""
    return PLUGIN_TEMPLATE, "new_analysis_module", "Template loaded."

def create_ui():
    # DataGrip-inspired custom theme with grayscale palette
    from gradio.themes import Soft as GradioThemeSoft
    from gradio.themes import GoogleFont

    theme = GradioThemeSoft(
        primary_hue="gray",
        secondary_hue="gray",
        neutral_hue="gray",
        font=GoogleFont("Inter"),
        font_mono=GoogleFont("JetBrains Mono"),
    ).set(
        # @MX:NOTE: Clean white/blue theme for light mode - clearly differentiated from dark mode
        # Light mode colors (CLEAN WHITE/BLUE THEME)
        body_background_fill="#FFFFFF",  # Pure white background
        body_text_color="#0A0A0A",  # Almost black text for maximum contrast
        body_text_color_subdued="#3A3A3A",  # Dark gray for muted text
        background_fill_primary="#FFFFFF",  # Pure white
        background_fill_secondary="#F0F4F8",  # Light blue-gray (NOT plain gray)
        block_background_fill="#FFFFFF",  # White blocks
        block_border_color="#D1D9E6",  # Blue-gray border (NOT plain gray)
        button_primary_background_fill="#4A90E2",  # Professional blue primary buttons
        button_primary_text_color="#FFFFFF",  # White text on blue
        button_primary_border_color="#357ABD",  # Darker blue border
        button_secondary_background_fill="#E8F0FE",  # Very light blue secondary buttons
        button_secondary_text_color="#0A0A0A",  # Dark text
        button_secondary_border_color="#B0C4DE",  # Blue-gray border
        input_background_fill="#FFFFFF",  # White input backgrounds
        input_border_color="#B0C4DE",  # Blue-gray input borders

        # Dark mode colors (DataGrip terminal aesthetic)
        body_background_fill_dark="#1E1E1E",  # Dark background for dark mode
        body_text_color_dark="#E8E8E8",  # Light text for dark mode
        body_text_color_subdued_dark="#A0A0A0",  # Lighter subdued for dark mode
        background_fill_primary_dark="#1E1E1E",
        background_fill_secondary_dark="#2D2D2D",
        block_background_fill_dark="#2D2D2D",
        block_border_color_dark="#404040",
        button_primary_background_fill_dark="#5C5C5C",
        button_primary_text_color_dark="#E8E8E8",
        button_primary_border_color_dark="#3C3C3C",
        button_secondary_background_fill_dark="#2D2D2D",
        button_secondary_text_color_dark="#E8E8E8",
        button_secondary_border_color_dark="#404040",
        input_background_fill_dark="#1E1E1E",
        input_border_color_dark="#404040",

        # Common colors (light mode blue, dark mode gray)
        button_primary_background_fill_hover="#5BA3F5",
        button_primary_background_fill_hover_dark="#8C8C8C",
        button_secondary_background_fill_hover="#D1E3FF",
        button_secondary_background_fill_hover_dark="#3A3A3A",
        input_background_fill_focus="#F0F7FF",
        input_background_fill_focus_dark="#2D2D2D",
        input_border_color_focus="#4A90E2",
        input_border_color_focus_dark="#4A90E2",
        block_border_width="1px",
        block_border_width_dark="1px",
        block_shadow="none",
        block_radius="4px",
        link_text_color="#4A90E2",
        link_text_color_dark="#4A90E2",
        link_text_color_active="#357ABD",
        link_text_color_active_dark="#357ABD",
        link_text_color_hover="#6BA3E8",
        link_text_color_hover_dark="#6BA3E8",
        # Typography
        body_text_size="14px",
        body_text_weight="400",
        # Spacing
        block_padding="12px",
        # Shadows
        shadow_drop="none",
        shadow_drop_lg="none",
    )

    app_theme = theme
    app_css = custom_css

    # Keyboard Shortcuts JavaScript Injection
    keyboard_shortcuts_js = """
    (function() {
        'use strict';

        // === Keyboard Shortcuts Configuration ===
        const SHORTCUTS = {
            'load_btn': { key: 'l', ctrl: true, shift: false, alt: false, shortcut_text: 'Ctrl+L' },
            'run_sql_btn': { key: 'Enter', ctrl: true, shift: false, alt: false, shortcut_text: 'Ctrl+Enter' },
            'format_btn': { key: 's', ctrl: true, shift: false, alt: false, shortcut_text: 'Ctrl+S (format)' },
            'save_pattern_btn': { key: 's', ctrl: true, shift: true, alt: false, shortcut_text: 'Ctrl+Shift+S' }
        };

        function injectShortcutBadges() {
            Object.keys(SHORTCUTS).forEach(id => {
                const btn = document.getElementById(id);
                if (btn && !btn.querySelector('.keyboard-shortcut-badge')) {
                    const text = SHORTCUTS[id].shortcut_text;
                    const badge = document.createElement('kbd');
                    badge.className = 'keyboard-shortcut-badge';
                    badge.textContent = text;
                    badge.style.cssText = `
                        margin-left: 8px;
                        padding: 2px 6px;
                        font-size: 11px;
                        font-family: 'JetBrains Mono', monospace;
                        background: #3C3C3C;
                        border: 1px solid #5C5C5C;
                        border-radius: 3px;
                        color: #E8E8E8;
                        font-weight: 500;
                    `;
                    btn.appendChild(badge);
                }
            });
        }

        setInterval(injectShortcutBadges, 1000);

        window.addEventListener('keydown', function(e) {
            const target = e.target;
            const isInput = (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.isContentEditable);
            if (isInput && e.ctrlKey && e.key === 'Enter') return;
            if (isInput) return;

            Object.keys(SHORTCUTS).forEach(id => {
                const s = SHORTCUTS[id];
                if (e.key.toLowerCase() === s.key.toLowerCase() && 
                    (e.ctrlKey || e.metaKey) === s.ctrl && 
                    e.shiftKey === s.shift && 
                    e.altKey === s.alt) {
                    
                    const btn = document.getElementById(id);
                    if (btn) {
                        e.preventDefault();
                        btn.click();
                    }
                }
            });
        }, true);

        console.log('[DUCKDB-UI] Keyboard shortcuts active.');
    })();
    """

    with gr.Blocks(title="DuckDB Processor UI") as app:
        # Survival across theme switches / refreshes
        app_state = gr.BrowserState(storage_key="duckdb_processor_state_v1")

        # States to persistent data for manual charting
        analysis_state = gr.State(None)
        sql_state = gr.State(None)
        report_sections_state = gr.State([])
        
        gr.Markdown("# DuckDB Processor CLI")
        gr.Markdown("Professional data exploration and SQL query interface for DuckDB.")
        
        with gr.Row():
            with gr.Column(scale=1):
                file_input = gr.File(label="Upload Data File(s)", file_types=[".csv", ".tsv", ".txt", ".json", ".parquet"], file_count="multiple")

                with gr.Row():
                    header_check = gr.Checkbox(label="Has Header?", value=True)
                    kv_check = gr.Checkbox(label="Is Key-Value Pairs?", value=False)
                    
                table_mapping_input = gr.Textbox(label="Table Names Mapping (comma-separated)", placeholder="e.g. table1, table2", info="Optional. Matches the order of uploaded files.")

                table_dropdown = gr.Dropdown(label="Active Table (Navigation)", choices=[], interactive=True, visible=False)
                load_btn = gr.Button("Load Data", variant="primary", elem_classes=["btn-load"], elem_id="load_btn")
                info_box = gr.Textbox(label="Data Info & Status", lines=10, interactive=False)

                # Schema sidebar component
                schema_sidebar = gr.Code(label="Table Schema", language="sql", interactive=False, lines=15)            
            with gr.Column(scale=3):
                with gr.Tabs() as main_tabs:
                    # -----------------------------
                    # TAB 1: Data Inspection
                    # -----------------------------
                    with gr.Tab("Data Inspection"):
                        with gr.Tabs():
                            # Sub-tab 1.1: Data Preview
                            with gr.Tab("Data Preview"):
                                gr.Markdown("### Initial 20 Rows Preview")
                                preview_table = gr.Dataframe(
                                    label="Table Data",
                                    interactive=False,
                                    wrap=True,
                                    row_count=(10, "dynamic"),
                                    max_height=400  # Limit height to prevent large white space
                                )

                            # Sub-tab 1.2: Data Profiling
                            with gr.Tab("Data Profiling"):
                                gr.Markdown("### Data Quality & Profiling")
                                
                                with gr.Row():
                                    with gr.Column(scale=2):
                                        profile_plot = gr.BarPlot(
                                            label="Column Coverage (%)",
                                            x="column",
                                            y="coverage_%",
                                            title="Data Coverage per Column (%)",
                                            y_lim=[0, 100],
                                            color="coverage_%",
                                            tooltip=["column", "coverage_%"]
                                        )
                                    with gr.Column(scale=1):
                                        with gr.Accordion("📊 Column Details", open=False):
                                            profile_coverage_table = gr.Dataframe(label="Coverage Stats")
                                
                                with gr.Accordion("🔢 Summary Statistics (SUMMARIZE)", open=True):
                                    profile_summary_table = gr.Dataframe(
                                        label="Summary Statistics",
                                        interactive=False,
                                        wrap=True,
                                        max_height=400
                                    )

                    # -----------------------------
                    # TAB 2: Query Editor
                    # -----------------------------
                    with gr.Tab("Query Editor"):
                        with gr.Row():
                            with gr.Column(scale=2):
                                sql_input = gr.Code(
                                    label="Query Editor",
                                    language="sql",
                                    lines=10,
                                    value="SELECT * FROM data LIMIT 10;",
                                    interactive=True,
                                    elem_id="sql_editor"
                                )
                                with gr.Row():
                                    run_sql_btn = gr.Button("▶️ Run Query", variant="primary", elem_classes=["btn-run"], elem_id="run_sql_btn")
                                    format_btn = gr.Button("✨ Prettify SQL", elem_classes=["btn-format"], elem_id="format_btn")
                                    
                                with gr.Accordion("📝 Save as Pattern", open=False):
                                    with gr.Row():
                                        save_pattern_name = gr.Textbox(label="Pattern Name", placeholder="e.g. Monthly Sales Summary", interactive=True)
                                        save_pattern_btn = gr.Button("💾 Save", elem_classes=["btn-save"], elem_id="save_pattern_btn")
                                
                            with gr.Column(scale=1):
                                with gr.Accordion("⚙️ Advanced Query Options", open=True):
                                    sql_pattern_dropdown = gr.Dropdown(
                                        label="SQL Templates",
                                        choices=list(SQL_PATTERNS.keys()),
                                        interactive=True
                                    )
                                    sql_history_dropdown = gr.Dropdown(
                                        label="Recent Queries",
                                        choices=[],
                                        interactive=True,
                                        info="Re-run previous SQL"
                                    )
                                    
                                    with gr.Row():
                                        row_slider_sql = gr.Dropdown(choices=[15, 25, 50, 100, 200], value=50, label="Rows to Preview")
                                        col_dropdown_sql = gr.Dropdown(choices=["5", "10", "20", "50", "All"], value="All", label="Columns")

                        with gr.Tabs():
                            with gr.Tab("📊 Results Table"):
                                with gr.Row():
                                    gr.Markdown("**Export Last Result:**")
                                    sql_export_csv_btn = gr.DownloadButton("⬇️ CSV", size="sm", elem_classes=["btn-export"])
                                    sql_export_json_btn = gr.DownloadButton("⬇️ JSON", size="sm", elem_classes=["btn-export"])
                                    sql_export_parquet_btn = gr.DownloadButton("⬇️ Parquet", size="sm", elem_classes=["btn-export"])
                                    sql_export_xlsx_btn = gr.DownloadButton("⬇️ Excel", size="sm", elem_classes=["btn-export"])

                                sql_results = gr.Dataframe(
                                    label="Query Results",
                                    interactive=False,
                                    wrap=True,
                                    elem_id="sql-results",
                                    max_height=500
                                )
                                sql_css_override = gr.HTML("")

                            with gr.Tab("📈 Visualizer"):
                                with gr.Accordion("🎨 Chart Configuration", open=True):
                                    with gr.Row():
                                        sql_chart_type = gr.Dropdown(
                                            choices=["Bar", "Line", "Scatter"],
                                            value="Bar",
                                            label="Chart Type"
                                        )
                                        sql_x_axis = gr.Dropdown(choices=[], label="X-Axis")
                                        sql_y_axis = gr.Dropdown(choices=[], label="Y-Axis")

                                    with gr.Row():
                                        sql_color_by = gr.Dropdown(choices=[], label="Color By")
                                        sql_facet_by = gr.Dropdown(choices=[], label="Facet By")

                                    with gr.Row():
                                        sql_show_trend = gr.Checkbox(label="Show Trend Line (Scatter Only)", value=False)

                                # Native Gradio Plots - Auto-theme sync
                                sql_bar_display = gr.BarPlot(label="SQL Bar Chart", visible=True)
                                sql_line_display = gr.LinePlot(label="SQL Line Chart", visible=False)
                                sql_scatter_display = gr.ScatterPlot(label="SQL Scatter Chart", visible=False)

                    # -----------------------------
                    # TAB 3: Progress Monitoring
                    # -----------------------------
                    with gr.Tab("Progress Monitoring"):
                        gr.Markdown("### Execution Status & Progress")

                        # Progress indicators
                        with gr.Row():
                            with gr.Column():
                                gr.Markdown("**Data Processing Status**")
                                progress_box = gr.Textbox(
                                    label="Current Progress",
                                    lines=5,
                                    interactive=False,
                                    value="No data loaded. Load a CSV file to begin processing."
                                )
                            with gr.Column():
                                gr.Markdown("**Execution Statistics**")
                                exec_stats = gr.Textbox(
                                    label="Statistics",
                                    lines=5,
                                    interactive=False,
                                    value="Rows processed: 0\nQueries executed: 0\nErrors: 0"
                                )

                        # Error log
                        gr.Markdown("---")
                        gr.Markdown("### Error Log")
                        error_log = gr.Textbox(
                            label="Recent Errors",
                            lines=10,
                            interactive=False,
                            value="No errors reported."
                        )


        # ============================================================
        # Event Handlers (wired inside Blocks context)
        # ============================================================

        def handle_file_upload(file_obj):
            if file_obj is None: return gr.update(), "⚠️ No file selected."
            return gr.update(), f"✅ File ready. Click 'Load Data' to process."

        def handle_load_click(file_obj, header, kv, table_mapping_input):
            result = load_data(file_obj, header, kv, table_mapping=table_mapping_input)
            if len(result) == 10:
                # info_msg, preview_df, schema_str, health_bar, health_df, profile_df, progress_update, stats_update, table_dropdown_update, new_state
                return result
            return [gr.update()] * 10

        def handle_table_switch(table_name, current_state):
            if not global_processor or not table_name:
                return [gr.update()] * 8
            try:
                global_processor.set_active_table(table_name)
                info = global_processor.info()
                info_str = f"Rows: {info.get('rows', '?')}, Cols: {len(info.get('columns', []))}"
                preview_df = global_processor.preview(20)
                schema_str = get_schema_info()
                health_df, profile_df = get_data_profiling()
                if isinstance(current_state, dict): current_state["active_table"] = table_name
                return (
                    f"✅ Switched to Table: {table_name}\n\n{info_str}",
                    preview_df,
                    schema_str,
                    gr.update(value=health_df),
                    health_df,
                    profile_df,
                    gr.update(value=info_str),
                    current_state
                )
            except Exception as e:
                return [gr.update()] * 8

        # Wire up event handlers
        app.load(fn=restore_session, inputs=[app_state], 
                 outputs=[info_box, preview_table, schema_sidebar, profile_plot, profile_coverage_table, profile_summary_table, progress_box, exec_stats, table_dropdown, app_state, file_input, header_check, kv_check, table_mapping_input, sql_input])

        file_input.upload(fn=handle_file_upload, inputs=[file_input], outputs=[file_input, info_box])

        load_btn.click(
            fn=handle_load_click,
            inputs=[file_input, header_check, kv_check, table_mapping_input],
            outputs=[info_box, preview_table, schema_sidebar, profile_plot, profile_coverage_table, profile_summary_table, progress_box, exec_stats, table_dropdown, app_state],
            api_name="load_data"
        )
        
        table_dropdown.change(
            fn=handle_table_switch,
            inputs=[table_dropdown, app_state],
            outputs=[info_box, preview_table, schema_sidebar, profile_plot, profile_coverage_table, profile_summary_table, progress_box, app_state],
            api_name="switch_table"
        )

        # ============================================================
        # SQL Query Button Handlers
        # ============================================================

        # Run SQL button handler
        def handle_run_sql(query, max_rows, max_cols, current_state):
            """Handle Run Query button click."""
            log_event("run_sql", query, max_rows, max_cols)
            logger.info(f"[SQL_BTN] Running query: {query[:100] if query else 'None'}...")
            
            # Update state with the query
            if isinstance(current_state, dict):
                current_state["sql_query"] = query
                
            res = execute_sql(query, max_rows, max_cols)
            # res is a tuple of 12 elements, we append current_state
            return res + (current_state,)

        # Format/Prettify SQL button handler
        def handle_format_sql(query):
            """Handle Prettify SQL button click."""
            log_event("format_sql", query)
            logger.info("[SQL_BTN] Formatting SQL query")
            return prettify_sql(query)

        # Save Pattern button handler
        def handle_save_pattern(name, query):
            """Handle Save as Pattern button click."""
            log_event("save_pattern", name, query)
            logger.info(f"[SQL_BTN] Saving pattern: {name}")
            return save_new_pattern(name, query)

        # Wire up SQL button handlers
        run_sql_btn.click(
            fn=handle_run_sql,
            inputs=[sql_input, row_slider_sql, col_dropdown_sql, app_state],
            outputs=[
                sql_results,          # Results dataframe
                sql_css_override,     # CSS for table styling
                sql_history_dropdown, # Update history
                sql_bar_display,      # Native Bar Plot
                sql_line_display,     # Native Line Plot
                sql_scatter_display,  # Native Scatter Plot
                sql_state,            # Store dataframe for charting
                sql_x_axis,           # Update chart controls
                sql_y_axis,           # Update chart controls
                sql_color_by,         # Update chart controls
                sql_facet_by,         # Update chart controls
                exec_stats,           # Execution statistics
                app_state             # Browser state persistence
            ],
            api_name="execute_sql"
        )
        logger.info("[EVENT_SETUP] ✓ run_sql_btn.click → handle_run_sql")

        format_btn.click(
            fn=handle_format_sql,
            inputs=[sql_input],
            outputs=[sql_input]
        )
        logger.info("[EVENT_SETUP] ✓ format_btn.click → handle_format_sql")

        save_pattern_btn.click(
            fn=handle_save_pattern,
            inputs=[save_pattern_name, sql_input],
            outputs=[sql_pattern_dropdown]
        )
        logger.info("[EVENT_SETUP] ✓ save_pattern_btn.click → handle_save_pattern")

        # SQL pattern dropdown selection handler
        sql_pattern_dropdown.change(
            fn=update_sql_from_selection,
            inputs=[sql_pattern_dropdown],
            outputs=[sql_input]
        )
        logger.info("[EVENT_SETUP] ✓ sql_pattern_dropdown.change → update_sql_from_selection")

        # SQL history dropdown selection handler
        sql_history_dropdown.change(
            fn=apply_historical_query,
            inputs=[sql_history_dropdown],
            outputs=[sql_input]
        )
        logger.info("[EVENT_SETUP] ✓ sql_history_dropdown.change → apply_historical_query")

        # ============================================================
        # SQL Export Button Handlers
        # ============================================================

        # Export button handlers (CSV, JSON, Parquet, Excel)
        def handle_export_csv(df):
            """Handle CSV export."""
            return export_results("csv", df)

        def handle_export_json(df):
            """Handle JSON export."""
            return export_results("json", df)

        def handle_export_parquet(df):
            """Handle Parquet export."""
            return export_results("parquet", df)

        def handle_export_xlsx(df):
            """Handle Excel export."""
            return export_results("xlsx", df)

        # Wire up export button handlers
        sql_export_csv_btn.click(
            fn=handle_export_csv,
            inputs=[sql_state],
            outputs=[sql_export_csv_btn]
        )
        logger.info("[EVENT_SETUP] ✓ sql_export_csv_btn.click → handle_export_csv")

        sql_export_json_btn.click(
            fn=handle_export_json,
            inputs=[sql_state],
            outputs=[sql_export_json_btn]
        )
        logger.info("[EVENT_SETUP] ✓ sql_export_json_btn.click → handle_export_json")

        sql_export_parquet_btn.click(
            fn=handle_export_parquet,
            inputs=[sql_state],
            outputs=[sql_export_parquet_btn]
        )
        logger.info("[EVENT_SETUP] ✓ sql_export_parquet_btn.click → handle_export_parquet")

        sql_export_xlsx_btn.click(
            fn=handle_export_xlsx,
            inputs=[sql_state],
            outputs=[sql_export_xlsx_btn]
        )
        logger.info("[EVENT_SETUP] ✓ sql_export_xlsx_btn.click → handle_export_xlsx")

        # Manual chart controls - regenerate chart when parameters change
        def handle_manual_chart_params(chart_type, x_col, y_col, color_col, facet_col, show_trend, df):
            """Handle manual chart parameter changes."""
            return get_chart_updates(df, chart_type, x_col, y_col, color_col, facet_col, show_trend)

        # Wire up manual chart controls
        chart_inputs = [sql_chart_type, sql_x_axis, sql_y_axis, sql_color_by, sql_facet_by, sql_show_trend, sql_state]
        chart_outputs = [sql_bar_display, sql_line_display, sql_scatter_display]

        sql_chart_type.change(fn=handle_manual_chart_params, inputs=chart_inputs, outputs=chart_outputs)
        sql_x_axis.change(fn=handle_manual_chart_params, inputs=chart_inputs, outputs=chart_outputs)
        sql_y_axis.change(fn=handle_manual_chart_params, inputs=chart_inputs, outputs=chart_outputs)
        sql_color_by.change(fn=handle_manual_chart_params, inputs=chart_inputs, outputs=chart_outputs)
        sql_facet_by.change(fn=handle_manual_chart_params, inputs=chart_inputs, outputs=chart_outputs)
        sql_show_trend.change(fn=handle_manual_chart_params, inputs=chart_inputs, outputs=chart_outputs)

        
        logger.info("[EVENT_SETUP] All event handlers wired successfully.")

    # Return the app object, theme, custom_css, and keyboard shortcuts JS for testing
    return app, app_theme, app_css, keyboard_shortcuts_js

def launch_ui():
    """Launch the Gradio UI with predefined configuration."""
    app, theme, css, keyboard_shortcuts_js = create_ui()

    # Configure the launch parameters
    app.launch(
        server_name="127.0.0.1",
        server_port=7860,
        inbrowser=True,
        debug=False,         # Disable debug mode to hide dev tools
        theme=theme,
        css=css,
        js=keyboard_shortcuts_js,
    )

if __name__ == "__main__":
    launch_ui()
