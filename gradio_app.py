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
from fpdf import FPDF
from fpdf.enums import XPos, YPos

# Set up detailed logging for debugging file loading issues
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)
logger.info("=== DuckDB Processor CLI Starting ===")
logger.info("Debug logging enabled. All events will be logged.")

from duckdb_processor.loader import load
from duckdb_processor.config import ProcessorConfig
from duckdb_processor.analyzer import list_analyzers, get_analyzer

import plotly.express as px
import plotly.graph_objects as go

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
        return "⚠️ Name and Query cannot be empty.", gr.update()
    
    SQL_PATTERNS[name] = query
    try:
        # Save only what's changed/added
        with open(PATTERNS_FILE, "w") as f:
            json.dump(SQL_PATTERNS, f, indent=2)
        choices = list(SQL_PATTERNS.keys())
        return f"✅ Pattern '{name}' saved successfully!", gr.update(choices=choices)
    except Exception as e:
        return f"❌ Error saving pattern: {e}", gr.update()

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

def get_data_profiling(is_dark=False):
    """Fetch coverage and return a Plotly figure."""
    global global_processor
    if global_processor is None:
        return None, "No data loaded.", None
    
    template = "plotly_dark" if is_dark else "plotly_white"
    bg_color = "#111827" if is_dark else "white"
    
    try:
        df = global_processor.coverage()
        fig = px.bar(
            df,
            x="column",
            y="coverage_%",
            title="Data Coverage per Column (%)",
            labels={"coverage_%": "Coverage Percentage", "column": "Column Name"},
            range_y=[0, 100],
            color="coverage_%",
            color_continuous_scale="RdYlGn",
            template=template
        )
        fig.update_layout(showlegend=False)
        if is_dark:
            fig.update_layout(paper_bgcolor=bg_color, plot_bgcolor=bg_color)
        
        # New: Get summary statistics using DuckDB SUMMARIZE
        profile_df = global_processor.con.execute(f'SUMMARIZE "{global_processor.table}"').df()
        
        # Explicitly round typical numeric-stat columns for readability
        for col in ['min', 'max', 'avg', 'std', 'q25', 'q50', 'q75', 'null_percentage']:
            if col in profile_df.columns:
                try:
                    # Convert to numeric (coercing non-numeric to NaN) then round
                    numeric_series = pd.to_numeric(profile_df[col], errors='coerce')
                    profile_df[col] = numeric_series.round(2)  # type: ignore[arg-type]
                except Exception:
                    pass
        
        return fig, df, profile_df
    except Exception as e:
        return None, f"Error calculating metrics: {e}", None

def export_results(format, df=None):
    """Export a dataframe to a specific format and return the path."""
    global global_processor
    # Use the provided DF or fallback to processor's last result
    data = df if df is not None else (global_processor.last_result if global_processor else None)
    
    if data is None or data.empty:
        logger.warning("Export attempted with no data.")
        return None
    
    try:
        filename = f"duck_export_{format}.{format}"
        path = os.path.abspath(filename)
        
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
        return path
    except Exception as e:
        logger.error(f"Export error: {e}")
        return None

def generate_auto_chart(df, is_dark=False):
    """Attempt to generate a relevant chart from a dataframe."""
    if df is None or df.empty:
        return None
    
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

def render_manual_chart(df, chart_type, x_axis, y_axis, color_by=None, facet_by=None, show_trend=False, is_dark=False):
    """Generate a Plotly chart based on manual user selection."""
    if df is None or df.empty:
        return None
    
    # Fallback: if user hasn't selected an axis or chart type yet, 
    # show the auto-chart instead of disappearing.
    if not chart_type or not x_axis:
        return generate_auto_chart(df, is_dark=is_dark)
    
    template = "plotly_dark" if is_dark else "plotly_white"
    bg_color = "#111827" if is_dark else "white"
    
    try:
        # Create a copy so we don't mutate the original state
        df_plot = df.copy()
        
        # Plotly Express needs unique column names - ensure they are unique
        cols = []
        counts = {}
        for c in df_plot.columns:
            counts[c] = counts.get(c, 0) + 1
            if counts[c] > 1:
                cols.append(f"{c}_{counts[c]}")
            else:
                cols.append(c)
        df_plot.columns = cols
        
        # Mapping updated column names back to selected axes
        # (This handles the case where user selects a column that was renamed)
        # Assuming for now that the input names match the first occurrence.
        
        title = f"Manual {chart_type}: {y_axis or 'Count'} by {x_axis}"
        if color_by: title += f" (Color by {color_by})"
        if facet_by: title += f" (Split by {facet_by})"
        
        # Trend line only for Scatter
        trend = "ols" if show_trend and chart_type == "Scatter" else None
        
        # Set up standard kwargs
        kwargs = {
            "title": title,
            "x": x_axis,
            "y": y_axis,
            "color": color_by if color_by and color_by != "None" else None,
            "facet_col": facet_by if facet_by and facet_by != "None" else None,
            "facet_col_wrap": 2 if facet_by else None,
            "template": template
        }
        
        if chart_type == "Pie":
            fig = px.pie(df_plot, names=x_axis, values=y_axis, title=title, template=template)
        elif chart_type == "Histogram":
            kwargs.pop("y", None)
            fig = px.histogram(df_plot, **kwargs)
        else:
            # px.bar, px.line, px.scatter all handled by the same kwargs (in kwargs)
            # but we need to call the right function
            if chart_type == "Bar": 
                fig = px.bar(df_plot, **kwargs)
            elif chart_type == "Line": 
                fig = px.line(df_plot, **kwargs)
            else: 
                if trend == "ols":
                    # Pre-check: OLS requires numeric data
                    try:
                        pd.to_numeric(df_plot[x_axis])
                        if y_axis: pd.to_numeric(df_plot[y_axis])
                    except:
                        raise ValueError("Trend Line (OLS) requires numeric values on both X and Y axes. Please select numeric columns or uncheck 'Show Trend Line'.")
                    kwargs["trendline"] = trend
                fig = px.scatter(df_plot, **kwargs)

        if is_dark:
            fig.update_layout(paper_bgcolor=bg_color, plot_bgcolor=bg_color)
        return fig
        
    except Exception as e:
        error_str = str(e)
        logger.error(f"Manual Chart error: {error_str}")
        # Return a "Visual Error" chart so the user sees the issue on the UI
        fig = go.Figure()
        fig.update_layout(
            xaxis={"visible": False},
            yaxis={"visible": False},
            annotations=[{
                "text": f"⚠️ Chart Error:<br>{error_str}",
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 14, "color": "red"}
            }]
        )
        return fig

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

def load_data(file_objs, header, kv, table_mapping="", is_dark=False):
    """Load the CSV into DuckDB via Processor API and return preview."""
    global global_processor, execution_stats
    
    if not file_objs:
        return "⚠️ No file provided.", None, "No data.", None, None, None, gr.update(), gr.update(), gr.update(visible=False)

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
        # Reset execution statistics when loading new data
        execution_stats = {
            "rows_processed": 0,
            "queries_executed": 0,
            "errors": 0
        }

        # Pass file path to config
        config = ProcessorConfig(files=file_paths, header=header, kv=kv)
        global_processor = load(config)

        # Save session info for auto-recovery
        save_session_to_disk(file_paths[0] if file_paths else None, header, kv)

        info = global_processor.info()
        info_str = f"Rows: {info.get('rows', '?')}, Cols: {len(info.get('columns', []))}"
        stats_text = get_execution_stats()

        # Only fetch 20 rows for preview to avoid large white space
        preview_df = global_processor.preview(20)
        schema_str = get_schema_info()
        health_fig, health_df, profile_df = get_data_profiling(is_dark=is_dark)

        tables = global_processor.get_tables()
        table_dropdown_update = gr.update(choices=tables, value=global_processor.table, visible=True)

        logger.info("Data loaded successfully.")
        
        # Prepare state for BrowserState persistence
        # We store the raw file paths (without mapping suffixes) to avoid duplication on recovery
        raw_paths = []
        for f in file_objs:
            p = f if isinstance(f, str) else (f.name if hasattr(f, 'name') else None)
            if p:
                # If it already has a colon mapping, strip it for storage
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
            f"✅ Data Loaded Successfully\n\n{info_str}\n\n💡 Theme switching is now protected! Your data will persist.",
            preview_df,
            schema_str,
            health_fig,
            health_df,
            profile_df,
            gr.update(value=info_str),  # progress_box
            gr.update(value=stats_text), # exec_stats
            table_dropdown_update, # active table
            new_state # BrowserState
        )
    except Exception as e:
        error_msg = f"❌ Error loading data: {e}"
        logger.error(f"{error_msg}\n{traceback.format_exc()}")
        return error_msg, None, "Error", None, None, None, gr.update(), gr.update(), gr.update(visible=False), gr.update()

def run_analysis(analyzer_name, max_rows, max_cols, is_dark=False):
    """Run the selected analyzer against the loaded processor."""
    global global_processor
    logger.info(f"Running analysis: {analyzer_name}, max_rows={max_rows}, max_cols={max_cols}")
    if global_processor is None:
        return "❌ Error: Please load data first (go to Data Preview tab).", gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update()
    
    if not analyzer_name:
        return "⚠️ Please select an analyzer from the dropdown.", gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update()
    
    try:
        analyzer = get_analyzer(analyzer_name)
        analyzer.run(global_processor)
        
        df = global_processor.last_result
        if df is None or df.empty:
            return f"✅ Analyzer '{analyzer_name}' ran successfully, but returned no results.", gr.update(), gr.update(), None, None, gr.update(), gr.update(), gr.update(), gr.update()
            
        # Calculate dynamic height
        height_px = int(max_rows) * 35 + 80
        
        # Instead of hiding columns, we force min-width via CSS to control visual "width"
        col_width = 150 if max_cols == "All" else (1500 // int(max_cols))
        style_injection = f"<style>#analysis-results td, #analysis-results th {{ min-width: {col_width}px !important; }}</style>"
        
        chart_fig = generate_auto_chart(df, is_dark=is_dark)
        cols = df.columns.tolist()
        choices_with_none = [None] + cols
        
        return (
            f"✅ Analyzer '{analyzer_name}' ran successfully!", 
            gr.update(value=df, max_height=height_px), 
            style_injection, 
            chart_fig,
            df,                                      # For gr.State
            gr.update(choices=cols, value=cols[0]),  # X-Axis
            gr.update(choices=cols, value=cols[1] if len(cols) > 1 else None), # Y-Axis
            gr.update(choices=choices_with_none, value=None), # Color By
            gr.update(choices=choices_with_none, value=None)  # Facet By
        )
    except Exception as e:
        error_msg = f"❌ Error running analyzer: {e}"
        logger.error(error_msg)
        return (
            error_msg, gr.update(), gr.update(), gr.update(), gr.update(), 
            gr.update(), gr.update(), gr.update(), gr.update()
        )

def execute_sql(query, max_rows, max_cols, is_dark=False):
    """Run arbitrary SQL from the SQL Editor."""
    global global_processor, execution_stats
    logger.info(f"Executing SQL query, max_rows={max_rows}, max_cols={max_cols}")
    if global_processor is None:
        return "❌ Error: Please load data first (go to Data Preview tab).", gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update()

    if not query or not query.strip():
        return "⚠️ Query is empty.", gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update()

    # Auto-fix: DuckDB 1.x has a known parser bug where backticks with multi-byte or
    # even some ASCII chars trigger a '__postfix' scalar function error.
    # Standard SQL uses double quotes for identifiers, so we auto-convert backticks.
    if "`" in query:
        logger.info("Auto-converting backticks to double quotes for DuckDB compatibility.")
        query = query.replace("`", '"')

    try:
        df = global_processor.sql(query)
        total_rows = len(df)

        # Update execution statistics
        execution_stats['queries_executed'] += 1
        execution_stats['rows_processed'] += total_rows

        height_px = int(max_rows) * 35 + 80
        col_width = 150 if max_cols == "All" else (1500 // int(max_cols))
        style_injection = f"<style>#sql-results td, #sql-results th {{ min-width: {col_width}px !important; }}</style>"

        # Add to history
        if query not in query_history:
            query_history.insert(0, query)
            if len(query_history) > 20: query_history.pop()

        chart_fig = generate_auto_chart(df, is_dark=is_dark)
        cols = df.columns.tolist()
        choices_with_none = [None] + cols
        stats_text = get_execution_stats()

        return (
            f"✅ Query executed successfully! Returned {total_rows} total rows.",
            gr.update(value=df, max_height=height_px),
            style_injection,
            gr.update(choices=query_history),
            chart_fig,
            df,                                      # For gr.State
            gr.update(choices=cols, value=cols[0]),  # X-Axis
            gr.update(choices=cols, value=cols[1] if len(cols) > 1 else None), # Y-Axis
            gr.update(choices=choices_with_none, value=None), # Color By
            gr.update(choices=choices_with_none, value=None),  # Facet By
            gr.update(value=stats_text)  # Execution statistics
        )
    except Exception as e:
        err_str = str(e)
        error_msg = f"❌ Error executing SQL: {err_str}"

        # Update error count
        execution_stats['errors'] += 1
        stats_text = get_execution_stats()

        # Check for specific DuckDB backtick bug ('__postfix' error)
        if "__postfix" in err_str and "`" in query:
            error_msg += "\n\n💡 Tip: DuckDB's parser often misinterprets MySQL-style backticks (`) as postfix operators. Try using standard double quotes (\") for column names instead!"

        logger.error(error_msg)
        return (
            error_msg, gr.update(), gr.update(), gr.update(), gr.update(), gr.update(),
            gr.update(), gr.update(), gr.update(), gr.update(), gr.update(value=stats_text)
        )

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
    /* Light mode: white background */
    background-color: #FFFFFF !important;
}

/* Dark mode override */
.dark .gradio-container {
    background-color: #1E1E1E !important;
}

/* Gradio dataframe component - Light mode (CLEAN WHITE/BLUE THEME) */
.gradio-container .gr-dataframe table {
    background: #FFFFFF !important;
    color: #0A0A0A !important;
}

.gradio-container .gr-dataframe th {
    background: #F0F4F8 !important; /* Light blue-gray */
    color: #0A0A0A !important;
    border-color: #D1D9E6 !important; /* Blue-gray border */
    font-weight: 600 !important;
}

.gradio-container .gr-dataframe td {
    background: #FFFFFF !important;
    color: #0A0A0A !important;
    border-color: #E8E8E8 !important; /* Very light gray border */
}

.gradio-container .gr-dataframe tr:hover td {
    background: #F7FAFC !important; /* Subtle blue on hover */
}

/* Gradio dataframe - Dark mode (DataGrip DARK THEME) */
.dark .gradio-container .gr-dataframe table {
    background: #1E1E1E !important;
    color: #E8E8E8 !important;
}

.dark .gradio-container .gr-dataframe th {
    background: #2A2A2A !important;
    color: #E8E8E8 !important;
    border-color: #404040 !important;
}

.dark .gradio-container .gr-dataframe td {
    background: #1E1E1E !important;
    color: #E8E8E8 !important;
    border-color: #333333 !important;
}

.dark .gradio-container .gr-dataframe tr:hover td {
    background: #2A2A2A !important;
}

/* Syntax highlighting for SQL editor */
.sql-keyword { color: #569CD6; font-weight: 500; }
.sql-string { color: #CE9178; }
.sql-number { color: #B5CEA8; }
.sql-comment { color: #6A9955; font-style: italic; }
.sql-function { color: #DCDCAA; }

/* Data table styling with DataGrip-inspired aesthetics */
.data-table {
    border-collapse: collapse;
    width: 100%;
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
}

/* Light mode table styling - CLEAN WHITE/BLUE THEME */
.data-table th {
    background: #F0F4F8; /* Light blue-gray */
    color: #0A0A0A; /* Almost black */
    padding: 8px 12px;
    text-align: left;
    font-weight: 600;
    border-bottom: 2px solid #D1D9E6; /* Blue-gray */
    position: sticky;
    top: 0;
}

.data-table td {
    padding: 8px 12px;
    border-bottom: 1px solid #E8E8E8; /* Very light gray */
    color: #0A0A0A; /* Almost black */
    background: #FFFFFF;
}

.data-table tbody tr:hover {
    background: #F7FAFC; /* Subtle blue on hover */
}

/* Dark mode table styling - DataGrip dark look */
.dark .data-table th {
    background: #2A2A2A;
    color: #E8E8E8;
    border-bottom: 2px solid #404040;
}

.dark .data-table td {
    border-bottom: 1px solid #333333;
    color: #E8E8E8;
    background: #1E1E1E;
}

.dark .data-table tbody tr:hover {
    background: #2A2A2A;
}

/* Keyboard shortcut badges with terminal-native styling */
kbd {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 20px;
    height: 20px;
    padding: 0 6px;
    background: #F0F0F0;
    border: 1px solid #D0D0D0;
    border-radius: 2px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    color: #1E1E1E;
    box-shadow: none;
}

/* Dark mode keyboard shortcuts */
.dark kbd {
    background: #3A3A3A;
    border: 1px solid #404040;
    color: #E8E8E8;
    box-shadow: none;
}

/* Status badge with DataGrip-inspired colors */
.status-badge {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 2px 8px;
    border-radius: 2px;
    font-size: 11px;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.status-badge::before {
    content: '';
    width: 6px;
    height: 6px;
    border-radius: 50%;
}

.status-ready {
    background: rgba(76, 175, 80, 0.1);
    color: #4CAF50;
}

.status-ready::before {
    background: #4CAF50;
}

.status-running {
    background: rgba(33, 150, 243, 0.1);
    color: #2196F3;
}

.status-running::before {
    background: #2196F3;
    animation: pulse 1.5s infinite;
}

@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
}

/* Focus indicators for accessibility - 2px minimum */
*:focus-visible {
    outline: 2px solid #4A90E2 !important;
    outline-offset: 2px !important;
}

/* Remove default outline for mouse users */
*:focus:not(:focus-visible) {
    outline: none;
}

/* Loading spinner */
.loading-spinner {
    display: inline-block;
    width: 16px;
    height: 16px;
    border: 2px solid #404040;
    border-top-color: #4A90E2;
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
}

@keyframes spin {
    to { transform: rotate(360deg); }
}

/* Custom scrollbar for terminal-native feel */
::-webkit-scrollbar {
    width: 8px;
    height: 8px;
}

::-webkit-scrollbar-track {
    background: #F0F4F8; /* Light blue-gray for light mode */
}

.dark ::-webkit-scrollbar-track {
    background: #1E1E1E; /* Dark for dark mode */
}

::-webkit-scrollbar-thumb {
    background: #B0C4DE; /* Blue-gray thumb for light mode */
    border-radius: 4px;
}

.dark ::-webkit-scrollbar-thumb {
    background: #404040;
}

::-webkit-scrollbar-thumb:hover {
    background: #4A90E2;
}

.dark ::-webkit-scrollbar-thumb:hover {
    background: #5C5C5C;
}

/* Fix text fields (inputs, textareas, code) for theme consistency */
input, textarea, .gr-textbox textarea, .gr-code textarea {
    background-color: #FFFFFF !important;
    color: #0A0A0A !important;
    border-color: #B0C4DE !important;
}

.dark input, .dark textarea, .dark .gr-textbox textarea, .dark .gr-code textarea {
    background-color: #1E1E1E !important;
    color: #E8E8E8 !important;
    border-color: #404040 !important;
}

/* SQL editor specific fixes */
.cm-editor, .cm-gutters {
    background-color: #FFFFFF !important;
    color: #0A0A0A !important;
}

.dark .cm-editor, .dark .cm-gutters {
    background-color: #1E1E1E !important;
    color: #E8E8E8 !important;
}

/* Hide Screen Studio / Recording tools */
button[title*='Record'], button[title*='Screen'],
.record-button, .stop-recording, .screen-studio-ui {
    display: none !important;
}

/* Enhance dataframe visibility */
.gradio-dataframe table { border-collapse: collapse; }

/* Export Buttons (Light mode) */
button.btn-export, .btn-export {
    color: #4A90E2 !important;
    border: 1px solid #B0B0B0 !important;
    background: transparent !important;
    transition: all 0.2s !important;
    font-weight: 500 !important;
}

button.btn-export:hover, .btn-export:hover {
    background: #4A90E2 !important;
    color: white !important;
    border-color: #4A90E2 !important;
}

/* Dark mode export buttons */
.dark button.btn-export, .dark .btn-export {
    color: #4A90E2 !important;
    border-color: #404040 !important;
    background: transparent !important;
}

.dark .btn-export:hover {
    background: #4A90E2 !important;
    color: white !important;
    border-color: #4A90E2 !important;
}

/* Custom coloring for SQL code blocks */
.cm-s-default .cm-keyword { color: #d73a49; font-weight: bold; }
.cm-s-default .cm-string { color: #032f62; }
.cm-s-default .cm-variable { color: #005cc5; }

/* Load Data Button - Professional Blue (Light mode) */
button.btn-load, .btn-load {
    background: #4A90E2 !important;
    color: white !important;
    border: none !important;
    font-weight: 600 !important;
}
button.btn-load:hover, .btn-load:hover {
    background: #5BA3F5 !important;
    box-shadow: none;
}

/* Run Button - Accent Blue (works in both modes) */
button.btn-run, .btn-run {
    background: #4A90E2 !important;
    color: white !important;
    border: none !important;
    font-weight: 600 !important;
}
button.btn-run:hover, .btn-run:hover {
    background: #5BA3F5 !important;
    box-shadow: none;
}

/* Prettify/Format Button - Blue Tones (Light mode) */
button.btn-format, .btn-format {
    color: #0A0A0A !important;
    border: 1px solid #B0C4DE !important;
    background: #E8F0FE !important;
    transition: all 0.2s !important;
}
button.btn-format:hover, .btn-format:hover {
    background: #D1E3FF !important;
    border-color: #4A90E2 !important;
}
/* Dark Mode Override */
.dark button.btn-format, .dark .btn-format {
    color: #E8E8E8 !important;
    border-color: #5C5C5C !important;
    background: #2D2D2D !important;
}
.dark button.btn-format:hover, .dark .btn-format:hover {
    background: #3C3C3C !important;
    border-color: #8C8C8C !important;
}

/* Save Button - Accent Blue (Light mode) */
button.btn-save, .btn-save {
    color: #4A90E2 !important;
    border: 1px solid #4A90E2 !important;
    background: white !important;
    transition: all 0.2s !important;
}
button.btn-save:hover, .btn-save:hover {
    background: #F0F7FF !important;
    border-color: #5BA3F5 !important;
    color: #5BA3F5 !important;
}
/* Dark Mode Override */
.dark button.btn-save, .dark .btn-save {
    color: #4A90E2 !important;
    border-color: #4A90E2 !important;
    background: #1E1E1E !important;
}
.dark button.btn-save:hover, .dark .btn-save:hover {
    background: #2D2D2D !important;
    border-color: #5BA3F5 !important;
    color: #5BA3F5 !important;
}

/* Test Button - Accent Blue */
button.btn-test, .btn-test {
    background: #4A90E2 !important;
    color: white !important;
    font-weight: 600 !important;
    border: none !important;
}
button.btn-test:hover, .btn-test:hover {
    background: #5BA3F5 !important;
    box-shadow: none;
}

/* Delete Button - Rose/Danger */
button.btn-delete, .btn-delete { color: #e11d48 !important; border: 1px solid #fecdd3 !important; background: #fff1f2 !important; }
button.btn-delete:hover, .btn-delete:hover { background: #f43f5e !important; color: white !important; border-color: #f43f5e !important; }
.dark button.btn-delete, .dark .btn-delete { color: #fb7185 !important; border-color: #881337 !important; background: #4c0519 !important; }
.dark button.btn-delete:hover, .dark .btn-delete:hover { background: #e11d48 !important; color: white !important; }

/* New Button - Indigo */
button.btn-new, .btn-new { color: #4f46e5 !important; border: 1px solid #c7d2fe !important; background: #eef2ff !important; }
button.btn-new:hover, .btn-new:hover { background: #4f46e5 !important; color: white !important; }
.dark button.btn-new, .dark .btn-new { color: #818cf8 !important; border-color: #312e81 !important; background: #1e1b4b !important; }
.dark button.btn-new:hover, .dark .btn-new:hover { background: #4f46e5 !important; color: white !important; }

/* Logs View */
.logs-view { font-family: 'Fira Code', monospace !important; font-size: 13px !important; line-height: 1.4 !important; }

/* Keyboard Shortcut Badges - Blue Tones (Light mode) */
.kbd-shortcut, button kbd, .btn-load kbd, .btn-run kbd, .btn-format kbd {
    display: inline-flex;
    align-items: center;
    margin-left: 8px;
    padding: 2px 6px;
    background: #E8F0FE !important;
    border: 1px solid #B0C4DE !important;
    border-radius: 2px;
    font-family: 'JetBrains Mono', 'Fira Code', Consolas, monospace;
    font-size: 11px;
    font-weight: 500;
    color: #0A0A0A !important;
    box-shadow: none;
    line-height: 1;
    min-width: 24px;
    justify-content: center;
}

.dark .kbd-shortcut, .dark button kbd, .dark .btn-load kbd, .dark .btn-run kbd, .dark .btn-format kbd {
    background: #3A3A3A !important;
    border-color: #404040 !important;
    color: #E8E8E8 !important;
    box-shadow: none;
}

.kbd-shortcut .modifier, button kbd .modifier {
    margin-right: 2px;
    font-size: 10px;
    color: #666666 !important;
}

.dark .kbd-shortcut .modifier, .dark button kbd .modifier {
    color: #A0A0A0 !important;
}

/* Animation for shortcut badge appearance */
@keyframes shortcutFadeIn {
    from { opacity: 0; transform: translateX(-5px); }
    to { opacity: 1; transform: translateX(0); }
}

.kbd-shortcut {
    animation: shortcutFadeIn 0.2s ease-out;
}

/* Logs view with monospace font */
.logs-view {
    font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace !important;
    font-size: 13px !important;
    line-height: 1.4 !important;
}

/* Report section styling */
.report-section-list {
    font-family: 'Inter', sans-serif !important;
    background: #F5F5F5 !important;
    border-radius: 4px !important;
    padding: 12px !important;
    border: 1px solid #D0D0D0 !important;
}

.dark .report-section-list {
    background: #2D2D2D !important;
    border: 1px solid #404040 !important;
}
"""

def restore_session(state, profile_dark, sql_dark):
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
            table_mapping=state.get("table_mapping", ""), 
            is_dark=profile_dark
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
                    health_fig, health_df, profile_df = get_data_profiling(is_dark=profile_dark)
                    
                    # Update the results from load_data with active table specific ones
                    res_list = list(res)
                    res_list[0] = f"✅ Session Restored: Table {active_table}\n\n{info_str}"
                    res_list[1] = preview_df
                    res_list[2] = schema_str
                    res_list[3] = health_fig
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
                gr.update(value=state.get("sql_query", "SELECT * FROM data LIMIT 10"))
            ]
    except Exception as e:
        logger.error(f"[RECOVERY] Restoration failed: {e}")
    
    return [gr.update()] * 15

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

        // === Utility Functions ===

        // Find and click button by id (Gradio sets elem_id as the HTML id)
        function clickButton(elemId) {
            const btn = document.getElementById(elemId);
            if (btn) {
                btn.click();
                return true;
            }

            // Fallback: try elem_id attribute (for older Gradio versions)
            const fallback = document.querySelector(`[elem_id="${elemId}"]`);
            if (fallback) {
                fallback.click();
                return true;
            }

            return false;
        }

        // === Global Keyboard Event Listener ===
        document.addEventListener('keydown', function(event) {
            // Ignore if user is typing in an input field (unless it's a special command)
            const target = event.target;
            const tagName = target.tagName.toLowerCase();
            const isInput = (tagName === 'input' || tagName === 'textarea' || target.isContentEditable);

            // Allow Ctrl+Enter in textareas (submit query)
            if (isInput && event.ctrlKey && event.key === 'Enter') {
                // Let the default behavior happen for SQL editor
                return;
            }

            // Block all other shortcuts when typing
            if (isInput) {
                return;
            }

            // Find matching shortcut
            for (const [elemId, shortcut] of Object.entries(SHORTCUTS)) {
                if (
                    event.key.toLowerCase() === shortcut.key.toLowerCase() &&
                    event.ctrlKey === shortcut.ctrl &&
                    event.shiftKey === shortcut.shift &&
                    event.altKey === shortcut.alt
                ) {
                    event.preventDefault();
                    clickButton(elemId);
                    return;
                }
            }
        });

        // === Shortcut Badge Injection ===
        function injectShortcutBadges() {
            const shortcuts = {
                'load_btn': 'Ctrl+L',
                'run_sql_btn': 'Ctrl+Enter',
                'format_btn': 'Ctrl+S',
                'save_pattern_btn': 'Ctrl+Shift+S'
            };

            for (const [elemId, text] of Object.entries(shortcuts)) {
                // Find buttons by id attribute (Gradio sets elem_id as the HTML id)
                const btn = document.getElementById(elemId);

                if (!btn) {
                    // Silently skip - button might not be rendered yet (e.g., in unopened tab)
                    continue;
                }

                // Check if badge already exists
                if (btn.querySelector('.keyboard-shortcut-badge')) {
                    continue;
                }

                // Create badge element
                const badge = document.createElement('kbd');
                badge.className = 'keyboard-shortcut-badge';
                badge.textContent = text;
                badge.style.cssText = `
                    margin-left: 8px;
                    padding: 2px 6px;
                    font-size: 11px;
                    font-family: 'JetBrains Mono', 'Fira Code', Consolas, monospace;
                    background: #3C3C3C;
                    border: 1px solid #5C5C5C;
                    border-radius: 3px;
                    color: #E8E8E8;
                    font-weight: 500;
                `;

                // Append badge to button
                btn.appendChild(badge);
            }
        }

        // Initialize shortcut badges on page load
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', injectShortcutBadges);
        } else {
            injectShortcutBadges();
        }

        // Re-inject badges when Gradio updates the DOM (for dynamic content)
        // Use debounce to avoid excessive calls
        let badgeTimeout;
        const observer = new MutationObserver(function(mutations) {
            clearTimeout(badgeTimeout);
            badgeTimeout = setTimeout(injectShortcutBadges, 100);
        });

        // Start observing immediately
        const mainContainer = document.querySelector('.gradio-container');
        if (mainContainer) {
            observer.observe(mainContainer, {childList: true, subtree: true});
        } else {
            // If container doesn't exist yet, wait for it
            const containerCheck = setInterval(function() {
                const container = document.querySelector('.gradio-container');
                if (container) {
                    clearInterval(containerCheck);
                    observer.observe(container, {childList: true, subtree: true});
                    injectShortcutBadges();
                }
            }, 100);
        }

        // === Theme Switch Confirmation ===
        (function() {
            console.log('[THEME PROTECTION] Script loaded');
            console.log('[THEME PROTECTION] Page theme:', document.body.classList.contains('dark') ? 'dark' : 'light');

            // Add visible indicator that protection is active
            setTimeout(function() {
                const indicator = document.createElement('div');
                indicator.id = 'theme-protection-indicator';
                indicator.style.cssText = `
                    position: fixed;
                    bottom: 10px;
                    right: 10px;
                    padding: 6px 12px;
                    background: rgba(74, 144, 226, 0.9);
                    color: white;
                    border-radius: 4px;
                    font-size: 11px;
                    font-family: sans-serif;
                    z-index: 9998;
                    pointer-events: none;
                    opacity: 0;
                    transition: opacity 0.3s;
                `;
                indicator.textContent = '🛡️ Theme Protection Active';
                document.body.appendChild(indicator);

                // Show briefly to indicate it's loaded
                setTimeout(() => { indicator.style.opacity = '1'; }, 100);
                setTimeout(() => { indicator.style.opacity = '0'; }, 3000);
            }, 1000);

            let hasData = false;

            // Check if data is loaded
            function checkForData() {
                // Check info box for success
                const infoBox = document.querySelector('#info_box textarea, [label="Data Info & Status"] textarea');
                if (infoBox && infoBox.value) {
                    if (infoBox.value.includes('✅') || infoBox.value.includes('Data loaded') ||
                        infoBox.value.includes('rows') || infoBox.value.includes('2120')) {
                        console.log('[THEME PROTECTION] Data detected in info box');
                        return true;
                    }
                }

                // Check preview table
                const previewTable = document.querySelector('[label="Table Data"] table tbody');
                if (previewTable) {
                    const rows = previewTable.querySelectorAll('tr');
                    if (rows.length > 1) {
                        console.log('[THEME PROTECTION] Data detected in table:', rows.length, 'rows');
                        return true;
                    }
                }

                return false;
            }

            // Show confirmation dialog
            function showThemeDialog() {
                console.log('[THEME PROTECTION] Showing confirmation dialog...');

                const overlay = document.createElement('div');
                overlay.id = 'theme-switch-overlay';
                overlay.style.cssText = `
                    position: fixed;
                    top: 0;
                    left: 0;
                    width: 100%;
                    height: 100%;
                    background: rgba(0, 0, 0, 0.7);
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    z-index: 99999;
                `;

                const isDark = document.body.classList.contains('dark');
                const dialog = document.createElement('div');
                dialog.style.cssText = `
                    background: ${isDark ? '#2D2D2D' : '#FFFFFF'};
                    color: ${isDark ? '#E8E8E8' : '#0A0A0A'};
                    padding: 30px;
                    border-radius: 12px;
                    box-shadow: none;
                    max-width: 500px;
                    border: 1px solid ${isDark ? '#404040' : '#D1D9E6'};
                `;

                dialog.innerHTML = `
                    <h2 style="margin: 0 0 20px 0; font-size: 20px; font-weight: 700; color: #f43f5e;">
                        ⚠️ Data Loss Warning
                    </h2>
                    <p style="margin: 0 0 24px 0; line-height: 1.6; font-size: 15px;">
                        <strong>Switching themes will clear all loaded data.</strong><br>
                        This includes your data preview, schema, and any analysis results.<br><br>
                        This action cannot be undone. Are you sure?
                    </p>
                    <div style="display: flex; gap: 16px; justify-content: flex-end;">
                        <button id="theme-cancel-btn" style="
                            padding: 12px 24px;
                            border: 2px solid ${isDark ? '#5C5C5C' : '#D1D5DB'};
                            background: ${isDark ? '#1E1E1E' : '#FFFFFF'};
                            color: ${isDark ? '#E8E8E8' : '#0A0A0A'};
                            border-radius: 6px;
                            cursor: pointer;
                            font-weight: 600;
                            font-size: 14px;
                            transition: all 0.2s;
                        ">Cancel</button>
                        <button id="theme-confirm-btn" style="
                            padding: 12px 24px;
                            border: none;
                            background: #f43f5e;
                            color: white;
                            border-radius: 6px;
                            cursor: pointer;
                            font-weight: 700;
                            font-size: 14px;
                            box-shadow: none;
                            transition: all 0.2s;
                        ">Switch Theme Anyway</button>
                    </div>
                `;

                overlay.appendChild(dialog);
                document.body.appendChild(overlay);

                // Add hover effects
                const cancelBtn = document.getElementById('theme-cancel-btn');
                const confirmBtn = document.getElementById('theme-confirm-btn');

                cancelBtn.onmouseenter = function() {
                    this.style.transform = 'translateY(-1px)';
                };
                cancelBtn.onmouseleave = function() {
                    this.style.transform = 'translateY(0)';
                };

                confirmBtn.onmouseenter = function() {
                    this.style.background = '#e11d48';
                    this.style.transform = 'translateY(-1px)';
                };
                confirmBtn.onmouseleave = function() {
                    this.style.background = '#f43f5e';
                    this.style.transform = 'translateY(0)';
                };

                // Handle clicks
                cancelBtn.onclick = function() {
                    document.body.removeChild(overlay);
                    console.log('[THEME PROTECTION] User cancelled theme switch');
                };

                confirmBtn.onclick = function() {
                    document.body.removeChild(overlay);
                    console.log('[THEME PROTECTION] User confirmed theme switch');
                    window.themeSwitchConfirmed = true;

                    // Find and click theme button
                    setTimeout(function() {
                        const selectors = [
                            'button[aria-label*="theme"]',
                            'button[aria-label*="dark"]',
                            'button[aria-label*="light"]',
                            'button[class*="theme"]'
                        ];

                        for (const sel of selectors) {
                            const btn = document.querySelector(sel);
                            if (btn) {
                                console.log('[THEME PROTECTION] Clicking theme button:', sel);
                                btn.click();
                                break;
                            }
                        }
                    }, 100);
                };

                // Close on overlay click
                overlay.onclick = function(e) {
                    if (e.target === overlay) {
                        document.body.removeChild(overlay);
                        console.log('[THEME PROTECTION] Dialog cancelled');
                    }
                };

                // Close on Escape key
                const escapeHandler = function(e) {
                    if (e.key === 'Escape' && document.body.contains(overlay)) {
                        document.body.removeChild(overlay);
                        document.removeEventListener('keydown', escapeHandler);
                    }
                };
                document.addEventListener('keydown', escapeHandler);
            }

            // Intercept clicks on theme toggle button
            document.addEventListener('click', function(e) {
                const target = e.target;

                // Only look for buttons or links (exclude checkboxes, labels, etc.)
                const btn = target.closest('button') || target.closest('a');
                if (!btn) return;

                // Exclude checkboxes and input elements
                if (target.tagName === 'INPUT' || target.type === 'checkbox') return;

                // Get button properties
                const btnText = (btn.textContent || btn.innerText || '').toLowerCase();
                const btnAria = (btn.getAttribute('aria-label') || '').toLowerCase();
                const btnClass = (btn.className || '').toLowerCase();
                const btnId = (btn.id || '').toLowerCase();

                // Exclude chart-related controls and labels
                if (btnText.includes('chart') ||
                    btnClass.includes('chart') ||
                    btnAria.includes('chart') ||
                    target.closest('.gr-checkbox') ||
                    target.closest('label')) {
                    return;
                }

                // Check for Gradio theme button specifically
                // Gradio's theme toggle is usually in the header with specific classes
                const isGradioThemeButton =
                    btnClass.includes('theme') ||
                    btnAria.includes('theme') ||
                    (btnClass.includes('header') && (btnText.includes('light') || btnText.includes('dark')));

                if (isGradioThemeButton) {
                    console.log('[THEME PROTECTION] Theme button detected!');
                    console.log('[THEME PROTECTION] Text:', btnText);
                    console.log('[THEME PROTECTION] Aria:', btnAria);
                    console.log('[THEME PROTECTION] Class:', btnClass);
                    console.log('[THEME PROTECTION] ID:', btnId);

                    hasData = checkForData();
                    console.log('[THEME PROTECTION] Has data:', hasData);

                    if (hasData && !window.themeSwitchConfirmed) {
                        console.log('[THEME PROTECTION] Blocking theme switch - showing dialog');
                        e.preventDefault();
                        e.stopPropagation();
                        e.stopImmediatePropagation();
                        showThemeDialog();
                        return false;
                    }

                    window.themeSwitchConfirmed = false;
                    console.log('[THEME PROTECTION] Allowing theme switch');
                }
            }, true); // Use capture phase

            console.log('[THEME PROTECTION] Theme protection initialized');
        })();
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
                                    # @MX:NOTE: Defaults to False (follows Gradio theme)
                                    # User can check to force dark charts even in light mode
                                    profile_dark_toggle = gr.Checkbox(label="Dark Mode Charts", value=False)

                                with gr.Row():
                                    with gr.Column(scale=2):
                                        profile_plot = gr.Plot(label="Column Coverage (%)")
                                    with gr.Column(scale=1):
                                        profile_coverage_table = gr.Dataframe(label="Coverage Stats")

                                gr.Markdown("#### Column Statistics (SUMMARIZE)")
                                profile_summary_table = gr.Dataframe(
                                    label="Summary Statistics",
                                    interactive=False,
                                    wrap=True,
                                    max_height=400
                                )

                    # -----------------------------
                    # TAB 2: Query Editor
                    # -----------------------------
                    with gr.Tab("Query Editor") as sql_tab:
                        gr.Markdown("### SQL Query Interface")

                        with gr.Row():
                            with gr.Column(scale=2):
                                sql_pattern_dropdown = gr.Dropdown(
                                    choices=list(SQL_PATTERNS.keys()),
                                    label="SQL Pattern Library",
                                    info="Quick start queries"
                                )
                            with gr.Column(scale=2):
                                sql_history_dropdown = gr.Dropdown(
                                    choices=[],
                                    label="Recent Queries",
                                    info="Re-run previous SQL"
                                )

                        sql_input = gr.Code(
                            language="sql",
                            lines=10,
                            label="Query Editor",
                            value="SELECT * FROM data LIMIT 10;",
                            interactive=True
                        )

                        with gr.Row():
                            run_sql_btn = gr.Button("Run Query", variant="primary", elem_classes=["btn-run"], elem_id="run_sql_btn")
                            format_btn = gr.Button("Prettify SQL", elem_classes=["btn-format"], elem_id="format_btn")

                        with gr.Row():
                            with gr.Column(scale=2):
                                save_pattern_name = gr.Textbox(label="New Pattern Name", placeholder="e.g. My Custom Analysis", interactive=True)
                            with gr.Column(scale=1):
                                save_pattern_btn = gr.Button("Save as Pattern", elem_classes=["btn-save"], elem_id="save_pattern_btn")

                        save_status = gr.Textbox(label="Save Status", lines=1, interactive=False)

                        with gr.Row():
                            row_slider_sql = gr.Dropdown(choices=[15, 25, 50, 100, 200], value=50, label="Rows")
                            col_dropdown_sql = gr.Dropdown(choices=["5", "10", "20", "50", "All"], value="All", label="Cols")

                        sql_status = gr.Textbox(label="Execution Status", lines=1, interactive=False)

                        # Export buttons for SQL
                        with gr.Row():
                            gr.Markdown("**Export Last Result:**")
                            sql_export_csv_btn = gr.Button("CSV", size="sm", elem_classes=["btn-export"], elem_id="sql_export_csv_btn")
                            sql_export_json_btn = gr.Button("JSON", size="sm", elem_classes=["btn-export"], elem_id="sql_export_json_btn")
                            sql_export_parquet_btn = gr.Button("Parquet", size="sm", elem_classes=["btn-export"], elem_id="sql_export_parquet_btn")
                            sql_export_xlsx_btn = gr.Button("Excel", size="sm", elem_classes=["btn-export"], elem_id="sql_export_xlsx_btn")

                        sql_export_download = gr.File(label="Download Exported File", visible=False)

                        sql_results = gr.Dataframe(
                            label="Query Results",
                            interactive=False,
                            wrap=True,
                            elem_id="sql-results",
                            max_height=500
                        )
                        sql_css_override = gr.HTML("")

                        gr.Markdown("---")
                        gr.Markdown("### Visualization Control")
                        with gr.Row():
                            sql_chart_type = gr.Dropdown(
                                choices=["Bar", "Line", "Scatter", "Pie", "Histogram"],
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
                            # @MX:NOTE: Defaults to False (follows Gradio theme)
                            # User can check to force dark charts even in light mode
                            sql_dark_toggle = gr.Checkbox(label="Dark Mode Charts", value=False)

                        sql_chart_display = gr.Plot(label="SQL Chart")

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

        # Debug logging wrapper
        def log_event(event_name, *args, **kwargs):
            """Log Gradio events for debugging."""
            logger.debug(f"[EVENT] {event_name} called with {len(args)} args, {len(kwargs)} kwargs")
            if args and args[0] is not None:
                try:
                    logger.debug(f"[EVENT] {event_name} first arg type: {type(args[0])}, value: {str(args[0])[:100]}")
                except:
                    logger.debug(f"[EVENT] {event_name} first arg: {args[0]}")

        # File upload handler - log and prepare file for loading
        def handle_file_upload(file_obj):
            """Handle file upload with debug logging."""
            log_event("file_upload", file_obj)
            if file_obj is None:
                logger.warning("[FILE_UPLOAD] No file received (None)")
                return gr.update(), "⚠️ No file selected. Please upload a CSV file."
            logger.info(f"[FILE_UPLOAD] File uploaded: {file_obj}")
            return gr.update(), f"✅ File '{file_obj.name if hasattr(file_obj, 'name') else file_obj}' ready. Click 'Load Data' to process."

        # Load button click handler - loads the data
        def handle_load_click(file_obj, header, kv, table_mapping_input):
            """Handle Load Data button click with debug logging."""
            log_event("load_click", file_obj, header, kv)
            logger.info(f"[LOAD_BTN] Loading: file={file_obj}, header={header}, kv={kv}")

            result = load_data(file_obj, header, kv, table_mapping=table_mapping_input, is_dark=False)
            logger.info(f"[LOAD_BTN] Result: type={type(result)}, len={len(result) if hasattr(result, '__len__') else 'N/A'}")

            # Unpack the 10-element tuple from load_data
            if len(result) == 10:
                info_msg, preview_df, schema_str, health_fig, health_df, profile_df, progress_update, stats_update, table_dropdown_update, new_state = result
                logger.info(f"[LOAD_BTN] Info: {info_msg[:50] if info_msg else 'None'}...")
                return (
                    info_msg,           # info_box
                    preview_df,         # preview_table
                    schema_str,         # schema_sidebar
                    health_fig,         # profile_plot
                    health_df,          # profile_coverage_table
                    profile_df,         # profile_summary_table
                    progress_update,    # progress_box
                    stats_update,       # exec_stats
                    table_dropdown_update, # table_dropdown
                    new_state           # app_state
                )
            else:
                logger.error(f"[LOAD_BTN] Unexpected result length: {len(result)}")
                # Add missing updates if needed
                while isinstance(result, tuple) and len(result) < 10:
                    result = result + (gr.update(),)
                if not isinstance(result, tuple):
                    result = (gr.update(),) * 10
                return result

        def handle_table_switch(table_name, current_state, is_dark=False):
            if not global_processor or not table_name:
                return gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update()
            
            try:
                global_processor.set_active_table(table_name)
                info = global_processor.info()
                info_str = f"Rows: {info.get('rows', '?')}, Cols: {len(info.get('columns', []))}"
                stats_text = get_execution_stats()
                
                preview_df = global_processor.preview(20)
                schema_str = get_schema_info()
                health_fig, health_df, profile_df = get_data_profiling(is_dark=is_dark)
                
                # Update state
                if isinstance(current_state, dict):
                    current_state["active_table"] = table_name

                return (
                    f"✅ Switched to Table: {table_name}\n\n{info_str}",
                    preview_df,
                    schema_str,
                    health_fig,
                    health_df,
                    profile_df,
                    gr.update(value=info_str),
                    current_state
                )
            except Exception as e:
                logger.error(f"Error switching table: {e}")
                return gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update()

        # Wire up event handlers
        logger.info("[EVENT_SETUP] Wiring up Gradio event handlers...")

        # Survival restoration on load (theme switch / refresh)
        app.load(
            fn=restore_session,
            inputs=[app_state, profile_dark_toggle, sql_dark_toggle],
            outputs=[info_box, preview_table, schema_sidebar, profile_plot, profile_coverage_table, profile_summary_table, progress_box, exec_stats, table_dropdown, app_state, file_input, header_check, kv_check, table_mapping_input, sql_input]
        )
        logger.info("[EVENT_SETUP] ✓ app.load → restore_session")

        # File upload → update info box
        file_input.upload(
            fn=handle_file_upload,
            inputs=[file_input],
            outputs=[file_input, info_box]
        )
        logger.info("[EVENT_SETUP] ✓ file_input.upload → handle_file_upload")

        # Load button → load data
        load_btn.click(
            fn=handle_load_click,
            inputs=[file_input, header_check, kv_check, table_mapping_input],
            outputs=[info_box, preview_table, schema_sidebar, profile_plot, profile_coverage_table, profile_summary_table, progress_box, exec_stats, table_dropdown, app_state]
        )
        logger.info("[EVENT_SETUP] ✓ load_btn.click → handle_load_click")
        
        table_dropdown.change(
            fn=handle_table_switch,
            inputs=[table_dropdown, app_state, profile_dark_toggle],
            outputs=[info_box, preview_table, schema_sidebar, profile_plot, profile_coverage_table, profile_summary_table, progress_box, app_state]
        )

        logger.info("[EVENT_SETUP] All event handlers wired successfully.")

        # ============================================================
        # Dark Mode Toggle Handlers
        # ============================================================

        # Data Inspection tab - dark mode toggle
        def handle_profile_dark_toggle(is_dark):
            """Regenerate profiling charts with dark/light mode."""
            log_event("profile_dark_toggle", is_dark)
            logger.info(f"[DARK_MODE] Profile charts: dark={is_dark}")

            if global_processor is None:
                logger.warning("[DARK_MODE] No data loaded, skipping chart regeneration")
                return gr.update(), gr.update()

            try:
                health_fig, health_df, profile_df = get_data_profiling(is_dark=is_dark)
                logger.info(f"[DARK_MODE] Regenerated profiling charts with dark={is_dark}")
                return (
                    gr.update(value=health_fig),  # profile_plot
                    gr.update(value=health_df),   # profile_coverage_table
                    gr.update(value=profile_df),  # profile_summary_table
                )
            except Exception as e:
                logger.error(f"[DARK_MODE] Failed to regenerate charts: {e}")
                return gr.update(), gr.update()

        # Query Editor tab - dark mode toggle for SQL charts
        def handle_sql_dark_toggle(is_dark, current_chart, df):
            """Regenerate SQL chart with dark/light mode."""
            log_event("sql_dark_toggle", is_dark, current_chart)
            logger.info(f"[DARK_MODE] SQL chart: dark={is_dark}")

            if df is None or df.empty:
                logger.warning("[DARK_MODE] No data to regenerate chart")
                return gr.update()

            try:
                # Regenerate the auto-chart with the correct dark mode setting
                logger.info(f"[DARK_MODE] Regenerating SQL chart with dark={is_dark}")
                new_chart = generate_auto_chart(df, is_dark=is_dark)
                return gr.update(value=new_chart)
            except Exception as e:
                logger.error(f"[DARK_MODE] Failed to regenerate SQL chart: {e}")
                return gr.update()

        # Wire up dark mode toggle handlers
        profile_dark_toggle.change(
            fn=handle_profile_dark_toggle,
            inputs=[profile_dark_toggle],
            outputs=[profile_plot, profile_coverage_table, profile_summary_table]
        )
        logger.info("[EVENT_SETUP] ✓ profile_dark_toggle.change → handle_profile_dark_toggle")

        sql_dark_toggle.change(
            fn=handle_sql_dark_toggle,
            inputs=[sql_dark_toggle, sql_chart_display, sql_state],
            outputs=[sql_chart_display]
        )
        logger.info("[EVENT_SETUP] ✓ sql_dark_toggle.change → handle_sql_dark_toggle")

        # ============================================================
        # SQL Query Button Handlers
        # ============================================================

        # Run SQL button handler
        def handle_run_sql(query, max_rows, max_cols, is_dark, current_state):
            """Handle Run Query button click."""
            log_event("run_sql", query, max_rows, max_cols, is_dark)
            logger.info(f"[SQL_BTN] Running query: {query[:100] if query else 'None'}..., dark mode={is_dark}")
            
            # Update state with the query
            if isinstance(current_state, dict):
                current_state["sql_query"] = query
                
            res = execute_sql(query, max_rows, max_cols, is_dark=is_dark)
            # res is a tuple of 11 elements, we append current_state
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
            inputs=[sql_input, row_slider_sql, col_dropdown_sql, sql_dark_toggle, app_state],
            outputs=[
                sql_status,           # Execution status message
                sql_results,          # Results dataframe
                sql_css_override,     # CSS for table styling
                sql_history_dropdown, # Update history
                sql_chart_display,    # Auto-generated chart
                sql_state,            # Store dataframe for charting
                sql_x_axis,           # Update chart controls
                sql_y_axis,           # Update chart controls
                sql_color_by,         # Update chart controls
                sql_facet_by,         # Update chart controls
                exec_stats,           # Execution statistics
                app_state             # Browser state persistence
            ]
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
            outputs=[save_status, sql_pattern_dropdown]
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
            path = export_results("csv", df)
            return gr.update(value=path, visible=True) if path else gr.update()

        def handle_export_json(df):
            """Handle JSON export."""
            path = export_results("json", df)
            return gr.update(value=path, visible=True) if path else gr.update()

        def handle_export_parquet(df):
            """Handle Parquet export."""
            path = export_results("parquet", df)
            return gr.update(value=path, visible=True) if path else gr.update()

        def handle_export_xlsx(df):
            """Handle Excel export."""
            path = export_results("xlsx", df)
            return gr.update(value=path, visible=True) if path else gr.update()

        # Wire up export button handlers
        sql_export_csv_btn.click(
            fn=handle_export_csv,
            inputs=[sql_state],
            outputs=[sql_export_download]
        )
        logger.info("[EVENT_SETUP] ✓ sql_export_csv_btn.click → handle_export_csv")

        sql_export_json_btn.click(
            fn=handle_export_json,
            inputs=[sql_state],
            outputs=[sql_export_download]
        )
        logger.info("[EVENT_SETUP] ✓ sql_export_json_btn.click → handle_export_json")

        sql_export_parquet_btn.click(
            fn=handle_export_parquet,
            inputs=[sql_state],
            outputs=[sql_export_download]
        )
        logger.info("[EVENT_SETUP] ✓ sql_export_parquet_btn.click → handle_export_parquet")

        sql_export_xlsx_btn.click(
            fn=handle_export_xlsx,
            inputs=[sql_state],
            outputs=[sql_export_download]
        )
        logger.info("[EVENT_SETUP] ✓ sql_export_xlsx_btn.click → handle_export_xlsx")

        # Manual chart controls - regenerate chart when parameters change
        def handle_manual_chart_params(chart_type, x_col, y_col, color_col, facet_col, show_trend, is_dark, df):
            """Handle manual chart parameter changes."""
            if df is None:
                return gr.update()
            return render_manual_chart(df, chart_type, x_col, y_col, color_col, facet_col, show_trend, is_dark=is_dark)

        # Wire up manual chart controls
        sql_chart_type.change(
            fn=handle_manual_chart_params,
            inputs=[sql_chart_type, sql_x_axis, sql_y_axis, sql_color_by, sql_facet_by, sql_show_trend, sql_dark_toggle, sql_state],
            outputs=[sql_chart_display]
        )
        logger.info("[EVENT_SETUP] ✓ sql_chart_type.change → handle_manual_chart_params")

        # Also update when axis selections change
        sql_x_axis.change(
            fn=handle_manual_chart_params,
            inputs=[sql_chart_type, sql_x_axis, sql_y_axis, sql_color_by, sql_facet_by, sql_show_trend, sql_dark_toggle, sql_state],
            outputs=[sql_chart_display]
        )
        logger.info("[EVENT_SETUP] ✓ sql_x_axis.change → handle_manual_chart_params")

        sql_y_axis.change(
            fn=handle_manual_chart_params,
            inputs=[sql_chart_type, sql_x_axis, sql_y_axis, sql_color_by, sql_facet_by, sql_show_trend, sql_dark_toggle, sql_state],
            outputs=[sql_chart_display]
        )
        logger.info("[EVENT_SETUP] ✓ sql_y_axis.change → handle_manual_chart_params")

        sql_color_by.change(
            fn=handle_manual_chart_params,
            inputs=[sql_chart_type, sql_x_axis, sql_y_axis, sql_color_by, sql_facet_by, sql_show_trend, sql_dark_toggle, sql_state],
            outputs=[sql_chart_display]
        )
        logger.info("[EVENT_SETUP] ✓ sql_color_by.change → handle_manual_chart_params")

        sql_facet_by.change(
            fn=handle_manual_chart_params,
            inputs=[sql_chart_type, sql_x_axis, sql_y_axis, sql_color_by, sql_facet_by, sql_show_trend, sql_dark_toggle, sql_state],
            outputs=[sql_chart_display]
        )
        logger.info("[EVENT_SETUP] ✓ sql_facet_by.change → handle_manual_chart_params")

        sql_show_trend.change(
            fn=handle_manual_chart_params,
            inputs=[sql_chart_type, sql_x_axis, sql_y_axis, sql_color_by, sql_facet_by, sql_show_trend, sql_dark_toggle, sql_state],
            outputs=[sql_chart_display]
        )
        logger.info("[EVENT_SETUP] ✓ sql_show_trend.change → handle_manual_chart_params")

        logger.info("[EVENT_SETUP] All event handlers wired successfully.")

    # Return the app object, theme, custom_css, and keyboard shortcuts JS for testing
    return app, app_theme, app_css, keyboard_shortcuts_js

def launch_ui():
    """Launch the Gradio UI with predefined configuration."""
    app, theme, css, keyboard_shortcuts_js = create_ui()

    # Debug: Verify JavaScript is generated correctly
    print(f"[DEBUG] JavaScript length: {len(keyboard_shortcuts_js)} characters")
    print(f"[DEBUG] JavaScript contains theme protection: {'THEME PROTECTION' in keyboard_shortcuts_js}")
    print(f"[DEBUG] JavaScript preview (first 500 chars):\n{keyboard_shortcuts_js[:500]}")

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
