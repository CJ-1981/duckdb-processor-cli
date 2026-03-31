import gradio as gr
import pandas as pd
import sqlparse
import json
import os
import shutil
import sys
import logging
import traceback

# Set up logging to see debug info in the terminal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from duckdb_processor.loader import load
from duckdb_processor.config import ProcessorConfig
from duckdb_processor.analyzer import list_analyzers, get_analyzer

import plotly.express as px
import plotly.graph_objects as go

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
LAST_SESSION_FILE = ".gradio_session.json"

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

def save_session(file_path, header, kv):
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

# Global state for a single-user local app
global_processor = None
query_history = []

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
        profile_df = global_processor.sql(f"SUMMARIZE {global_processor.table}")
        
        # Explicitly round typical numeric-stat columns for readability
        for col in ['min', 'max', 'avg', 'std', 'q25', 'q50', 'q75', 'null_percentage']:
            if col in profile_df.columns:
                try:
                    # Convert to numeric (coercing non-numeric to NaN) then round
                    profile_df[col] = pd.to_numeric(profile_df[col], errors='coerce').round(2)
                except:
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

def save_session(file_path, header, kv):
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

def load_data(file_obj, header, kv, is_dark=False):
    """Load the CSV into DuckDB via Processor API and return preview."""
    global global_processor
    # Handle the case where file_obj is a path (string) or a Gradio file object
    file_path = file_obj if isinstance(file_obj, str) else (file_obj.name if file_obj else None)
    
    logger.info(f"Loading data: file={file_path}, header={header}, kv={kv}")
    if not file_path:
        return "⚠️ No file provided.", None, "No data.", None, None, None
    
    try:
        # Pass file path to config
        config = ProcessorConfig(file=file_path, header=header, kv=kv)
        global_processor = load(config)
        
        # Save session info for auto-recovery
        save_session(file_path, header, kv)
        
        info = global_processor.info()
        info_str = f"Rows: {info.get('rows', '?')}, Cols: {info.get('cols', '?')}"
        
        preview_df = global_processor.preview(100)
        schema_str = get_schema_info()
        health_fig, health_df, profile_df = get_data_profiling(is_dark=is_dark)
        
        logger.info("Data loaded successfully.")
        return (
            f"✅ Data Loaded Successfully\n\n{info_str}", 
            preview_df, 
            schema_str, 
            health_fig,
            health_df,
            profile_df
        )
    except Exception as e:
        error_msg = f"❌ Error loading data: {e}"
        logger.error(f"{error_msg}\n{traceback.format_exc()}")
        return error_msg, None, "Error", None, None, None

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
    global global_processor
    logger.info(f"Executing SQL query, max_rows={max_rows}, max_cols={max_cols}")
    if global_processor is None:
        return "❌ Error: Please load data first (go to Data Preview tab).", gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update()
    
    if not query or not query.strip():
        return "⚠️ Query is empty.", gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), gr.update()
    
    try:
        df = global_processor.sql(query)
        total_rows = len(df)
        
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
            gr.update(choices=choices_with_none, value=None)  # Facet By
        )
    except Exception as e:
        error_msg = f"❌ Error executing SQL: {e}"
        logger.error(error_msg)
        return (
            error_msg, gr.update(), gr.update(), gr.update(), gr.update(), gr.update(), 
            gr.update(), gr.update(), gr.update(), gr.update()
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

# Custom CSS for UI polish
custom_css = """
/* Hide Screen Studio / Recording tools */
button[title*='Record'], button[title*='Screen'], 
.record-button, .stop-recording, .screen-studio-ui {
    display: none !important;
}

/* Enhance dataframe visibility */
.gradio-dataframe table { border-collapse: collapse; }

/* Custom coloring for SQL code blocks */
.cm-s-default .cm-keyword { color: #d73a49; font-weight: bold; }
.cm-s-default .cm-string { color: #032f62; }
.cm-s-default .cm-variable { color: #005cc5; }

/* Load Data Button - Emerald Green */
.btn-load { background: linear-gradient(90deg, #059669, #10b981) !important; color: white !important; border: none !important; }
.btn-load:hover { background: linear-gradient(90deg, #047857, #059669) !important; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2) !important; }

/* Run Buttons - Indigo */
.btn-run { background: linear-gradient(90deg, #4f46e5, #6366f1) !important; color: white !important; border: none !important; }
.btn-run:hover { background: linear-gradient(90deg, #4338ca, #4f46e5) !important; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2) !important; }

/* Export Buttons - Soft Purple Outline */
.btn-export { color: #8b5cf6 !important; border: 1px solid #c4b5fd !important; background: transparent !important; transition: all 0.2s !important; }
.btn-export:hover { background: #8b5cf6 !important; color: white !important; border-color: #8b5cf6 !important; }
/* Dark Mode Override */
.dark .btn-export { color: #a78bfa !important; border-color: #4c1d95 !important; background: transparent !important; }
.dark .btn-export:hover { background: #a78bfa !important; color: #111827 !important; border-color: #a78bfa !important; }

/* Prettify Button - Amber */
.btn-format { color: #d97706 !important; border: 1px solid #fcd34d !important; background: #fffbeb !important; transition: all 0.2s !important; }
.btn-format:hover { background: #fef3c7 !important; color: #b45309 !important; border-color: #fbbf24 !important; }
/* Dark Mode Override */
.dark .btn-format { color: #fbbf24 !important; border-color: #92400e !important; background: #451a03 !important; }
.dark .btn-format:hover { background: #78350f !important; color: #fef3c7 !important; border-color: #fbbf24 !important; }

/* Save Pattern Button - Sky Blue */
.btn-save { color: #0284c7 !important; border: 1px solid #bae6fd !important; background: #f0f9ff !important; transition: all 0.2s !important; }
.btn-save:hover { background: #e0f2fe !important; border-color: #7dd3fc !important; color: #0369a1 !important; }
/* Dark Mode Override */
.dark .btn-save { color: #38bdf8 !important; border-color: #0369a1 !important; background: #082f49 !important; }
.dark .btn-save:hover { background: #0c4a6e !important; color: #7dd3fc !important; border-color: #38bdf8 !important; }
"""

def create_ui():
    theme = gr.themes.Soft(
        primary_hue="blue",
        neutral_hue="slate",
    ).set(
        button_primary_background_fill="*primary_500",
        button_primary_background_fill_hover="*primary_600",
    )
    
    with gr.Blocks(title="DuckDB Processor UI") as app:
        # States to persistent data for manual charting
        analysis_state = gr.State(None)
        sql_state = gr.State(None)
        
        gr.Markdown("# 🦆 DuckDB CSV Processor")
        gr.Markdown("An interactive dashboard to explore, transform, and analyze your CSV data quickly.")
        
        with gr.Row():
            with gr.Column(scale=1):
                file_input = gr.File(label="Upload CSV File", file_types=[".csv", ".tsv", ".txt"])
                
                with gr.Row():
                    header_check = gr.Checkbox(label="Has Header?", value=True)
                    kv_check = gr.Checkbox(label="Is Key-Value Pairs?", value=False)
                
                load_btn = gr.Button("Load Data", variant="primary", elem_classes=["btn-load"])
                info_box = gr.Textbox(label="Data Info & Status", lines=10, interactive=False)
                
                # Schema sidebar component
                schema_sidebar = gr.Code(label="Table Schema", language="sql", interactive=False, lines=15)
            
            with gr.Column(scale=3):
                with gr.Tabs() as main_tabs:
                    # -----------------------------
                    # TAB 1: Data Preview
                    # -----------------------------
                    with gr.Tab("Data Preview"):
                        gr.Markdown("### 📊 Initial 100 Rows Preview")
                        preview_table = gr.Dataframe(
                            label="Table Data", 
                            interactive=False, 
                            wrap=True,
                            row_count=(10, "dynamic")
                        )

                    # -----------------------------
                    # TAB 2: Data Profiling
                    # -----------------------------
                    with gr.Tab("Data Profiling"):
                        gr.Markdown("### 🔍 Data Quality & Profiling")
                        with gr.Row():
                            profile_dark_toggle = gr.Checkbox(label="Dark Mode Charts", value=False)
                        
                        with gr.Row():
                            with gr.Column(scale=2):
                                profile_plot = gr.Plot(label="Column Coverage (%)")
                            with gr.Column(scale=1):
                                profile_coverage_table = gr.Dataframe(label="Coverage Stats")
                        
                        gr.Markdown("#### 📈 Column Statistics (SUMMARIZE)")
                        profile_summary_table = gr.Dataframe(
                            label="Summary Statistics", 
                            interactive=False,
                            wrap=True,
                            max_height=400
                        )

                    # -----------------------------
                    # TAB 3: Run Analytics
                    # -----------------------------
                    with gr.Tab("Run Analytics") as analysis_tab:
                        gr.Markdown("### 📈 Built-in and Custom Analyzers")
                        
                        analyzer_choices = get_analyzer_choices()
                        
                        with gr.Row():
                            with gr.Column(scale=2):
                                analyzer_dropdown = gr.Dropdown(
                                    choices=analyzer_choices, 
                                    label="Select Analyzer"
                                )
                                with gr.Row():
                                    row_slider_analysis = gr.Dropdown(choices=[15, 25, 50, 100, 200], value=50, label="Rows")
                                    col_dropdown_analysis = gr.Dropdown(choices=["5", "10", "20", "50", "All"], value="All", label="Cols")
                                run_analyzer_btn = gr.Button("▶ Run Analyzer", variant="primary", elem_classes=["btn-run"])
                                
                            with gr.Column(scale=1):
                                plugin_upload = gr.File(label="Drop custom plugin (.py)", file_types=[".py"])
                                plugin_status = gr.Textbox(label="Plugin Status", lines=1, interactive=False)
                        
                        analyzer_status = gr.Textbox(label="Status", lines=1, interactive=False)
                        
                        # Export buttons
                        with gr.Row():
                            gr.Markdown("**Export Last Result:**")
                            export_csv_btn = gr.Button("CSV", size="sm", elem_classes=["btn-export"])
                            export_json_btn = gr.Button("JSON", size="sm", elem_classes=["btn-export"])
                            export_parquet_btn = gr.Button("Parquet", size="sm", elem_classes=["btn-export"])
                            export_xlsx_btn = gr.Button("Excel", size="sm", elem_classes=["btn-export"])
                        
                        export_download = gr.File(label="Download Exported File", visible=False)
                        
                        analyzer_results = gr.Dataframe(
                            label="Analysis Results",
                            interactive=False,
                            wrap=True,
                            elem_id="analysis-results",
                            max_height=500
                        )
                        analysis_css_override = gr.HTML("")
                        
                        gr.Markdown("---")
                        gr.Markdown("### 📊 Visualization Control")
                        with gr.Row():
                            analysis_chart_type = gr.Dropdown(
                                choices=["Bar", "Line", "Scatter", "Pie", "Histogram"], 
                                value="Bar", 
                                label="Chart Type"
                            )
                            analysis_x_axis = gr.Dropdown(choices=[], label="X-Axis")
                            analysis_y_axis = gr.Dropdown(choices=[], label="Y-Axis")
                        
                        with gr.Row():
                            analysis_color_by = gr.Dropdown(choices=[], label="Color By")
                            analysis_facet_by = gr.Dropdown(choices=[], label="Facet By")
                        
                        with gr.Row():
                            analysis_show_trend = gr.Checkbox(label="Show Trend Line (Scatter Only)", value=False)
                            analysis_dark_toggle = gr.Checkbox(label="Dark Mode Charts", value=False)
                        
                        analysis_chart_display = gr.Plot(label="Analysis Chart")
                        
                        # Added this to keep "Auto Chart" around if needed
                        analysis_auto_chart_state = gr.State(None)

                    # -----------------------------
                    # TAB 4: SQL Editor
                    # -----------------------------
                    with gr.Tab("SQL Editor") as sql_tab:
                        gr.Markdown("### 💻 Custom SQL Query")
                        
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
                            run_sql_btn = gr.Button("▶ Run SQL", variant="primary", elem_classes=["btn-run"])
                            format_btn = gr.Button("✨ Prettify SQL", elem_classes=["btn-format"])
                        
                        with gr.Row():
                            with gr.Column(scale=2):
                                save_pattern_name = gr.Textbox(label="New Pattern Name", placeholder="e.g. My Custom Analysis", interactive=True)
                            with gr.Column(scale=1):
                                save_pattern_btn = gr.Button("💾 Save as Pattern", elem_classes=["btn-save"])
                        
                        save_status = gr.Textbox(label="Save Status", lines=1, interactive=False)
                        
                        with gr.Row():
                            row_slider_sql = gr.Dropdown(choices=[15, 25, 50, 100, 200], value=50, label="Rows")
                            col_dropdown_sql = gr.Dropdown(choices=["5", "10", "20", "50", "All"], value="All", label="Cols")
                        
                        sql_status = gr.Textbox(label="Execution Status", lines=1, interactive=False)
                        
                        # Export buttons for SQL
                        with gr.Row():
                            gr.Markdown("**Export Last Result:**")
                            sql_export_csv_btn = gr.Button("CSV", size="sm", elem_classes=["btn-export"])
                            sql_export_json_btn = gr.Button("JSON", size="sm", elem_classes=["btn-export"])
                            sql_export_parquet_btn = gr.Button("Parquet", size="sm", elem_classes=["btn-export"])
                            sql_export_xlsx_btn = gr.Button("Excel", size="sm", elem_classes=["btn-export"])
                        
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
                        gr.Markdown("### 📊 Visualization Control")
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
                            sql_dark_toggle = gr.Checkbox(label="Dark Mode Charts", value=False)
                        
                        sql_chart_display = gr.Plot(label="SQL Chart")

        # --- Event Listeners ---
        
        # Load Data
        # Auto-restore session logic
        def auto_restore_session(is_dark):
            """Attempt to restore the last session from the local cache."""
            if not os.path.exists(LAST_SESSION_FILE):
                return [gr.update()]*6
                
            try:
                with open(LAST_SESSION_FILE, "r") as f:
                    session = json.load(f)
                if session and os.path.exists(session["file_path"]):
                    logger.info(f"Auto-restoring session: {session['file_path']}")
                    return load_data(session["file_path"], session["header"], session["kv"], is_dark=is_dark)
            except Exception as e:
                logger.error(f"Session restoration failed: {e}")
            return [gr.update()]*6

        app.load(
            fn=auto_restore_session,
            inputs=[profile_dark_toggle],
            outputs=[info_box, preview_table, schema_sidebar, profile_plot, profile_coverage_table, profile_summary_table]
        )

        load_btn.click(
            fn=load_data,
            inputs=[file_input, header_check, kv_check, profile_dark_toggle],
            outputs=[info_box, preview_table, schema_sidebar, profile_plot, profile_coverage_table, profile_summary_table]
        )

        profile_dark_toggle.change(
            fn=refresh_profiling,
            inputs=[profile_dark_toggle],
            outputs=[profile_plot]
        )
        
        # Run Analyzer
        run_analyzer_btn.click(
            fn=run_analysis,
            inputs=[analyzer_dropdown, row_slider_analysis, col_dropdown_analysis, analysis_dark_toggle],
            outputs=[
                analyzer_status, 
                analyzer_results, 
                analysis_css_override, 
                analysis_chart_display,
                analysis_state,
                analysis_x_axis,
                analysis_y_axis,
                analysis_color_by,
                analysis_facet_by
            ]
        )
        
        # Manual Charting (Analysis)
        analysis_man_inputs = [
            analysis_state, analysis_chart_type, analysis_x_axis, 
            analysis_y_axis, analysis_color_by, analysis_facet_by, 
            analysis_show_trend, analysis_dark_toggle
        ]
        analysis_chart_type.change(render_manual_chart, inputs=analysis_man_inputs, outputs=analysis_chart_display)
        analysis_x_axis.change(render_manual_chart, inputs=analysis_man_inputs, outputs=analysis_chart_display)
        analysis_y_axis.change(render_manual_chart, inputs=analysis_man_inputs, outputs=analysis_chart_display)
        analysis_color_by.change(render_manual_chart, inputs=analysis_man_inputs, outputs=analysis_chart_display)
        analysis_facet_by.change(render_manual_chart, inputs=analysis_man_inputs, outputs=analysis_chart_display)
        analysis_dark_toggle.change(render_manual_chart, inputs=analysis_man_inputs, outputs=analysis_chart_display)
        analysis_show_trend.change(render_manual_chart, inputs=analysis_man_inputs, outputs=analysis_chart_display)
        analysis_dark_toggle.change(render_manual_chart, inputs=analysis_man_inputs, outputs=analysis_chart_display)
        
        # When "Sync" is unchecked, we do nothing special. When checked, the JS will handle it.
        
        # Run SQL
        run_sql_btn.click(
            fn=execute_sql,
            inputs=[sql_input, row_slider_sql, col_dropdown_sql, sql_dark_toggle],
            outputs=[
                sql_status, 
                sql_results, 
                sql_css_override, 
                sql_history_dropdown, 
                sql_chart_display,
                sql_state,
                sql_x_axis,
                sql_y_axis,
                sql_color_by,
                sql_facet_by
            ]
        )
        
        # Manual Charting (SQL)
        sql_man_inputs = [
            sql_state, sql_chart_type, sql_x_axis, 
            sql_y_axis, sql_color_by, sql_facet_by, 
            sql_show_trend, sql_dark_toggle
        ]
        sql_chart_type.change(render_manual_chart, inputs=sql_man_inputs, outputs=sql_chart_display)
        sql_x_axis.change(render_manual_chart, inputs=sql_man_inputs, outputs=sql_chart_display)
        sql_y_axis.change(render_manual_chart, inputs=sql_man_inputs, outputs=sql_chart_display)
        sql_color_by.change(render_manual_chart, inputs=sql_man_inputs, outputs=sql_chart_display)
        sql_facet_by.change(render_manual_chart, inputs=sql_man_inputs, outputs=sql_chart_display)
        sql_show_trend.change(render_manual_chart, inputs=sql_man_inputs, outputs=sql_chart_display)
        sql_dark_toggle.change(render_manual_chart, inputs=sql_man_inputs, outputs=sql_chart_display)
        
        # Prettify SQL
        format_btn.click(fn=prettify_sql, inputs=[sql_input], outputs=[sql_input])
        
        # SQL Pattern Selection
        sql_pattern_dropdown.change(
            fn=update_sql_from_selection,
            inputs=[sql_pattern_dropdown],
            outputs=[sql_input]
        )
        
        # SQL History Selection
        sql_history_dropdown.change(
            fn=apply_historical_query,
            inputs=[sql_history_dropdown],
            outputs=[sql_input]
        )
        
        # Save Pattern
        save_pattern_btn.click(
            fn=save_new_pattern,
            inputs=[save_pattern_name, sql_input],
            outputs=[save_status, sql_pattern_dropdown]
        )
        
        # Plugin Upload
        plugin_upload.upload(
            fn=upload_plugin,
            inputs=[plugin_upload],
            outputs=[plugin_status, analyzer_dropdown]
        )
        
        # Exporting Analysis Results
        def make_analyzer_export(fmt, df):
            """Wrapper for analyzer exports."""
            return export_results(fmt, df=df)

        export_csv_btn.click(lambda df: make_analyzer_export("csv", df), inputs=[analysis_state], outputs=export_download, concurrency_limit=1)
        export_json_btn.click(lambda df: make_analyzer_export("json", df), inputs=[analysis_state], outputs=export_download, concurrency_limit=1)
        export_parquet_btn.click(lambda df: make_analyzer_export("parquet", df), inputs=[analysis_state], outputs=export_download, concurrency_limit=1)
        export_xlsx_btn.click(lambda df: make_analyzer_export("xlsx", df), inputs=[analysis_state], outputs=export_download, concurrency_limit=1)
        
        # Exporting SQL Results
        def make_sql_export(fmt, df):
            """Wrapper for SQL exports."""
            return export_results(fmt, df=df)

        sql_export_csv_btn.click(lambda df: make_sql_export("csv", df), inputs=[sql_state], outputs=sql_export_download, concurrency_limit=1)
        sql_export_json_btn.click(lambda df: make_sql_export("json", df), inputs=[sql_state], outputs=sql_export_download, concurrency_limit=1)
        sql_export_parquet_btn.click(lambda df: make_sql_export("parquet", df), inputs=[sql_state], outputs=sql_export_download, concurrency_limit=1)
        sql_export_xlsx_btn.click(lambda df: make_sql_export("xlsx", df), inputs=[sql_state], outputs=sql_export_download, concurrency_limit=1)

        # Floating Back to Top Button
        gr.HTML("""
            <button id='back-to-top' onclick='window.scrollTo({top: 0, behavior: "smooth"});'
                    style='position: fixed; bottom: 30px; right: 30px; z-index: 1000; width: 50px; height: 50px; border-radius: 50%; background-color: #4f46e5; color: white; border: none; box-shadow: 0 4px 12px rgba(0,0,0,0.15); cursor: pointer; display: flex; align-items: center; justify-content: center; font-size: 20px;'>
                ⬆️
            </button>
        """)

        # JavaScript for UI Cleanup (Screen Studio Removal) and auto-restore stability
        gr.HTML("""
            <script>
                function cleanUI() {
                    // Remove any elements that mention Screen Studio or Recording
                    const selectors = ['div', 'h1', 'h2', 'h3', 'p', 'span', 'button', 'label'];
                    selectors.forEach(tag => {
                        document.querySelectorAll(tag).forEach(el => {
                            const txt = el.innerText || "";
                            if (txt.includes('Screen Studio') || 
                                txt.includes('Record your screen') || 
                                txt.includes('Start Recording') ||
                                txt.includes('Stop Recording')) {
                                el.style.display = 'none';
                                // If it's a settings row, hide the parent
                                if (el.closest('.setting-item') || el.closest('.row')) {
                                    (el.closest('.setting-item') || el.closest('.row')).style.display = 'none';
                                }
                            }
                        });
                    });
                }

                // Run frequently to catch elements as they load (like the settings modal)
                setInterval(cleanUI, 1000);
                
                // Also trigger on initial load
                window.addEventListener('load', cleanUI);
            </script>
        """)

        gr.HTML("<div style='text-align: center; margin-top: 50px; padding-bottom: 30px; color: #888; font-size: 12px;'>DuckDB Processor UI - Enhanced Build 1.1.0</div>")

    return app, theme, custom_css

if __name__ == "__main__":
    app, app_theme, app_css = create_ui()
    # Configure the launch parameters
    app.launch(
        server_name="127.0.0.1",
        server_port=7860,
        inbrowser=True,
        debug=False,         # Disable debug mode to hide dev tools
        theme=app_theme,
        css=app_css,
    )
