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
        profile_df = global_processor.con.execute(f"SUMMARIZE {global_processor.table}").df()
        
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
    
    # Auto-fix: DuckDB 1.x has a known parser bug where backticks with multi-byte or 
    # even some ASCII chars trigger a '__postfix' scalar function error.
    # Standard SQL uses double quotes for identifiers, so we auto-convert backticks.
    if "`" in query:
        logger.info("Auto-converting backticks to double quotes for DuckDB compatibility.")
        query = query.replace("`", '"')

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
        err_str = str(e)
        error_msg = f"❌ Error executing SQL: {err_str}"
        
        # Check for specific DuckDB backtick bug ('__postfix' error)
        if "__postfix" in err_str and "`" in query:
            error_msg += "\n\n💡 Tip: DuckDB's parser often misinterprets MySQL-style backticks (`) as postfix operators. Try using standard double quotes (\") for column names instead!"
            
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
                    
                md += global_processor.last_result.head(20).to_markdown(index=False) + "\n\n"
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
    unicode_font_path = "/Library/Fonts/Arial Unicode.ttf"
    font_name = "helvetica"
    has_unicode = False
    
    if os.path.exists(unicode_font_path):
        font_name = "ArialUnicode"
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
                if global_processor.last_action:
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
            module = importlib.util.module_from_spec(spec)
            
            with contextlib.redirect_stdout(stdout_buffer):
                spec.loader.exec_module(module)
                
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

# Custom CSS for UI polish
custom_css = """
/* Hide Screen Studio / Recording tools */
button[title*='Record'], button[title*='Screen'], 
.record-button, .stop-recording, .screen-studio-ui {
    display: none !important;
}

/* Enhance dataframe visibility */
.gradio-dataframe table { border-collapse: collapse; }

/* Export Buttons Styling for Visibility */
.btn-export {
    border: 2px solid #10b981 !important;
    background: rgba(16, 185, 129, 0.05) !important;
    color: #059669 !important;
    font-weight: bold !important;
}

/* Dark mode specific overrides for buttons */
.dark .btn-export, [data-theme='dark'] .btn-export {
    border-color: #34d399 !important;
    color: #34d399 !important;
    background: rgba(52, 211, 153, 0.1) !important;
}

.btn-export:hover {
    background-color: #10b981 !important;
    color: white !important;
    box-shadow: 0 0 10px rgba(16, 185, 129, 0.4) !important;
}

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

/* Test Button - Emerald/Success */
.btn-test { background: linear-gradient(90deg, #10b981, #059669) !important; color: white !important; font-weight: bold !important; border: none !important; }
.btn-test:hover { box-shadow: 0 0 15px rgba(16, 185, 129, 0.4) !important; transform: translateY(-1px); }

/* Delete Button - Rose/Danger */
.btn-delete { color: #e11d48 !important; border: 1px solid #fecdd3 !important; background: #fff1f2 !important; }
.btn-delete:hover { background: #f43f5e !important; color: white !important; border-color: #f43f5e !important; }
.dark .btn-delete { color: #fb7185 !important; border-color: #881337 !important; background: #4c0519 !important; }
.dark .btn-delete:hover { background: #e11d48 !important; color: white !important; }

/* New Button - Indigo */
.btn-new { color: #4f46e5 !important; border: 1px solid #c7d2fe !important; background: #eef2ff !important; }
.btn-new:hover { background: #4f46e5 !important; color: white !important; }
.dark .btn-new { color: #818cf8 !important; border-color: #312e81 !important; background: #1e1b4b !important; }
.dark .btn-new:hover { background: #4f46e5 !important; color: white !important; }

/* Logs View */
.logs-view { font-family: 'Fira Code', monospace !important; font-size: 13px !important; line-height: 1.4 !important; }

/* Report Section styling */
.report-section-list { font-family: sans-serif; background: rgba(0,0,0,0.02); border-radius: 8px; padding: 10px; }
.dark .report-section-list { background: rgba(255,255,255,0.02); }

/* Keyboard Shortcut Badges */
.kbd-shortcut {
    display: inline-flex;
    align-items: center;
    margin-left: 8px;
    padding: 2px 6px;
    background: linear-gradient(180deg, #f8f9fa, #e9ecef);
    border: 1px solid #dee2e6;
    border-radius: 4px;
    font-family: 'SF Mono', 'Monaco', 'Inconsolata', 'Fira Code', monospace;
    font-size: 11px;
    font-weight: 600;
    color: #495057;
    box-shadow: 0 1px 2px rgba(0,0,0,0.1), inset 0 1px 0 rgba(255,255,255,0.6);
    line-height: 1;
    min-width: 24px;
    justify-content: center;
}

.dark .kbd-shortcut {
    background: linear-gradient(180deg, #374151, #1f2937);
    border-color: #4b5563;
    color: #e5e7eb;
    box-shadow: 0 1px 2px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.1);
}

.kbd-shortcut .modifier {
    margin-right: 2px;
    font-size: 10px;
    color: #6c757d;
}

.dark .kbd-shortcut .modifier {
    color: #9ca3af;
}

/* Animation for shortcut badge appearance */
@keyframes shortcutFadeIn {
    from { opacity: 0; transform: translateX(-5px); }
    to { opacity: 1; transform: translateX(0); }
}

.kbd-shortcut {
    animation: shortcutFadeIn 0.2s ease-out;
}
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
        report_sections_state = gr.State([])
        
        gr.Markdown("# 🦆 DuckDB CSV Processor")
        gr.Markdown("An interactive dashboard to explore, transform, and analyze your CSV data quickly.")
        
        with gr.Row():
            with gr.Column(scale=1):
                file_input = gr.File(label="Upload CSV File", file_types=[".csv", ".tsv", ".txt"])
                
                with gr.Row():
                    header_check = gr.Checkbox(label="Has Header?", value=True)
                    kv_check = gr.Checkbox(label="Is Key-Value Pairs?", value=False)
                
                load_btn = gr.Button("Load Data", variant="primary", elem_classes=["btn-load"], elem_id="load_btn")
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
                    # -----------------------------
                    # TAB 3: Run Analytics
                    # -----------------------------
                    with gr.Tab("Run Analytics") as analysis_tab:
                        with gr.Tabs():
                            # Sub-tab 3.1: Running Analyzers
                            with gr.Tab("📈 Run Analyzers"):
                                gr.Markdown("### 🚀 Execute Analysis Modules")
                                
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
                                        run_analyzer_btn = gr.Button("▶ Run Analyzer", variant="primary", elem_classes=["btn-run"], elem_id="run_analyzer_btn")
                                        
                                    with gr.Column(scale=1):
                                        plugin_upload = gr.File(label="Quick Upload (.py)", file_types=[".py"])
                                        plugin_status = gr.Textbox(label="Upload Status", lines=1, interactive=False)
                                
                                analyzer_status = gr.Textbox(label="Status", lines=1, interactive=False)
                                
                                # Export buttons
                                with gr.Row():
                                    gr.Markdown("**Export Last Result:**")
                                    export_csv_btn = gr.Button("CSV", size="sm", elem_classes=["btn-export"], elem_id="export_csv_btn")
                                    export_json_btn = gr.Button("JSON", size="sm", elem_classes=["btn-export"], elem_id="export_json_btn")
                                    export_parquet_btn = gr.Button("Parquet", size="sm", elem_classes=["btn-export"], elem_id="export_parquet_btn")
                                    export_xlsx_btn = gr.Button("Excel", size="sm", elem_classes=["btn-export"], elem_id="export_xlsx_btn")
                                
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
                                analysis_auto_chart_state = gr.State(None)

                            # Sub-tab 3.2: Plugin Editor
                            with gr.Tab("🐍 Plugin Editor"):
                                gr.Markdown("### 🛠️ Python Plugin Creator & Editor")
                                
                                with gr.Row():
                                    with gr.Column(scale=2):
                                        plugin_editor_dropdown = gr.Dropdown(
                                            choices=get_plugin_list(),
                                            label="Browse Plugins",
                                            info="Select any plugin to view or edit"
                                        )
                                    with gr.Column(scale=1):
                                        with gr.Row():
                                            new_plugin_btn = gr.Button("📄 New", elem_classes=["btn-new"], elem_id="new_plugin_btn")
                                            load_plugin_btn = gr.Button("📂 Load", variant="secondary")
                                            delete_plugin_btn = gr.Button("🗑️ Delete", elem_classes=["btn-delete"])
                                
                                with gr.Row():
                                    plugin_name_input = gr.Textbox(label="Plugin Name (filename)", placeholder="my_custom_analysis")
                                    plugin_save_btn = gr.Button("💾 Save Plugin", elem_classes=["btn-save"], scale=0, elem_id="plugin_save_btn")
                                
                                plugin_code_editor = gr.Code(
                                    label="Python Source",
                                    language="python",
                                    lines=20,
                                    interactive=True,
                                    value="# Create a new analyzer or select one from the dropdown above"
                                )
                                
                                with gr.Row():
                                    prettify_plugin_btn = gr.Button("✨ Prettify Python", elem_classes=["btn-format"], elem_id="prettify_plugin_btn")
                                    test_plugin_btn = gr.Button("▶ Test Plugin", elem_classes=["btn-test"], elem_id="test_plugin_btn")
                                
                                plugin_editor_status = gr.Textbox(label="Editor Status", lines=1, interactive=False)
                                
                                with gr.Row():
                                    with gr.Column(scale=2):
                                        plugin_test_results = gr.Dataframe(label="Test Execution Result (Last Result)", max_height=300)
                                    with gr.Column(scale=1):
                                        plugin_test_logs = gr.Textbox(label="Execution Logs / Console Output", lines=12, elem_classes=["logs-view"])

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
                            run_sql_btn = gr.Button("▶ Run SQL", variant="primary", elem_classes=["btn-run"], elem_id="run_sql_btn")
                            format_btn = gr.Button("✨ Prettify SQL", elem_classes=["btn-format"], elem_id="format_btn")
                        
                        with gr.Row():
                            with gr.Column(scale=2):
                                save_pattern_name = gr.Textbox(label="New Pattern Name", placeholder="e.g. My Custom Analysis", interactive=True)
                            with gr.Column(scale=1):
                                save_pattern_btn = gr.Button("💾 Save as Pattern", elem_classes=["btn-save"], elem_id="save_pattern_btn")
                        
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

                    # -----------------------------
                    # TAB 5: Report Builder
                    # -----------------------------
                    with gr.Tab("📝 Report Builder") as report_tab:
                        gr.Markdown("### 📄 Custom Report Composition")
                        gr.Markdown("Compose a professional document using your analysis results and custom notes.")
                        
                        with gr.Row():
                            with gr.Column(scale=2):
                                with gr.Group():
                                    gr.Markdown("#### 1. Report Metadata")
                                    report_title = gr.Textbox(label="Report Title", value="DuckDB Analysis Report")
                                    report_author = gr.Textbox(label="Author Name", placeholder="Your Name")
                                    
                                with gr.Group():
                                    gr.Markdown("#### 2. Templates")
                                    template_dropdown = gr.Dropdown(
                                        choices=list(REPORT_TEMPLATES.keys()),
                                        label="Select Template",
                                        info="Load a predefined report layout"
                                    )
                                    with gr.Row():
                                        apply_template_btn = gr.Button("📋 Apply Template", variant="secondary")
                                        save_template_name = gr.Textbox(label="Save as Template", placeholder="Template Name")
                                        save_template_btn = gr.Button("💾 Save", scale=0, elem_id="save_template_btn")
                            
                            with gr.Column(scale=3):
                                with gr.Group():
                                    gr.Markdown("#### 3. Add Content Section")
                                    with gr.Row():
                                        section_type = gr.Dropdown(
                                            choices=["Text/Note", "Analyzer Results Table", "SQL Results Table", "Data Summary", "Schema Info"],
                                            value="Text/Note",
                                            label="Section Type"
                                        )
                                        section_heading = gr.Textbox(label="Section Heading", placeholder="e.g. Findings Overview")
                                    
                                    section_body = gr.Textbox(
                                        label="Content Body (for Text/Note sections)", 
                                        lines=3, 
                                        placeholder="Enter your observations or notes here..."
                                    )
                                    
                                    with gr.Row():
                                        add_section_btn = gr.Button("➕ Add Section", variant="primary", elem_classes=["btn-run"], elem_id="add_section_btn")
                                        remove_idx = gr.Number(label="Section # to Remove", precision=0, minimum=1, step=1, value=1, scale=0)
                                        remove_section_btn = gr.Button("🗑️ Remove", elem_classes=["btn-delete"], scale=0)
                                        clear_sections_btn = gr.Button("🧹 Clear All", variant="stop", scale=0)
                                    
                                    report_status = gr.Textbox(label="Status", lines=1, interactive=False)
                        
                        gr.Markdown("---")
                        
                        with gr.Row():
                            with gr.Column(scale=2):
                                # Visual list of sections
                                sections_markdown_view = gr.Markdown("_No sections added yet._", elem_classes=["report-section-list"])
                                
                                with gr.Row():
                                    export_report_md_btn = gr.Button("📄 Export Markdown", elem_classes=["btn-export"], elem_id="export_report_md_btn")
                                    export_report_pdf_btn = gr.Button("📕 Export PDF", elem_classes=["btn-export"], elem_id="export_report_pdf_btn")
                                
                                report_file_download = gr.File(label="Download Report")
                                
                            with gr.Column(scale=3):
                                gr.Markdown("#### 🔍 Live Preview")
                                report_preview = gr.Markdown(
                                    "Your report preview will appear here...",
                                    label="Report Preview",
                                    show_label=True,
                                    container=True
                                )

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
        
        # Plugin Editor Events
        new_plugin_btn.click(
            fn=new_plugin_template,
            outputs=[plugin_code_editor, plugin_name_input, plugin_editor_status]
        )
        
        load_plugin_btn.click(
            fn=load_plugin_code,
            inputs=[plugin_editor_dropdown],
            outputs=[plugin_code_editor, plugin_name_input, plugin_editor_status]
        )
        
        plugin_save_btn.click(
            fn=save_plugin_file,
            inputs=[plugin_name_input, plugin_code_editor],
            outputs=[plugin_editor_status, analyzer_dropdown, plugin_editor_dropdown]
        )
        
        delete_plugin_btn.click(
            fn=delete_plugin_file,
            inputs=[plugin_editor_dropdown],
            outputs=[plugin_editor_status, analyzer_dropdown, plugin_editor_dropdown]
        )
        
        prettify_plugin_btn.click(
            fn=prettify_python_code,
            inputs=[plugin_code_editor],
            outputs=[plugin_code_editor]
        )
        
        test_plugin_btn.click(
            fn=test_custom_plugin,
            inputs=[plugin_code_editor],
            outputs=[plugin_editor_status, plugin_test_results, plugin_test_logs]
        )
        
        # Original Plugin Upload (kept for quick drops)
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

        # Report Builder Events
        def add_and_update(sections, s_type, s_heading, s_body, title, author):
            new_sections, status = add_report_section(sections, s_type, s_heading, s_body)
            return new_sections, status, render_sections_view(new_sections), generate_report_markdown(title, author, new_sections)

        def remove_and_update(sections, idx, title, author):
            new_sections, status = remove_report_section(sections, int(idx)-1)
            return new_sections, status, render_sections_view(new_sections), generate_report_markdown(title, author, new_sections)

        def clear_and_update(title, author):
            new_sections, status = clear_report_sections()
            return new_sections, status, render_sections_view(new_sections), generate_report_markdown(title, author, new_sections)

        add_section_btn.click(
            fn=add_and_update,
            inputs=[report_sections_state, section_type, section_heading, section_body, report_title, report_author],
            outputs=[report_sections_state, report_status, sections_markdown_view, report_preview]
        )

        remove_section_btn.click(
            fn=remove_and_update,
            inputs=[report_sections_state, remove_idx, report_title, report_author],
            outputs=[report_sections_state, report_status, sections_markdown_view, report_preview]
        )

        clear_sections_btn.click(
            fn=clear_and_update,
            inputs=[report_title, report_author],
            outputs=[report_sections_state, report_status, sections_markdown_view, report_preview]
        )

        apply_template_btn.click(
            fn=apply_report_template,
            inputs=[template_dropdown],
            outputs=[report_sections_state, report_status, sections_markdown_view, report_preview]
        )

        save_template_btn.click(
            fn=save_new_template,
            inputs=[save_template_name, report_sections_state],
            outputs=[report_status, template_dropdown]
        )

        export_report_md_btn.click(
            fn=lambda t, a, s: export_report_file("md", t, a, s),
            inputs=[report_title, report_author, report_sections_state],
            outputs=[report_file_download]
        )

        export_report_pdf_btn.click(
            fn=lambda t, a, s: export_report_file("pdf", t, a, s),
            inputs=[report_title, report_author, report_sections_state],
            outputs=[report_file_download]
        )

        # Refresh preview when metadata changes
        report_title.change(
            fn=generate_report_markdown,
            inputs=[report_title, report_author, report_sections_state],
            outputs=[report_preview]
        )
        report_author.change(
            fn=generate_report_markdown,
            inputs=[report_title, report_author, report_sections_state],
            outputs=[report_preview]
        )

        # Keyboard Shortcuts JavaScript
        gr.HTML("""
            <script>
                (function() {
                    // @MX:NOTE: Keyboard shortcuts system with OS-specific key detection
                    // Detect operating system for correct modifier key display
                    const isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0 ||
                                  navigator.userAgent.toUpperCase().indexOf('MAC') >= 0;
                    const modifierKey = isMac ? '⌘' : 'Ctrl';
                    const modifierCode = isMac ? 'MetaLeft' : 'ControlLeft';

                    // Button shortcuts mapping: elem_id -> {key, shift, alt, shortcut_text}
                    const shortcuts = {
                        'load_btn': {key: 'l', shift: false, alt: false, text: modifierKey + '+L'},
                        'run_analyzer_btn': {key: 'r', shift: false, alt: false, text: modifierKey + '+R'},
                        'run_sql_btn': {key: 'Enter', shift: false, alt: false, text: modifierKey + '+↵'},
                        'export_csv_btn': {key: 'c', shift: true, alt: false, text: modifierKey + '+Shift+C'},
                        'export_json_btn': {key: 'j', shift: true, alt: false, text: modifierKey + '+Shift+J'},
                        'export_parquet_btn': {key: 'p', shift: true, alt: false, text: modifierKey + '+Shift+P'},
                        'export_xlsx_btn': {key: 'e', shift: true, alt: false, text: modifierKey + '+Shift+E'},
                        'new_plugin_btn': {key: 'n', shift: false, alt: false, text: modifierKey + '+N'},
                        'plugin_save_btn': {key: 's', shift: false, alt: false, text: modifierKey + '+S'},
                        'test_plugin_btn': {key: 't', shift: false, alt: false, text: modifierKey + '+T'},
                        'prettify_plugin_btn': {key: 'f', shift: true, alt: false, text: modifierKey + '+Shift+F'},
                        'format_btn': {key: 'f', shift: true, alt: false, text: modifierKey + '+Shift+F'},
                        'save_pattern_btn': {key: 's', shift: false, alt: true, text: modifierKey + '+Alt+S'},
                        'sql_export_csv_btn': {key: 'c', shift: true, alt: false, text: modifierKey + '+Shift+C'},
                        'sql_export_json_btn': {key: 'j', shift: true, alt: false, text: modifierKey + '+Shift+J'},
                        'sql_export_parquet_btn': {key: 'p', shift: true, alt: false, text: modifierKey + '+Shift+P'},
                        'sql_export_xlsx_btn': {key: 'e', shift: true, alt: false, text: modifierKey + '+Shift+E'},
                        'save_template_btn': {key: 's', shift: false, alt: false, text: modifierKey + '+S'},
                        'add_section_btn': {key: 'a', shift: false, alt: false, text: modifierKey + '+A'},
                        'export_report_pdf_btn': {key: 'p', shift: false, alt: true, text: modifierKey + '+Alt+P'},
                        'export_report_md_btn': {key: 'm', shift: false, alt: true, text: modifierKey + '+Alt+M'}
                    };

                    // Add shortcut badges to buttons
                    function addShortcutBadges() {
                        Object.entries(shortcuts).forEach(([elemId, shortcut]) => {
                            // Find buttons by elem_id attribute or by clicking target
                            const findButton = () => {
                                // Try to find by elem_id
                                let btn = document.querySelector(`[elem_id="${elemId}"]`);
                                if (!btn) {
                                    // Try to find by matching text content or class
                                    const buttons = document.querySelectorAll('button');
                                    for (let b of buttons) {
                                        const id = b.getAttribute('elem_id');
                                        if (id === elemId) return b;
                                    }
                                }
                                return btn;
                            };

                            const button = findButton();
                            if (button && !button.querySelector('.kbd-shortcut')) {
                                // Check if button already has content we need to preserve
                                const badge = document.createElement('span');
                                badge.className = 'kbd-shortcut';
                                badge.innerHTML = shortcut.text;
                                badge.setAttribute('aria-label', `Keyboard shortcut: ${shortcut.text}`);

                                // Append badge to button
                                button.appendChild(badge);
                            }
                        });
                    }

                    // Find and click button by elem_id
                    function clickButton(elemId) {
                        const button = document.querySelector(`[elem_id="${elemId}"]`);
                        if (button) {
                            button.click();
                            return true;
                        }
                        return false;
                    }

                    // Handle keyboard events
                    document.addEventListener('keydown', function(event) {
                        // Don't trigger shortcuts when typing in input fields
                        const target = event.target;
                        const tagName = target.tagName.toLowerCase();
                        const isInput = tagName === 'input' || tagName === 'textarea' ||
                                       target.isContentEditable ||
                                       target.classList.contains('cm-content');

                        // Allow Enter key in textareas/inputs to work normally
                        // But allow Ctrl+Enter for SQL execution
                        if (isInput && !(event.key === 'Enter' && event[modifierCode === 'MetaLeft' ? 'metaKey' : 'ctrlKey'])) {
                            return;
                        }

                        // Check each shortcut
                        Object.entries(shortcuts).forEach(([elemId, shortcut]) => {
                            // Skip SQL execution shortcut if not in SQL context
                            if (elemId === 'run_sql_btn' && !target.closest('.cm-content')) {
                                return;
                            }

                            const modifierPressed = event[modifierCode === 'MetaLeft' ? 'metaKey' : 'ctrlKey'];
                            const shiftPressed = event.shiftKey;
                            const altPressed = event.altKey;

                            if (modifierPressed &&
                                shiftPressed === shortcut.shift &&
                                altPressed === shortcut.alt &&
                                event.key.toLowerCase() === shortcut.key.toLowerCase()) {

                                event.preventDefault();
                                event.stopPropagation();

                                // Visual feedback
                                const button = document.querySelector(`[elem_id="${elemId}"]`);
                                if (button) {
                                    button.style.transform = 'scale(0.95)';
                                    setTimeout(() => button.style.transform = '', 100);
                                }

                                clickButton(elemId);
                            }
                        });
                    });

                    // Initialize shortcuts after DOM is ready
                    function initShortcuts() {
                        addShortcutBadges();

                        // Re-add badges when tabs change (Gradio dynamically rebuilds DOM)
                        const observer = new MutationObserver(() => {
                            setTimeout(addShortcutBadges, 100);
                        });

                        // Observe the main container for changes
                        const mainContainer = document.querySelector('.gradio-container');
                        if (mainContainer) {
                            observer.observe(mainContainer, {childList: true, subtree: true});
                        }
                    }

                    // Wait for DOM to be ready
                    if (document.readyState === 'loading') {
                        document.addEventListener('DOMContentLoaded', initShortcuts);
                    } else {
                        initShortcuts();
                    }

                    // Also re-initialize after a short delay to catch dynamically loaded elements
                    setTimeout(initShortcuts, 1000);
                    setTimeout(initShortcuts, 3000);
                })();
            </script>
        """)

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
