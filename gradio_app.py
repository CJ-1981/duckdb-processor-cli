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

def get_data_health():
    """Fetch coverage and return a Plotly figure."""
    global global_processor
    if global_processor is None:
        return None, "No data loaded."
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
            color_continuous_scale="RdYlGn"
        )
        fig.update_layout(showlegend=False)
        return fig, df
    except Exception as e:
        return None, f"Error calculating health: {e}"

def export_results(format):
    """Export the last result to a file and return the path."""
    global global_processor
    if global_processor is None or global_processor.last_result is None:
        return None
    
    try:
        filename = f"duck_export_{format}.{format}"
        path = os.path.abspath(filename)
        # Handle excel case as special if it doesn't support the generic export
        global_processor.export(path, format=format)
        return path
    except Exception as e:
        logger.error(f"Export error: {e}")
        return None

def generate_auto_chart(df):
    """Attempt to generate a relevant chart from a dataframe."""
    if df is None or df.empty:
        return None
    
    try:
        cols = df.columns
        if len(cols) < 2:
            return None
        
        # Try to find a numeric column for Y and categorical/date for X
        numeric_cols = df.select_dtypes(include=['number', 'float', 'int']).columns.tolist()
        other_cols = [c for c in cols if c not in numeric_cols]
        
        if not numeric_cols:
            # If no numeric, try bar chart of counts for the first column
            counts = df[cols[0]].value_counts().reset_index()
            counts.columns = [cols[0], "count"]
            return px.bar(counts, x=cols[0], y="count", title=f"Frequency of {cols[0]}")
        
        x_col = other_cols[0] if other_cols else cols[0]
        y_col = numeric_cols[0]
        
        # Categorize x types for better labels
        if "date" in x_col.lower() or "time" in x_col.lower():
            fig = px.line(df, x=x_col, y=y_col, title=f"{y_col} over {x_col}")
        else:
            fig = px.bar(df, x=x_col, y=y_col, title=f"{y_col} by {x_col}")
        
        return fig
    except Exception as e:
        logger.error(f"Chart error: {e}")
        return None

def load_data(file_obj, header, kv):
    """Load the CSV into DuckDB via Processor API and return preview."""
    global global_processor
    logger.info(f"Loading data: file={file_obj.name if file_obj else 'None'}, header={header}, kv={kv}")
    if file_obj is None:
        return "Please upload a CSV file.", None
    
    try:
        # Pass file path to config
        config = ProcessorConfig(file=file_obj.name, header=header, kv=kv)
        global_processor = load(config)
        
        info = global_processor.info()
        # Format info nicely
        info_str = json.dumps(info, indent=2)
        
        preview_df = global_processor.preview(100)
        schema_str = get_schema_info()
        health_fig, health_df = get_data_health()
        
        logger.info("Data loaded successfully.")
        return (
            f"✅ Data Loaded Successfully\n\n{info_str}", 
            preview_df, 
            schema_str, 
            health_fig,
            health_df
        )
    except Exception as e:
        error_msg = f"❌ Error loading data: {e}"
        logger.error(f"{error_msg}\n{traceback.format_exc()}")
        return error_msg, None, "Error", None, None

def run_analysis(analyzer_name, max_rows, max_cols):
    """Run the selected analyzer against the loaded processor."""
    global global_processor
    logger.info(f"Running analysis: {analyzer_name}, max_rows={max_rows}, max_cols={max_cols}")
    if global_processor is None:
        return "❌ Error: Please load data first (go to Data Preview tab).", gr.update()
    
    if not analyzer_name:
        return "⚠️ Please select an analyzer from the dropdown.", gr.update()
    
    try:
        analyzer = get_analyzer(analyzer_name)
        analyzer.run(global_processor)
        
        df = global_processor.last_result
        if df is None or df.empty:
            return f"✅ Analyzer '{analyzer_name}' ran successfully, but returned no results.", gr.update(), gr.update()
            
        # Calculate dynamic height
        height_px = int(max_rows) * 35 + 80
        
        # Instead of hiding columns, we force min-width via CSS to control visual "width"
        # If max_cols is 5, we make columns wider; if 50, we make them narrower to fit more.
        col_width = 150 if max_cols == "All" else (1500 // int(max_cols))
        style_injection = f"<style>#analysis-results td, #analysis-results th {{ min-width: {col_width}px !important; }}</style>"
        
        chart_fig = generate_auto_chart(df)
        
        return f"✅ Analyzer '{analyzer_name}' ran successfully!", gr.update(value=df, max_height=height_px), style_injection, chart_fig
    except Exception as e:
        error_msg = f"❌ Error running analyzer: {e}"
        logger.error(error_msg)
        return error_msg, gr.update(), gr.update(), None

def execute_sql(query, max_rows, max_cols):
    """Run arbitrary SQL from the SQL Editor."""
    global global_processor
    logger.info(f"Executing SQL query, max_rows={max_rows}, max_cols={max_cols}")
    if global_processor is None:
        return "❌ Error: Please load data first (go to Data Preview tab).", gr.update()
    
    if not query or not query.strip():
        return "⚠️ Query is empty.", gr.update()
    
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
        
        chart_fig = generate_auto_chart(df)
        
        return (
            f"✅ Query executed successfully! Returned {total_rows} total rows.", 
            gr.update(value=df, max_height=height_px), 
            style_injection,
            gr.update(choices=query_history),
            chart_fig
        )
    except Exception as e:
        error_msg = f"❌ Error executing SQL: {e}"
        logger.error(error_msg)
        return error_msg, gr.update(), gr.update(), gr.update(), None

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
/* Enhance dataframe visibility */
.gradio-dataframe table { border-collapse: collapse; }

/* Custom coloring for SQL code blocks */
.cm-s-default .cm-keyword { color: #d73a49; font-weight: bold; }
.cm-s-default .cm-string { color: #032f62; }
.cm-s-default .cm-variable { color: #005cc5; }
"""

def create_ui():
    theme = gr.themes.Soft(
        primary_hue="blue",
        neutral_hue="slate",
    ).set(
        button_primary_background_fill="*primary_500",
        button_primary_background_fill_hover="*primary_600",
    )
    
    with gr.Blocks(title="DuckDB Processor UI", css=custom_css) as app:
        gr.Markdown("# 🦆 DuckDB CSV Processor")
        gr.Markdown("An interactive dashboard to explore, transform, and analyze your CSV data quickly.")
        
        with gr.Row():
            with gr.Column(scale=1):
                file_input = gr.File(label="Upload CSV File", file_types=[".csv", ".tsv", ".txt"])
                
                with gr.Row():
                    header_check = gr.Checkbox(label="Has Header?", value=True)
                    kv_check = gr.Checkbox(label="Is Key-Value Pairs?", value=False)
                
                load_btn = gr.Button("Load Data", variant="primary")
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
                    # TAB 2: Data Health
                    # -----------------------------
                    with gr.Tab("Data Health"):
                        gr.Markdown("### 🩺 Data Quality & Coverage")
                        health_plot = gr.Plot(label="Column Coverage")
                        health_table = gr.Dataframe(label="Detailed Coverage Stats")

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
                                run_analyzer_btn = gr.Button("▶ Run Analyzer", variant="primary")
                                
                            with gr.Column(scale=1):
                                plugin_upload = gr.File(label="Drop custom plugin (.py)", file_types=[".py"])
                                plugin_status = gr.Textbox(label="Plugin Status", lines=1, interactive=False)
                        
                        analyzer_status = gr.Textbox(label="Status", lines=1, interactive=False)
                        
                        # Export buttons
                        with gr.Row():
                            gr.Markdown("**Export Last Result:**")
                            export_csv_btn = gr.Button("CSV", size="sm")
                            export_json_btn = gr.Button("JSON", size="sm")
                            export_parquet_btn = gr.Button("Parquet", size="sm")
                            export_xlsx_btn = gr.Button("Excel", size="sm")
                        
                        export_download = gr.File(label="Download Exported File", visible=False)
                        
                        analyzer_results = gr.Dataframe(
                            label="Analysis Results",
                            interactive=False,
                            wrap=True,
                            elem_id="analysis-results",
                            max_height=500
                        )
                        analysis_css_override = gr.HTML("")
                        analysis_chart_display = gr.Plot(label="Analysis Auto-Chart")

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
                            run_sql_btn = gr.Button("▶ Run SQL", variant="primary")
                        
                        with gr.Row():
                            row_slider_sql = gr.Dropdown(choices=[15, 25, 50, 100, 200], value=50, label="Rows")
                            col_dropdown_sql = gr.Dropdown(choices=["5", "10", "20", "50", "All"], value="All", label="Cols")
                        
                        sql_status = gr.Textbox(label="Execution Status", lines=1, interactive=False)
                        
                        # Export buttons for SQL
                        with gr.Row():
                            gr.Markdown("**Export Last Result:**")
                            sql_export_csv_btn = gr.Button("CSV", size="sm")
                            sql_export_json_btn = gr.Button("JSON", size="sm")
                            sql_export_parquet_btn = gr.Button("Parquet", size="sm")
                            sql_export_xlsx_btn = gr.Button("Excel", size="sm")
                        
                        sql_export_download = gr.File(label="Download Exported File", visible=False)
                        
                        sql_results = gr.Dataframe(
                            label="Query Results",
                            interactive=False,
                            wrap=True,
                            elem_id="sql-results",
                            max_height=500
                        )
                        sql_css_override = gr.HTML("")
                        sql_chart_display = gr.Plot(label="SQL Auto-Chart")

        # --- Event Listeners ---
        
        # Load Data
        load_btn.click(
            fn=load_data,
            inputs=[file_input, header_check, kv_check],
            outputs=[info_box, preview_table, schema_sidebar, health_plot, health_table]
        )
        
        # Run Analyzer
        run_analyzer_btn.click(
            fn=run_analysis,
            inputs=[analyzer_dropdown, row_slider_analysis, col_dropdown_analysis],
            outputs=[analyzer_status, analyzer_results, analysis_css_override, analysis_chart_display]
        )
        
        # Run SQL
        run_sql_btn.click(
            fn=execute_sql,
            inputs=[sql_input, row_slider_sql, col_dropdown_sql],
            outputs=[sql_status, sql_results, sql_css_override, sql_history_dropdown, sql_chart_display]
        )
        
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
        
        # Plugin Upload
        plugin_upload.upload(
            fn=upload_plugin,
            inputs=[plugin_upload],
            outputs=[plugin_status, analyzer_dropdown]
        )
        
        # Exporting Analysis Results
        def make_analyzer_export(fmt):
            path = export_results(fmt)
            return gr.update(value=path, visible=True) if path else gr.update(visible=False)

        export_csv_btn.click(lambda: make_analyzer_export("csv"), None, export_download)
        export_json_btn.click(lambda: make_analyzer_export("json"), None, export_download)
        export_parquet_btn.click(lambda: make_analyzer_export("parquet"), None, export_download)
        export_xlsx_btn.click(lambda: make_analyzer_export("xlsx"), None, export_download)
        
        # Exporting SQL Results
        def make_sql_export(fmt):
            path = export_results(fmt)
            return gr.update(value=path, visible=True) if path else gr.update(visible=False)

        sql_export_csv_btn.click(lambda: make_sql_export("csv"), None, sql_export_download)
        sql_export_json_btn.click(lambda: make_sql_export("json"), None, sql_export_download)
        sql_export_parquet_btn.click(lambda: make_sql_export("parquet"), None, sql_export_download)
        sql_export_xlsx_btn.click(lambda: make_sql_export("xlsx"), None, sql_export_download)

        # Floating Back to Top Button
        gr.HTML("""
            <button id='back-to-top' onclick='window.scrollTo({top: 0, behavior: "smooth"});'
                    style='position: fixed; bottom: 30px; right: 30px; z-index: 1000; width: 50px; height: 50px; border-radius: 50%; background-color: #2563eb; color: white; border: none; box-shadow: 0 4px 12px rgba(0,0,0,0.15); cursor: pointer; display: flex; align-items: center; justify-content: center; font-size: 20px;'>
                ⬆️
            </button>
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
        debug=True,
        theme=app_theme,
        css=app_css,
    )
