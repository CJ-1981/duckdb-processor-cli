import gradio as gr
import pandas as pd
import sqlparse
import json

from duckdb_processor.loader import load
from duckdb_processor.config import ProcessorConfig
from duckdb_processor.analyzer import list_analyzers, get_analyzer

# Global state for a single-user local app
global_processor = None

def load_data(file_obj, header, kv):
    """Load the CSV into DuckDB via Processor API and return preview."""
    global global_processor
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
        return f"✅ Data Loaded Successfully\n\n{info_str}", preview_df
    except Exception as e:
        return f"❌ Error loading data: {e}", None

def run_analysis(analyzer_name):
    """Run the selected analyzer against the loaded processor."""
    global global_processor
    if global_processor is None:
        return "❌ Error: Please load data first (go to Data Preview tab).", None
    
    if not analyzer_name:
        return "⚠️ Please select an analyzer from the dropdown.", None
    
    try:
        analyzer = get_analyzer(analyzer_name)
        analyzer.run(global_processor)
        
        df = global_processor.last_result
        if df is None or df.empty:
            return f"✅ Analyzer '{analyzer_name}' ran successfully, but returned no results.", None
        
        return f"✅ Analyzer '{analyzer_name}' ran successfully! Showing results:", df
    except Exception as e:
        return f"❌ Error running analyzer: {e}", None

def execute_sql(query):
    """Run arbitrary SQL from the SQL Editor."""
    global global_processor
    if global_processor is None:
        return "❌ Error: Please load data first (go to Data Preview tab).", None
    
    if not query or not query.strip():
        return "⚠️ Query is empty.", None
    
    try:
        df = global_processor.sql(query)
        return f"✅ Query executed successfully! Returned {len(df)} rows.", df
    except Exception as e:
        return f"❌ Error executing SQL: {e}", None

def prettify_sql(query):
    """Format SQL query using sqlparse."""
    if not query or not query.strip():
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

# Custom CSS for UI polish
custom_css = """
/* Enhance dataframe visibility and allow resizing */
.gradio-dataframe table { border-collapse: collapse; }
.gradio-dataframe { resize: vertical; overflow: auto; min-height: 400px; }

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
    
    with gr.Blocks(title="DuckDB Processor UI") as app:
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
            
            with gr.Column(scale=3):
                with gr.Tabs():
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
                        load_btn.click(
                            fn=load_data,
                            inputs=[file_input, header_check, kv_check],
                            outputs=[info_box, preview_table]
                        )

                    # -----------------------------
                    # TAB 2: Analysts
                    # -----------------------------
                    with gr.Tab("Run Analytics"):
                        gr.Markdown("### 📈 Built-in Analyzers")
                        gr.Markdown("Select an analyst script to run complex data transformations.")
                        
                        analyzer_choices = get_analyzer_choices()
                        analyzer_dropdown = gr.Dropdown(
                            choices=analyzer_choices, 
                            label="Select Analyzer", 
                            info="These reflect the scripts found in analysts/"
                        )
                        run_analyzer_btn = gr.Button("▶ Run Analyzer", variant="primary")
                        
                        analyzer_status = gr.Textbox(label="Status", lines=1, interactive=False)
                        analyzer_results = gr.Dataframe(
                            label="Analysis Results",
                            interactive=False,
                            wrap=True,
                            row_count=(15, "dynamic")
                        )
                        
                        run_analyzer_btn.click(
                            fn=run_analysis,
                            inputs=[analyzer_dropdown],
                            outputs=[analyzer_status, analyzer_results]
                        )

                    # -----------------------------
                    # TAB 3: SQL Editor
                    # -----------------------------
                    with gr.Tab("SQL Editor"):
                        gr.Markdown("### 💻 Custom SQL Query")
                        gr.Markdown("Run DuckDB SQL directly. The loaded table is available as `data`.")
                        
                        sql_input = gr.Code(
                            language="sql",
                            lines=10,
                            label="Query Editor",
                            value="SELECT * FROM data LIMIT 10;",
                            interactive=True
                        )
                        
                        with gr.Row():
                            format_btn = gr.Button("✨ Prettify SQL")
                            run_sql_btn = gr.Button("▶ Run SQL", variant="primary")
                        
                        sql_status = gr.Textbox(label="Execution Status", lines=2, interactive=False)
                        sql_results = gr.Dataframe(
                            label="Query Results",
                            interactive=False,
                            wrap=True,
                            row_count=(15, "dynamic")
                        )
                        
                        format_btn.click(
                            fn=prettify_sql,
                            inputs=[sql_input],
                            outputs=[sql_input]
                        )
                        
                        run_sql_btn.click(
                            fn=execute_sql,
                            inputs=[sql_input],
                            outputs=[sql_status, sql_results]
                        )

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
