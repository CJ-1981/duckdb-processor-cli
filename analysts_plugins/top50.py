from duckdb_processor.analyzer import BaseAnalyzer, register

@register
class CustomPlugin(BaseAnalyzer):
    name = "top50_analysis"
    description = "Quick-start custom plugin template"

    def run(self, p):
        # Your logic here
        df = p.sql("SELECT * FROM data LIMIT 50")
        print(f"Loaded {len(df)} rows.")
        p.last_result = df