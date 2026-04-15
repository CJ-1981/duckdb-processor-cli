from duckdb_processor.analyzer import BaseAnalyzer, register

@register
class CustomPlugin(BaseAnalyzer):
    name = "상위100"
    description = "Quick-start custom plugin template"

    def run(self, p):
        # Your logic here
        df = p.sql("SELECT * FROM data LIMIT 100")
        print(f"Loaded {len(df)} rows.")
        p.last_result = df