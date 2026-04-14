from duckdb_processor.analyzer import BaseAnalyzer, register

@register
class CustomPlugin(BaseAnalyzer):
    name = "test234"
    description = "Quick-start custom plugin template"

    def run(self, p):
        # Your logic here
        df = p.sql("SELECT * FROM data LIMIT 5")
        print(f"Loaded {len(df)} rows.")
        p.last_result = df