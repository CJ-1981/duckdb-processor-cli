"""Template for creating a new analysis module.

To use this template:
    1. Copy this file:  cp _template.py my_analysis.py
    2. Edit the class below — change `name`, `description`, and `run()`.
    3. Run it:         python -m duckdb_processor data.csv --run my_analysis

The ``@register`` decorator adds your class to the global analyzer
registry automatically when this file is imported (which happens at
package init time via ``analysts/__init__.py``).
"""
from duckdb_processor.analyzer import BaseAnalyzer, register


@register
class MyAnalysis(BaseAnalyzer):
    """TODO: Replace with a real description of what this analysis does."""

    name = "my_analysis"
    description = "TODO: One-line summary shown by --list-analyzers"

    def run(self, p):
        """TODO: Implement your business logic here.

        `p` is a duckdb_processor.Processor instance with the loaded data.

        Available methods:
            p.preview(n)          — first n rows
            p.coverage()          — column fill-rate report
            p.sql(query)          — arbitrary SQL → DataFrame
            p.filter(where)       — filtered rows → DataFrame
            p.create_view(name, where) — named view for chaining
            p.add_column(name, expr)   — derived column
            p.aggregate(group_by, field, func) — group-by agg
            p.pivot(row_key, col_key, val, func) — cross-tab
            p.export_csv(path, query)    — write CSV
            p.export_json(path, query)   — write JSON
            p.export_parquet(path, query) — write Parquet
        """
        # Example: explore data
        print(p.preview(5).to_string(index=False))
        print(p.coverage().to_string(index=False))

        # Example: derive and aggregate
        # p.add_column("category", "CASE ... END")
        # result = p.aggregate("region", "amount", "SUM")
        # p.export_csv("my_output.csv")
