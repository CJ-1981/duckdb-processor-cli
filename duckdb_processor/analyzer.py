"""Base analyzer framework for reusable, composable analysis modules.

Data analysts create analysis scripts by subclassing :class:`BaseAnalyzer`.
Each analyzer is a self-contained unit of business logic that can be:

* Run independently from the CLI (``--run <name>``).
* Chained with other analyzers (``--run step1,step2,step3``).
* Imported and called from notebooks or other Python code.

Creating a new analyzer
-----------------------
Save a file like ``analysts/my_report.py``::

    from duckdb_processor.analyzer import BaseAnalyzer, register

    @register
    class MyReport(BaseAnalyzer):
        name = "my_report"
        description = "Weekly sales summary by region"

        def run(self, p):
            # All business logic lives here.
            # ``p`` is a duckdb_processor.Processor instance.
            p.add_column("tier", "CASE ...")
            result = p.aggregate("region", "amount", "SUM")
            p.export_csv("weekly_summary.csv")

The ``@register`` decorator adds the class to a global registry so the
CLI can discover it automatically.

Listing available analyzers
----------------------------
``python -m duckdb_processor --list-analyzers``
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .processor import Processor


# ── Abstract base ────────────────────────────────────────────


class BaseAnalyzer(ABC):
    """Abstract base class for analysis modules.

    Subclass this, set :attr:`name` and :attr:`description`, and
    implement :meth:`run` to create a reusable analysis.

    Attributes
    ----------
    name:
        Unique identifier used on the CLI (``--run <name>``).
    description:
        One-line summary shown by ``--list-analyzers``.
    """

    name: str = ""
    description: str = ""

    @abstractmethod
    def run(self, p: Processor) -> None:
        """Execute the analysis.

        Parameters
        ----------
        p:
            A :class:`~duckdb_processor.processor.Processor` with the
            loaded data.  Use ``p.sql()``, ``p.filter()``,
            ``p.aggregate()``, ``p.export_csv()``, etc.
        """
        ...


# ── Global registry ──────────────────────────────────────────

_registry: dict[str, type[BaseAnalyzer]] = {}


def register(cls: type[BaseAnalyzer]) -> type[BaseAnalyzer]:
    """Class decorator that registers an analyzer by its ``name``.

    Raises
    ------
    ValueError
        If the class does not define a non-empty ``name``.
    """
    instance = cls()
    if not instance.name:
        raise ValueError(
            f"{cls.__name__} must define a non-empty 'name' class attribute"
        )
    _registry[instance.name] = cls
    return cls


def get_analyzer(name: str) -> BaseAnalyzer:
    """Instantiate a registered analyzer by name.

    Raises
    ------
    KeyError
        If *name* is not in the registry (message lists available names).
    """
    if name not in _registry:
        available = ", ".join(sorted(_registry)) or "(none registered)"
        raise KeyError(
            f"Analyzer '{name}' not found. Available: {available}"
        )
    return _registry[name]()


def list_analyzers() -> list[dict]:
    """Return metadata dicts for every registered analyzer."""
    return [
        {"name": name, "description": cls().description}
        for name, cls in sorted(_registry.items())
    ]


def run_analyzers(p: Processor, names: list[str]) -> None:
    """Execute one or more named analyzers in sequence.

    Each analyzer's :meth:`~BaseAnalyzer.run` receives the same
    :class:`Processor` instance, so earlier analyzers can create
    columns or views that later ones depend on.
    """
    for name in names:
        try:
            analyzer = get_analyzer(name)
        except KeyError as e:
            import sys
            print(f"\n❌ Error: {e}", file=sys.stderr)
            print(f"\n💡 Tip: Use --list-analyzers to see all available analyzers", file=sys.stderr)
            print(f"   Example: python -m duckdb_processor data.csv --list-analyzers\n", file=sys.stderr)
            continue

        desc = analyzer.description or ""
        print(f"\n{'─' * 58}")
        print(f"  [{name}] {desc}")
        print(f"{'─' * 58}")
        analyzer.run(p)
