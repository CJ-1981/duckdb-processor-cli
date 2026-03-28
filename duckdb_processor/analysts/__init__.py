"""Auto-discover and register all analyzers in this package.

When this subpackage is imported, every module in the same directory
is imported automatically, triggering any ``@register`` decorators
they contain.  This means a single ``import duckdb_processor.analysts``
(or simply ``import duckdb_processor`` from the project root) is
enough to make all analyzers visible to the CLI.
"""
from __future__ import annotations

import importlib
import pkgutil

for _importer, _modname, _ispkg in pkgutil.iter_modules(__path__):
    importlib.import_module(f"{__name__}.{_modname}")
