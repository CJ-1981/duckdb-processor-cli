"""Utility functions for duckdb-processor."""
import sys

def prompt_file_dialog() -> str | None:
    """Ask the user if they want to open a file dialog, then show one if yes.

    Only called when no input file is provided and stdin is an interactive
    terminal (not a pipe).  Returns the selected file path, or ``None`` if
    the user declines or no file is chosen.
    """
    try:
        answer = input("No input file specified. Open file dialog? [y/N] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return None

    if answer not in ("y", "yes"):
        return None

    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()          # Hide the empty root window
        root.attributes("-topmost", True)  # Bring dialog to front
        selected = filedialog.askopenfilename(
            title="Select input file",
            filetypes=[
                ("CSV files", "*.csv"),
                ("TSV files", "*.tsv"),
                ("Parquet files", "*.parquet"),
                ("All files", "*.*"),
            ],
        )
        root.destroy()
        return selected if selected else None
    except Exception as exc:  # tkinter may be unavailable in headless envs
        print(f"File dialog unavailable: {exc}", file=sys.stderr)
        print("Hint: Pass the file path as an argument, e.g.  duckdb-processor data.csv",
              file=sys.stderr)
        return None
