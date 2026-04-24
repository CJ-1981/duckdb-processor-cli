import os
import re
import tkinter as tk
from tkinter import filedialog, ttk, messagebox
import subprocess
import threading
import sys
import shlex
import json
import pandas as pd

# ---------------------------------------------------------------------------
# Runtime path fix: when bundled as a --onefile .exe PyInstaller extracts
# everything to sys._MEIPASS. Add that directory to sys.path so that
# vdn_compare.py (bundled via --add-data) is importable as a normal module.
# ---------------------------------------------------------------------------
_bundle_dir = (
    getattr(sys, '_MEIPASS', None)
    or os.path.dirname(os.path.abspath(__file__))
)
if _bundle_dir not in sys.path:
    sys.path.insert(0, _bundle_dir)

# When the GUI spawns itself as a CLI subprocess it sets VDN_NO_PAUSE=1 so
# that vdn_compare.main() never blocks on input().
_NO_PAUSE = os.environ.get('VDN_NO_PAUSE', '0') == '1'
if _NO_PAUSE:
    import builtins
    builtins.input = lambda prompt='': ''

# Strip ANSI escape codes produced by Rich before displaying in the Tk widget.
_ANSI_RE = re.compile(r'\x1b\[[0-9;]*[mGKHFABCDJK]|\x1b\].*?(?:\x07|\x1b\\)')

class ToolTip:
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tooltip_window = None
        self.id = None
        self.widget.bind("<Enter>", self.enter)
        self.widget.bind("<Leave>", self.leave)

    def enter(self, event=None):
        self.schedule()

    def leave(self, event=None):
        self.unschedule()
        self.hide()

    def schedule(self):
        self.unschedule()
        self.id = self.widget.after(500, self.show)

    def unschedule(self):
        id_ = self.id
        self.id = None
        if id_:
            self.widget.after_cancel(id_)

    def show(self, event=None):
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + 20
        self.tooltip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=self.text, justify='left',
                         background="#ffffe0", relief='solid', borderwidth=1,
                         font=("tahoma", "8", "normal"))
        label.pack(ipadx=1)

    def hide(self):
        tw = self.tooltip_window
        self.tooltip_window = None
        if tw:
            tw.destroy()

def add_tooltip(widget, text):
    ToolTip(widget, text)

class VDNCompareGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("VDN Compare GUI")
        self.root.geometry("800x900")

        # Internal Roles for mapping
        self.roles = {
            "vin": "VIN",
            "sw": "CONSUMER_SW_VERSION",
            "vdn": "VDN_LIST",
            "model": "MODEL",
            "region": "REGION"
        }
        
        # Mapping storage: role -> {s1: col, s2: col}
        self.mapping_vars = {}
        # Initial roles
        self.initial_roles = ["vin", "sw", "vdn", "model", "region"]
        for role in self.initial_roles:
            self.add_role_vars(role)

        # Output Formats storage
        self.format_options = ["rich", "md", "html", "csv"]
        self.format_vars = {fmt: tk.BooleanVar(value=True) for fmt in self.format_options}

        # Notebook for Tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Tab 1: Run Settings
        self.run_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.run_tab, text="Run Settings")
        
        # Tab 2: Column Settings
        self.col_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.col_tab, text="Column Settings")

        self.setup_run_tab()
        self.setup_col_tab()
        
        # Traces for auto-loading headers
        self.s1_var.trace_add("write", lambda *args: self.on_file_change("s1"))
        self.s2_var.trace_add("write", lambda *args: self.on_file_change("s2"))
        
        # Initial load if defaults exist
        self.root.after(500, lambda: self.on_file_change("s1"))
        self.root.after(600, lambda: self.on_file_change("s2"))
        
        # Load configuration from file
        self.load_config()

    def setup_run_tab(self):
        # Layout
        self.main_frame = ttk.Frame(self.run_tab, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)

        # Source 1
        lbl_s1 = ttk.Label(self.main_frame, text="Source 1 File:")
        lbl_s1.grid(row=0, column=0, sticky=tk.W, pady=5)
        self.s1_var = tk.StringVar(value="input/DB.csv")
        ent_s1 = ttk.Entry(self.main_frame, textvariable=self.s1_var, width=50)
        ent_s1.grid(row=0, column=1, pady=5)
        ttk.Button(self.main_frame, text="Browse", command=lambda: self.browse_file(self.s1_var)).grid(row=0, column=2, padx=5, pady=5)
        add_tooltip(lbl_s1, "The first data file (e.g., DB.csv). Serves as the primary reference for comparison.")

        # Source 2
        lbl_s2 = ttk.Label(self.main_frame, text="Source 2 File:")
        lbl_s2.grid(row=1, column=0, sticky=tk.W, pady=5)
        self.s2_var = tk.StringVar(value="input/PIE.csv")
        ent_s2 = ttk.Entry(self.main_frame, textvariable=self.s2_var, width=50)
        ent_s2.grid(row=1, column=1, pady=5)
        ttk.Button(self.main_frame, text="Browse", command=lambda: self.browse_file(self.s2_var)).grid(row=1, column=2, padx=5, pady=5)
        add_tooltip(lbl_s2, "The second data file (e.g., PIE.csv) to compare against Source 1.")

        # Config File
        lbl_cfg = ttk.Label(self.main_frame, text="Config File:")
        lbl_cfg.grid(row=2, column=0, sticky=tk.W, pady=5)
        self.config_var = tk.StringVar(value="config.json")
        ent_cfg = ttk.Entry(self.main_frame, textvariable=self.config_var, width=50)
        ent_cfg.grid(row=2, column=1, pady=5)
        ttk.Button(self.main_frame, text="Browse", command=lambda: self.browse_file(self.config_var)).grid(row=2, column=2, padx=5, pady=5)
        add_tooltip(lbl_cfg, "Path to a JSON file for custom configuration and column mapping.")

        # Hidden Compare Columns variable (synced from Column Settings tab)
        self.compare_var = tk.StringVar(value="all")

        # Sort VIN
        lbl_sort = ttk.Label(self.main_frame, text="Sort VIN:")
        lbl_sort.grid(row=3, column=0, sticky=tk.W, pady=5)
        self.sort_vin_var = tk.StringVar(value="asc")
        sort_cb = ttk.Combobox(self.main_frame, textvariable=self.sort_vin_var, values=("none", "asc", "desc"), state="readonly", width=47)
        sort_cb.grid(row=3, column=1, pady=5)
        add_tooltip(lbl_sort, "Sort the output records by VIN (none respects input order).")

        # Formats
        lbl_fmt = ttk.Label(self.main_frame, text="Formats:")
        lbl_fmt.grid(row=4, column=0, sticky=tk.W, pady=5)
        
        fmt_frame = ttk.Frame(self.main_frame)
        fmt_frame.grid(row=4, column=1, sticky=tk.W, pady=5)
        
        for fmt in self.format_options:
            cb = ttk.Checkbutton(fmt_frame, text=fmt.upper(), variable=self.format_vars[fmt])
            cb.pack(side=tk.LEFT, padx=(0, 10))
            
        add_tooltip(lbl_fmt, "Format(s) for summary output. Select multiple to generate different report types.")

        # Samples
        lbl_samp = ttk.Label(self.main_frame, text="Samples:")
        lbl_samp.grid(row=5, column=0, sticky=tk.W, pady=5)
        self.samples_var = tk.StringVar(value="10")
        ent_samp = ttk.Entry(self.main_frame, textvariable=self.samples_var, width=50)
        ent_samp.grid(row=5, column=1, pady=5)
        ttk.Label(self.main_frame, text="(integer or 'all')").grid(row=5, column=2, sticky=tk.W, pady=5)
        add_tooltip(lbl_samp, "Number of samples to show in the summary report (integer or 'all').")

        # Normalize Models
        lbl_nm = ttk.Label(self.main_frame, text="Normalize Models:")
        lbl_nm.grid(row=6, column=0, sticky=tk.W, pady=5)
        self.norm_models_var = tk.StringVar(value='"EX30,V216" "EX30 CC,V216-CC"')
        ent_nm = ttk.Entry(self.main_frame, textvariable=self.norm_models_var, width=50)
        ent_nm.grid(row=6, column=1, pady=5)
        add_tooltip(lbl_nm, 'Groups of equivalent models, space-separated groupings.\nUse comma inside groups. e.g. "EX30,V216" "PS4,P417"\nThe first item becomes the primary display name.')
        
        # Normalize SW
        lbl_nsw = ttk.Label(self.main_frame, text="Normalize SW:")
        lbl_nsw.grid(row=7, column=0, sticky=tk.W, pady=5)
        self.norm_sw_var = tk.StringVar(value='"MY27 J1,27 J1"')
        ent_nsw = ttk.Entry(self.main_frame, textvariable=self.norm_sw_var, width=50)
        ent_nsw.grid(row=7, column=1, pady=5)
        add_tooltip(lbl_nsw, 'Groups of equivalent SW versions. Use quotes if spaces exist within group names.\nExample: "MY27 J1,27 J1" "1.8.0,1.8.0-hotfix"')
        
        # Normalize Custom
        lbl_nc = ttk.Label(self.main_frame, text="Normalize Custom JSON:")
        lbl_nc.grid(row=8, column=0, sticky=tk.W, pady=5)
        self.norm_custom_var = tk.StringVar(value="{}")
        ent_nc = ttk.Entry(self.main_frame, textvariable=self.norm_custom_var, width=50)
        ent_nc.grid(row=8, column=1, pady=5)
        add_tooltip(lbl_nc, 'Custom normalization rules in JSON mapping column names to lists of equivalent groups.')

        # Skip Filter
        lbl_sf = ttk.Label(self.main_frame, text="Skip Filter JSON:")
        lbl_sf.grid(row=9, column=0, sticky=tk.W, pady=5)
        self.skip_filter_var = tk.StringVar(value="{}")
        ent_sf = ttk.Entry(self.main_frame, textvariable=self.skip_filter_var, width=50)
        ent_sf.grid(row=9, column=1, pady=5)
        add_tooltip(lbl_sf, 'Values to skip/exclude, in JSON format: {"ColumnName": ["Value1", "Value2"]}.')

        # Checkboxes for flags
        flags_frame = ttk.Frame(self.main_frame)
        flags_frame.grid(row=10, column=0, columnspan=3, sticky=tk.W, pady=5)
        
        self.skip_nodata_var = tk.BooleanVar(value=True)
        cb_nodata = ttk.Checkbutton(flags_frame, text="Skip Rows with No Data", variable=self.skip_nodata_var)
        cb_nodata.pack(side=tk.LEFT, padx=(0, 10))
        add_tooltip(cb_nodata, 'Skip any row that has missing data in any of the compared columns.')

        self.pager_var = tk.BooleanVar(value=False)
        cb_pager = ttk.Checkbutton(flags_frame, text="Use Pager for Output", variable=self.pager_var)
        cb_pager.pack(side=tk.LEFT)
        add_tooltip(cb_pager, 'Use a less command-line pager for table output.')

        # Run & Save Buttons
        btn_frame = ttk.Frame(self.main_frame)
        btn_frame.grid(row=11, column=0, columnspan=3, pady=10)
        
        self.run_btn = ttk.Button(btn_frame, text="Run Comparison", command=self.run_script)
        self.run_btn.pack(side=tk.LEFT, padx=5)
        
        self.save_btn = ttk.Button(btn_frame, text="Save to config.json", command=self.save_full_config)
        self.save_btn.pack(side=tk.LEFT, padx=5)

        # Output text area
        output_frame = ttk.Frame(self.main_frame)
        output_frame.grid(row=13, column=0, columnspan=3, pady=5, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.main_frame.rowconfigure(13, weight=1)
        self.main_frame.columnconfigure(1, weight=1)

        self.output_text = tk.Text(output_frame, height=12, width=75, wrap=tk.WORD)
        self.output_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(output_frame, command=self.output_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.output_text.config(yscrollcommand=scrollbar.set)

    def add_role_vars(self, role_id):
        self.mapping_vars[role_id] = {
            "s1": tk.StringVar(),
            "s2": tk.StringVar(),
            "compare": tk.BooleanVar(value=True if role_id != "vin" else False)
        }

    def setup_col_tab(self):
        self.col_frame = ttk.Frame(self.col_tab, padding="10")
        self.col_frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(self.col_frame, text="Map your file columns to comparison roles:", font=("", 10, "bold")).grid(row=0, column=0, columnspan=4, sticky=tk.W, pady=(0, 10))

        self.s1_headers = []
        self.s2_headers = []
        self.combos = {}
        self.role_rows = []

        self.mapping_grid_frame = ttk.Frame(self.col_frame)
        self.mapping_grid_frame.grid(row=1, column=0, columnspan=4, sticky=tk.NSEW)
        
        # Configure columns to have consistent widths
        self.mapping_grid_frame.columnconfigure(0, weight=1, minsize=100)
        self.mapping_grid_frame.columnconfigure(1, weight=2, minsize=200)
        self.mapping_grid_frame.columnconfigure(2, weight=2, minsize=200)
        self.mapping_grid_frame.columnconfigure(3, weight=1, minsize=80)

        self.render_mapping_rows()

        # Buttons
        self.btn_area = ttk.Frame(self.col_frame)
        self.btn_area.grid(row=2, column=0, columnspan=4, pady=20)
        
        ttk.Button(self.btn_area, text="Add Custom Role", command=self.add_custom_role).pack(side=tk.LEFT, padx=5)
        ttk.Button(self.btn_area, text="Refresh Headers", command=self.refresh_all_headers).pack(side=tk.LEFT, padx=5)
        ttk.Button(self.btn_area, text="Save to config.json", command=self.save_full_config).pack(side=tk.LEFT, padx=5)

        ttk.Label(self.col_frame, text="* VIN is required for joining files.\nColumns marked 'Compare?' are automatically used for comparison.", 
                  font=("", 8, "italic"), justify=tk.LEFT).grid(row=3, column=0, columnspan=4, sticky=tk.W, pady=10)

    def render_mapping_rows(self):
        # Clear existing rows in grid frame
        for widget in self.mapping_grid_frame.winfo_children():
            widget.destroy()
        
        # Re-add Headers into the same grid
        ttk.Label(self.mapping_grid_frame, text="Role", font=("", 9, "bold")).grid(row=0, column=0, sticky=tk.W, padx=5, pady=(0, 5))
        ttk.Label(self.mapping_grid_frame, text="Source 1 Column", font=("", 9, "bold")).grid(row=0, column=1, sticky=tk.W, padx=5, pady=(0, 5))
        ttk.Label(self.mapping_grid_frame, text="Source 2 Column", font=("", 9, "bold")).grid(row=0, column=2, sticky=tk.W, padx=5, pady=(0, 5))
        ttk.Label(self.mapping_grid_frame, text="Compare?", font=("", 9, "bold")).grid(row=0, column=3, sticky=tk.W, padx=5, pady=(0, 5))

        self.combos = {}
        row_idx = 1
        for role_id in self.mapping_vars.keys():
            display_name = role_id.upper()
            ttk.Label(self.mapping_grid_frame, text=f"{display_name} {'*' if role_id=='vin' else ''}:").grid(row=row_idx, column=0, sticky=tk.W, padx=5, pady=2)
            
            c1 = ttk.Combobox(self.mapping_grid_frame, textvariable=self.mapping_vars[role_id]["s1"], width=25)
            c1.grid(row=row_idx, column=1, padx=5, pady=2)
            c1['values'] = self.s1_headers
            
            c2 = ttk.Combobox(self.mapping_grid_frame, textvariable=self.mapping_vars[role_id]["s2"], width=25)
            c2.grid(row=row_idx, column=2, padx=5, pady=2)
            c2['values'] = self.s2_headers
            
            self.combos[role_id] = (c1, c2)

            if role_id != "vin":
                cb = ttk.Checkbutton(self.mapping_grid_frame, variable=self.mapping_vars[role_id]["compare"], command=self.sync_compare_var)
                cb.grid(row=row_idx, column=3, padx=5, pady=2)
            
            row_idx += 1

    def add_custom_role(self):
        # Simple dialog to ask for role name
        dialog = tk.Toplevel(self.root)
        dialog.title("Add Custom Role")
        dialog.geometry("300x150")
        ttk.Label(dialog, text="Enter internal role name (e.g. status):").pack(pady=10)
        entry = ttk.Entry(dialog)
        entry.pack(pady=5)
        
        def confirm():
            role_id = entry.get().strip().lower()
            if role_id and role_id not in self.mapping_vars:
                # Add to roles dict
                self.roles[role_id] = role_id.upper()
                self.add_role_vars(role_id)
                self.render_mapping_rows()
                dialog.destroy()
            else:
                messagebox.showwarning("Invalid Name", "Role name must be unique and not empty.")

        ttk.Button(dialog, text="OK", command=confirm).pack(pady=10)

    def on_file_change(self, source_key):
        path = self.s1_var.get() if source_key == "s1" else self.s2_var.get()
        headers = self.get_file_headers(path)
        if source_key == "s1":
            self.s1_headers = headers
        else:
            self.s2_headers = headers
        self.update_combos()

    def refresh_all_headers(self):
        self.on_file_change("s1")
        self.on_file_change("s2")

    def get_file_headers(self, path):
        if not path or not os.path.exists(path):
            return []
        try:
            ext = os.path.splitext(path)[1].lower()
            if ext == '.csv':
                with open(path, 'r', encoding='utf-8-sig') as f:
                    line = f.readline()
                sep = ';' if ';' in line else ('\t' if '\t' in line else ',')
                df = pd.read_csv(path, sep=sep, nrows=0, encoding='utf-8-sig')
                return df.columns.tolist()
            elif ext in ('.xlsx', '.xls'):
                df = pd.read_excel(path, nrows=0)
                return df.columns.tolist()
        except Exception as e:
            print(f"Error reading headers from {path}: {e}")
        return []

    def update_combos(self):
        for role_id, (c1, c2) in self.combos.items():
            c1['values'] = self.s1_headers
            c2['values'] = self.s2_headers
            
            # 1. Clear current S1 selection if it's no longer in the headers
            curr_s1 = self.mapping_vars[role_id]["s1"].get()
            if curr_s1 and curr_s1 not in self.s1_headers:
                self.mapping_vars[role_id]["s1"].set("")
                
            # 2. Clear current S2 selection if it's no longer in the headers
            curr_s2 = self.mapping_vars[role_id]["s2"].get()
            if curr_s2 and curr_s2 not in self.s2_headers:
                self.mapping_vars[role_id]["s2"].set("")

            # 3. Auto-select if exact match found and current is empty
            if not self.mapping_vars[role_id]["s1"].get():
                matches = [h for h in self.s1_headers if h.lower() == role_id.lower() or h.upper() == self.roles[role_id]]
                if matches: self.mapping_vars[role_id]["s1"].set(matches[0])
            
            if not self.mapping_vars[role_id]["s2"].get():
                matches = [h for h in self.s2_headers if h.lower() == role_id.lower() or h.upper() == self.roles[role_id]]
                if matches: self.mapping_vars[role_id]["s2"].set(matches[0])

    def sync_compare_var(self):
        selected = []
        for role_id in self.roles:
            if role_id != "vin" and self.mapping_vars[role_id]["compare"].get():
                selected.append(role_id)
        if not selected:
            self.compare_var.set("all")
        else:
            self.compare_var.set(" ".join(selected))

    def apply_mapping(self):
        # We don't change the CLI logic, but we update the compare_var 
        # and ensure the config.json will be saved correctly if they run.
        self.sync_compare_var()
        messagebox.showinfo("Mapping Applied", "Column mapping applied to Run Settings.\nNote: Mapping will be passed via the config file.")

    def save_full_config(self):
        self.sync_compare_var()
        config_path = self.config_var.get()
        
        # Build maps
        s1_map = {}
        s2_map = {}
        for role_id, role_internal in self.roles.items():
            s1_col = self.mapping_vars[role_id]["s1"].get()
            s2_col = self.mapping_vars[role_id]["s2"].get()
            if s1_col: s1_map[s1_col] = role_internal
            if s2_col: s2_map[s2_col] = role_internal

        # Build compare list
        compare_list = self.compare_var.get().split()

        config = {
            "compare": compare_list,
            "normalize_models": shlex.split(self.norm_models_var.get().strip()) if self.norm_models_var.get().strip() else [],
            "normalize_sw": shlex.split(self.norm_sw_var.get().strip()) if self.norm_sw_var.get().strip() else [],
            "format": [fmt for fmt, var in self.format_vars.items() if var.get()],
            "samples": self.samples_var.get().strip(),
            "sort_vin": self.sort_vin_var.get(),
            "use_default_input": True, 
            "s1_map": s1_map,
            "s2_map": s2_map,
            "skip_nodata": self.skip_nodata_var.get(),
            "normalize_custom": json.loads(self.norm_custom_var.get()) if self.norm_custom_var.get().strip() != "{}" else {}
        }
        
        try:
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4)
            messagebox.showinfo("Config Saved", f"Successfully saved to {config_path}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save config: {e}")

    def browse_file(self, var):
        filename = filedialog.askopenfilename(
            title="Select file",
            filetypes=[("CSV/Excel files", "*.csv *.xlsx *.xls"), ("JSON files", "*.json"), ("All files", "*.*")]
        )
        if filename:
            var.set(filename)

    def run_script(self):
        # Synchronize compare selections from checkboxes
        self.sync_compare_var()
        # Always save mapping to the current config file before running
        # so that the CLI subprocess picks up the UI-configured mapping.
        self.save_full_config_silent()
        
        self.run_btn.config(state=tk.DISABLED)
        self.output_text.delete(1.0, tk.END)
        self.output_text.insert(tk.END, "Running...\n")
        
        # Build command: re-invoke this exe/script with CLI args.
        # The __main__ block dispatches to vdn_compare.main() when argv[1:] is present.
        if getattr(sys, 'frozen', False):
            cmd = [sys.executable]          # frozen .exe re-invokes itself
        else:
            cmd = [sys.executable, __file__] # dev: re-invoke this script
            
        cmd.extend([
            "-s1", self.s1_var.get(),
            "-s2", self.s2_var.get(),
            "--config", self.config_var.get(),
            "--compare"
        ])
        cmd.extend(self.compare_var.get().split())
        
        cmd.extend(["--sort-vin", self.sort_vin_var.get()])
        
        cmd.extend(["--samples", self.samples_var.get().strip()])
        
        if self.norm_models_var.get().strip():
            cmd.extend(["--normalize-models"])
            # Split models using shlex-like parsing or just default python split
            # Using simple split by spaces handles "EX30,V216 EX30 CC,V216-CC"
            cmd.extend(shlex.split(self.norm_models_var.get().strip()))
            
        if self.norm_sw_var.get().strip():
            cmd.extend(["--normalize-sw"])
            cmd.extend(shlex.split(self.norm_sw_var.get().strip()))
            
        if self.norm_custom_var.get().strip():
            cmd.extend(["--normalize-custom", self.norm_custom_var.get().strip()])
            
        if self.skip_filter_var.get().strip():
            cmd.extend(["--skip-filter", self.skip_filter_var.get().strip()])

        # Always add --use-default-input so the CLI doesn't pop up its own file dialog
        cmd.append("--use-default-input")
        
        if self.skip_nodata_var.get():
            cmd.append("--skip-nodata")
        
        # Add selected formats
        selected_fmts = [fmt for fmt, var in self.format_vars.items() if var.get()]
        if selected_fmts:
            cmd.append("--format")
            cmd.extend(selected_fmts)

        if self.pager_var.get():
            cmd.append("--pager")

        # Pass VDN_NO_PAUSE so the CLI subprocess never blocks on input()
        _env = os.environ.copy()
        _env['VDN_NO_PAUSE'] = '1'

        def run_thread():
            try:
                # Built with --console so the parent already has a console window;
                # suppress a *second* console for the child process on Windows.
                creationflags = 0
                if sys.platform == "win32":
                    creationflags = subprocess.CREATE_NO_WINDOW
                    
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                    creationflags=creationflags,
                    env=_env,
                )
                
                for line in process.stdout:
                    self.root.after(0, self.append_output, line)
                
                process.wait()
                self.root.after(0, self.append_output, f"\nProcess finished with exit code {process.returncode}\n")
            except Exception as e:
                self.root.after(0, self.append_output, f"Error running script: {str(e)}\n")
            finally:
                self.root.after(0, lambda: self.run_btn.config(state=tk.NORMAL))

        threading.Thread(target=run_thread, daemon=True).start()

    def append_output(self, text):
        # Strip ANSI colour codes produced by Rich before inserting into Tk widget
        clean = _ANSI_RE.sub('', text)
        self.output_text.insert(tk.END, clean)
        self.output_text.see(tk.END)

    def load_config(self):
        config_path = self.config_var.get()
        if not os.path.exists(config_path):
            return
            
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Formats
            if "format" in config:
                saved_fmts = [f.lower() for f in config["format"]]
                for fmt in self.format_options:
                    self.format_vars[fmt].set(fmt in saved_fmts)
            
            # Main settings
            if "samples" in config: self.samples_var.set(str(config["samples"]))
            if "sort_vin" in config: self.sort_vin_var.set(config["sort_vin"])
            if "skip_nodata" in config: self.skip_nodata_var.set(config["skip_nodata"])
            
            # Normalization
            if "normalize_models" in config:
                models = config["normalize_models"]
                if isinstance(models, list):
                    # Force quotes around every item for consistency as requested
                    self.norm_models_var.set(" ".join(f'"{m}"' for m in models))
                else:
                    self.norm_models_var.set(str(models))
                    
            if "normalize_sw" in config:
                sw = config["normalize_sw"]
                if isinstance(sw, list):
                    self.norm_sw_var.set(" ".join(f'"{s}"' for s in sw))
                else:
                    self.norm_sw_var.set(str(sw))

            if "normalize_custom" in config:
                self.norm_custom_var.set(json.dumps(config["normalize_custom"]))
                
            # Column Mapping
            s1_map = config.get("s1_map", {})
            s2_map = config.get("s2_map", {})
            
            # Also check shared column_map
            shared_map = config.get("column_map", {})
            
            # Inverse mapping: internal_role -> column_name
            # Since multiple roles might map to one column, we need to be careful.
            # But here roles are keys in self.roles.
            
            for role_id, internal_name in self.roles.items():
                # Check S1
                s1_col = next((col for col, role in s1_map.items() if role == internal_name), 
                              next((col for col, role in shared_map.items() if role == internal_name), None))
                if s1_col: self.mapping_vars[role_id]["s1"].set(s1_col)
                
                # Check S2
                s2_col = next((col for col, role in s2_map.items() if role == internal_name), 
                              next((col for col, role in shared_map.items() if role == internal_name), None))
                if s2_col: self.mapping_vars[role_id]["s2"].set(s2_col)
            
            # Compare selection
            if "compare" in config:
                compare_list = [c.lower() for c in config["compare"]]
                if "all" in compare_list:
                    for r in self.mapping_vars:
                        if r != "vin": self.mapping_vars[r]["compare"].set(True)
                    self.compare_var.set("all")
                else:
                    for r in self.mapping_vars:
                        if r != "vin": self.mapping_vars[r]["compare"].set(r in compare_list)
                    self.sync_compare_var()

        except Exception as e:
            print(f"Error loading config: {e}")

    def save_full_config_silent(self):
        # Synchronize compare selections
        self.sync_compare_var()
        # Same as save_full_config but without message box
        config_path = self.config_var.get()
        s1_map = {}
        s2_map = {}
        for role_id, role_internal in self.roles.items():
            s1_col = self.mapping_vars[role_id]["s1"].get()
            s2_col = self.mapping_vars[role_id]["s2"].get()
            if s1_col: s1_map[s1_col] = role_internal
            if s2_col: s2_map[s2_col] = role_internal

        config = {
            "compare": self.compare_var.get().split(),
            "normalize_models": shlex.split(self.norm_models_var.get().strip()) if self.norm_models_var.get().strip() else [],
            "normalize_sw": shlex.split(self.norm_sw_var.get().strip()) if self.norm_sw_var.get().strip() else [],
            "format": [fmt for fmt, var in self.format_vars.items() if var.get()],
            "samples": self.samples_var.get().strip(),
            "sort_vin": self.sort_vin_var.get(),
            "use_default_input": True,
            "s1_map": s1_map,
            "s2_map": s2_map,
            "skip_nodata": self.skip_nodata_var.get()
        }
        try:
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4)
        except:
            pass

if __name__ == "__main__":
    # If any CLI arguments are present, run headless CLI mode.
    # The GUI always builds a command that includes at least -s1/-s2, so this
    # correctly distinguishes a direct CLI invocation from a GUI launch.
    if sys.argv[1:]:
        import vdn_compare  # type: ignore[import-not-found]
        vdn_compare.main()
        sys.exit(0)
    
    # Ensure messagebox is available for error reporting
    # (Removed redundant local import)

    root = tk.Tk()
    app = VDNCompareGUI(root)
    root.mainloop()
