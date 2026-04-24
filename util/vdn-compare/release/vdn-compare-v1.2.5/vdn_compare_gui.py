import tkinter as tk
from tkinter import filedialog, ttk
import subprocess
import threading
import sys
import shlex

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
        self.root.geometry("750x850")

        # Layout
        self.main_frame = ttk.Frame(self.root, padding="10")
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

        # Compare Columns
        lbl_cmp = ttk.Label(self.main_frame, text="Compare Columns:")
        lbl_cmp.grid(row=3, column=0, sticky=tk.W, pady=5)
        self.compare_var = tk.StringVar(value="all")
        ent_cmp = ttk.Entry(self.main_frame, textvariable=self.compare_var, width=50)
        ent_cmp.grid(row=3, column=1, pady=5)
        ttk.Label(self.main_frame, text="(e.g. all, or sw vdn model region)").grid(row=3, column=2, sticky=tk.W, pady=5)
        add_tooltip(lbl_cmp, "List of columns to compare. Options: sw, vdn, model, region, vin, or 'all'.\nSpace separated.")

        # Sort VIN
        lbl_sort = ttk.Label(self.main_frame, text="Sort VIN:")
        lbl_sort.grid(row=4, column=0, sticky=tk.W, pady=5)
        self.sort_vin_var = tk.StringVar(value="asc")
        sort_cb = ttk.Combobox(self.main_frame, textvariable=self.sort_vin_var, values=("none", "asc", "desc"), state="readonly", width=47)
        sort_cb.grid(row=4, column=1, sticky=tk.W, pady=5)
        add_tooltip(lbl_sort, "Sort the output records by VIN (none respects input order).")

        # Formats
        lbl_fmt = ttk.Label(self.main_frame, text="Formats:")
        lbl_fmt.grid(row=5, column=0, sticky=tk.W, pady=5)
        self.format_var = tk.StringVar(value="rich md html csv")
        ent_fmt = ttk.Entry(self.main_frame, textvariable=self.format_var, width=50)
        ent_fmt.grid(row=5, column=1, pady=5)
        ttk.Label(self.main_frame, text="(e.g. rich md html csv)").grid(row=5, column=2, sticky=tk.W, pady=5)
        add_tooltip(lbl_fmt, "Format(s) for summary output (can select multiple, space separated).")

        # Samples
        lbl_samp = ttk.Label(self.main_frame, text="Samples:")
        lbl_samp.grid(row=6, column=0, sticky=tk.W, pady=5)
        self.samples_var = tk.StringVar(value="10")
        ent_samp = ttk.Entry(self.main_frame, textvariable=self.samples_var, width=50)
        ent_samp.grid(row=6, column=1, pady=5)
        ttk.Label(self.main_frame, text="(integer or 'all')").grid(row=6, column=2, sticky=tk.W, pady=5)
        add_tooltip(lbl_samp, "Number of samples to show in the summary report (integer or 'all').")

        # Normalize Models
        lbl_nm = ttk.Label(self.main_frame, text="Normalize Models:")
        lbl_nm.grid(row=7, column=0, sticky=tk.W, pady=5)
        self.norm_models_var = tk.StringVar(value="EX30,V216 EX30 CC,V216-CC")
        ent_nm = ttk.Entry(self.main_frame, textvariable=self.norm_models_var, width=50)
        ent_nm.grid(row=7, column=1, pady=5)
        add_tooltip(lbl_nm, 'Groups of equivalent models, space-separated groupings.\nUse comma inside groups. e.g. "EX30,V216" "PS4,P417"\nThe first item becomes the primary display name.')
        
        # Normalize SW
        lbl_nsw = ttk.Label(self.main_frame, text="Normalize SW:")
        lbl_nsw.grid(row=8, column=0, sticky=tk.W, pady=5)
        self.norm_sw_var = tk.StringVar(value="MY27 J1,27 J1")
        ent_nsw = ttk.Entry(self.main_frame, textvariable=self.norm_sw_var, width=50)
        ent_nsw.grid(row=8, column=1, pady=5)
        add_tooltip(lbl_nsw, 'Groups of equivalent SW versions. Use quotes if spaces exist within group names.\nExample: "MY27 J1,27 J1" "1.8.0,1.8.0-hotfix"')
        
        # Normalize Custom
        lbl_nc = ttk.Label(self.main_frame, text="Normalize Custom JSON:")
        lbl_nc.grid(row=9, column=0, sticky=tk.W, pady=5)
        self.norm_custom_var = tk.StringVar(value="{}")
        ent_nc = ttk.Entry(self.main_frame, textvariable=self.norm_custom_var, width=50)
        ent_nc.grid(row=9, column=1, pady=5)
        add_tooltip(lbl_nc, 'Custom normalization rules in JSON mapping column names to lists of equivalent groups.\nBest to leave empty and configure via config.json instead.')

        # Skip Filter
        lbl_sf = ttk.Label(self.main_frame, text="Skip Filter JSON:")
        lbl_sf.grid(row=10, column=0, sticky=tk.W, pady=5)
        self.skip_filter_var = tk.StringVar(value="{}")
        ent_sf = ttk.Entry(self.main_frame, textvariable=self.skip_filter_var, width=50)
        ent_sf.grid(row=10, column=1, pady=5)
        add_tooltip(lbl_sf, 'Values to skip/exclude, in JSON format: {"ColumnName": ["Value1", "Value2"]}.\nRows matching any of these will be dropped entirely.')

        # Checkboxes for flags
        flags_frame = ttk.Frame(self.main_frame)
        flags_frame.grid(row=11, column=0, columnspan=3, sticky=tk.W, pady=5)
        
        self.skip_nodata_var = tk.BooleanVar(value=False)
        cb_nodata = ttk.Checkbutton(flags_frame, text="Skip Rows with No Data", variable=self.skip_nodata_var)
        cb_nodata.pack(side=tk.LEFT, padx=(0, 10))
        add_tooltip(cb_nodata, 'Skip any row that has missing data in any of the compared columns.')

        self.pager_var = tk.BooleanVar(value=False)
        cb_pager = ttk.Checkbutton(flags_frame, text="Use Pager for Output", variable=self.pager_var)
        cb_pager.pack(side=tk.LEFT)
        add_tooltip(cb_pager, 'Use a less command-line pager to display long console tables.\n(Useful if printing to console, but less useful in this GUI text box).')

        # Run Button
        self.run_btn = ttk.Button(self.main_frame, text="Run Comparison", command=self.run_script)
        self.run_btn.grid(row=12, column=0, columnspan=3, pady=10)

        # Output text area
        output_frame = ttk.Frame(self.main_frame)
        output_frame.grid(row=13, column=0, columnspan=3, pady=5, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.main_frame.rowconfigure(13, weight=1)
        self.main_frame.columnconfigure(1, weight=1)

        self.output_text = tk.Text(output_frame, height=15, width=75, wrap=tk.WORD)
        self.output_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(output_frame, command=self.output_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.output_text.config(yscrollcommand=scrollbar.set)

    def browse_file(self, var):
        filename = filedialog.askopenfilename(
            title="Select file",
            filetypes=[("CSV/Excel files", "*.csv *.xlsx *.xls"), ("JSON files", "*.json"), ("All files", "*.*")]
        )
        if filename:
            var.set(filename)

    def run_script(self):
        self.run_btn.config(state=tk.DISABLED)
        self.output_text.delete(1.0, tk.END)
        self.output_text.insert(tk.END, "Running...\n")
        
        # Build command
        if getattr(sys, 'frozen', False):
            # Running as bundled exe
            cmd = [sys.executable, "--run-cli-mode"]
        else:
            # Running as script
            cmd = [sys.executable, __file__, "--run-cli-mode"]
            
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
            cmd.extend(self.norm_models_var.get().strip().split())
            
        if self.norm_sw_var.get().strip():
            cmd.extend(["--normalize-sw"])
            # Split SW versions, if they contain spaces inside groups we need to be careful
            # We can use shlex.split here if user used quotes, or simply let the GUI pass it.
            # If the user separated by spaces and didn't quote, it might split incorrectly.
            import shlex
            cmd.extend(shlex.split(self.norm_sw_var.get().strip()))
            
        if self.norm_custom_var.get().strip():
            cmd.extend(["--normalize-custom", self.norm_custom_var.get().strip()])
            
        if self.skip_filter_var.get().strip():
            cmd.extend(["--skip-filter", self.skip_filter_var.get().strip()])

        if self.format_var.get().strip():
            cmd.extend(["--format"])
            cmd.extend(self.format_var.get().split())
        
        # Always add --use-default-input so the CLI doesn't pop up its own file dialog
        cmd.append("--use-default-input")
        
        if self.skip_nodata_var.get():
            cmd.append("--skip-nodata")
        if self.pager_var.get():
            cmd.append("--pager")

        def run_thread():
            try:
                # We need to capture output
                # Use CREATE_NO_WINDOW on Windows to prevent console flashing
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
                    creationflags=creationflags
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
        self.output_text.insert(tk.END, text)
        self.output_text.see(tk.END)

if __name__ == "__main__":
    if '--run-cli-mode' in sys.argv:
        sys.argv.remove('--run-cli-mode')
        
        # Prevent input() from blocking the subprocess
        import builtins
        builtins.input = lambda prompt="": ""

        import vdn_compare
        vdn_compare.main()
        sys.exit(0)
        
    root = tk.Tk()
    app = VDNCompareGUI(root)
    root.mainloop()

