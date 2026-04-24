# Column Settings Tab — GUI Implementation Plan

Add a **"Column Settings"** tab to the existing Tkinter GUI that lets users visually configure the `column_map` (header renaming for S1 & S2) and the `--compare` column list, replacing the need to edit `config.json` by hand.

---

## Overview of the Feature

The new tab will:
1. **Read the actual column headers from the loaded CSV/Excel files** (after Source 1 / Source 2 are selected) and display them in dropdowns/lists.
2. **Let users map each internal role** (`VIN`, `SW`, `VDN`, `MODEL`, `REGION`, and any custom columns) to the actual column names found in each file.
3. **Let users choose which mapped columns to include in the comparison** via checkboxes.
4. **Write the resulting `column_map` and `compare` back to the GUI state** so the comparison run picks it up automatically (no manual `config.json` editing needed).
5. Optionally save/load the mapping as `config.json` for reuse.

---

## Proposed Changes

### `vdn_compare_gui.py` — Single file change

#### Tab Structure

Convert the existing flat `main_frame` layout to a `ttk.Notebook` with two tabs:
- **"Run Settings"** — the current set of widgets (Sources, Config, Compare Columns, Formats, etc.)
- **"Column Settings"** — new tab described below

#### Column Settings Tab Layout

```
┌─────────────────────────────────────────────────────────────┐
│ [Load Columns from Files]   (button — reads S1 & S2 headers)│
├────────────────┬────────────────────┬────────────────────────┤
│  Role          │  S1 Column         │  S2 Column (if diff)   │
├────────────────┼────────────────────┼────────────────────────┤
│  VIN  ★        │  [Combobox ▼]      │  [Combobox ▼]          │
│  SW            │  [Combobox ▼]      │  [Combobox ▼]          │
│  VDN           │  [Combobox ▼]      │  [Combobox ▼]          │
│  MODEL         │  [Combobox ▼]      │  [Combobox ▼]          │
│  REGION        │  [Combobox ▼]      │  [Combobox ▼]          │
│  [+ Add Row]                                                 │
├────────────────┴────────────────────┴────────────────────────┤
│ Columns to Compare:                                          │
│  [✓] SW   [✓] VDN   [✓] MODEL   [✓] REGION   (checkboxes)  │
├──────────────────────────────────────────────────────────────┤
│  [Apply to Run]    [Save to config.json]                     │
└──────────────────────────────────────────────────────────────┘
```

#### Key Behaviours

| Action | Effect |
|---|---|
| **Load Columns from Files** | Reads headers from S1 & S2 paths (set on the Run tab) using `pandas` and populates all combobox option lists. |
| **Combobox selection** | Stores mapping `{ s1_header → internal_name, s2_header → internal_name }`. When S1 and S2 pick the same header name they share one mapping entry in `column_map`; when different they get per-file entries (`s1_map` / `s2_map`). |
| **Compare checkboxes** | Controls which roles are in the `--compare` list; also updates the "Compare Columns" field on the Run tab in sync. |
| **Apply to Run** | Marshals the current state into `cmd` args that `run_script()` will use (overrides the Run-tab "Compare Columns" field). |
| **Save to config.json** | Serialises the mapping + compare list + all other current Run-tab settings to the path in the "Config File" field. |

#### Implementation Details

- **Column header reading**: A small helper `_read_headers(path)` will open the file with `pandas` (same CSV/Excel logic already in `vdn_compare.py`) and return only the column names — lightweight, no full load.
- **Shared vs per-file maps**: If the S1 combobox and S2 combobox for the same role pick the **same column name**, the entry goes into the shared `column_map`. If they differ, it goes into `s1_map` and `s2_map`. The existing `vdn_compare.py` already handles both formats (lines 312–328).
- **VIN is required** (marked ★): If VIN is not mapped, "Apply to Run" shows a validation warning.
- **Custom rows**: The "+ Add Row" button adds an extra row with a free-text "Role name" field and two comboboxes, allowing mapping of arbitrary extra columns.
- **Sync with Run tab**: Changing the compare checkboxes updates `self.compare_var` on the Run tab so the displayed value always reflects reality.
- **Config save**: Merges the column mapping into the existing `config.json` format (including all current Run-tab values), writes to the path in `self.config_var`.

---

## Open Questions

> [!IMPORTANT]
> **Q1 — Separate S1/S2 maps or one shared map?**
> The current `config.json` uses a single `column_map` that is applied to **both** files with the same rename. If the two files always have the same header name for a given role (e.g. both call it `VIN`), one shared map is enough. But the `vdn_compare.py` code also supports separate `s1_map` / `s2_map` keys. Should the UI show **one** column per role or **two** (one for each file)? I've assumed **two columns** in the layout above to cover the most general case — confirm if that's what you need.

> [!NOTE]  
> **Q2 — Column header auto-load trigger**
> I plan to auto-populate the comboboxes when the user clicks "Load Columns from Files". Alternatively, we could auto-load whenever the file paths change on the Run tab. The explicit button is less surprising since file I/O might be slow.

> [!NOTE]
> **Q3 — Config.json save scope**
> When "Save to config.json" is clicked, should it save **all** Run-tab settings (formats, samples, normalize rules, etc.) or **only** the column mapping? Saving everything means the config file becomes a full snapshot — probably the most convenient behaviour.

---

## Verification Plan

### Manual verification
1. Launch the GUI (python or .exe).
2. Set Source 1 / Source 2 paths on Run tab.
3. Switch to Column Settings → click "Load Columns from Files" → verify comboboxes populate with actual CSV headers.
4. Change a mapping (e.g. remap SW to a different column) → click "Apply to Run" → verify the Compare Columns field and generated command reflect the change.
5. Uncheck a column (e.g. MODEL) → verify it is removed from the comparison run.
6. Click "Save to config.json" → inspect the file to confirm correct JSON.
7. Run the comparison and confirm results match expectations.
