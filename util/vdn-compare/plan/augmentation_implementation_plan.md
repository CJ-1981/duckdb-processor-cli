# Column Augmentation via Lookup Tables — Implementation Plan

This plan describes how to add a "Column Augmentation" feature that allows users to create new columns based on substrings of existing columns (e.g., VIN) and map them to descriptive names using a lookup table.

## 1. CLI Core Implementation (`vdn_compare.py`)

### Proposed Logic
We will add a new `--augment` argument that accepts a JSON list of augmentation rules.

**Standard Rule Format:**
```json
{
  "name": "PLANT",
  "source": "VIN",
  "start": 10, 
  "length": 1,
  "lookup": {
    "R": "Rockville",
    "S": "Springfield"
  },
  "default": "Unknown"
}
```

**Conditional Rule Format:**
```json
{
  "name": "PLANT",
  "source": "VIN",
  "start": 10,
  "length": 1,
  "condition_col": "BRAND",
  "conditional_lookups": {
    "VOLVO": { "R": "Ghent", "S": "Torslanda" },
    "POLESTAR": { "R": "Chengdu", "S": "Luqiao" }
  },
  "default": "Unknown"
}
```

### Changes:
- **`argparse`**: Add `--augment` (default `[]`).
- **`main()`**: 
    - Parse the `--augment` JSON.
    - Right after loading `df_s1` and `df_s2`, apply the augmentation rules.
    - For each rule:
        - Validate that `source` and `condition_col` (if any) exist.
        - Create the new column: 
            - If non-conditional: Apply substring slice and map `lookup`.
            - If conditional: Iterate through `conditional_lookups`, matching `condition_col` value to apply the specific sub-table.
        - Add the new column `name` to the potential comparison targets.
- **`preprocess_df()`**: Ensure augmented columns are treated as "Generic Custom" columns so they get normalization and audit support if requested.

## 2. GUI Integration (`vdn_compare_gui.py`)

### Proposed Changes:
- **"Column Settings" Tab**:
    - Add an "Augmentations" section below the Column Mapping grid.
    - List active augmentations in a table/listbox.
    - Provide an "Add Augmentation" button that opens a dialog.
    - The dialog will have fields for:
        - **Target Name**: The name of the new column (e.g., PLANT).
        - **Source Column**: Dropdown of available headers (S1/S2 union).
        - **Start Index**: Integer (0-indexed).
        - **Length**: Integer.
        - **Condition Column**: (Optional) Dropdown to select a column for conditional lookups.
        - **Lookup Table**: A small grid or text area for `Key:Value` pairs. If a condition is set, this accepts a JSON object of nested lookups.
        - **Default Value**: String for missing keys.
- **Config Sync**:
    - Include `augment` in the `config.json` when saving.
    - Ensure `run_script()` passes the `--augment` flag to the subprocess.

## 3. Verification Plan

1. **Unit Test**: Create a dummy CSV with a VIN column. Define an augmentation for "Plant" at index 10. Run comparison. Verify that "PLANT" appears in the output reports and can be compared.
2. **Conditional Test**: Create a dummy CSV with "VIN" and "BRAND". Define a conditional lookup for "PLANT" based on "BRAND". Verify that different plants are assigned correctly for Volvo vs. Polestar.
3. **GUI Test**: Open GUI, load files, add an augmentation in the UI, click "Run". Verify the CLI command generated in the output console includes the `--augment` flag.
