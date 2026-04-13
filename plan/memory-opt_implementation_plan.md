# Memory Optimization Implementation Plan

The current script consumes nearly 9GB of RAM for a 300MB input file due to how Pandas allocates memory for Python lists and large dictionary mapping data structures. By slightly altering the pipeline, we can cut this memory consumption down significantly (likely by 80% or more).

## Proposed Changes

### [MODIFY] run_vdn_compare.py
- **Remove List Columns**: Change the pre-processing loop to directly create the clean JSON string column (`VDN_LIST_CLEAN`) and **immediately drop** the raw `VDN_LIST` column. This prevents Pandas from holding heavy Python list structures and heavy raw strings in memory simultaneously.
- **Pass Data Through DuckDB**: Update the DuckSQL joining query to directly propagate the `source_vdns_json` and `target_vdns_json` strings to the final `result_df`. 
- **Eliminate Dictionary Mapping**: Drop the `source_dict` and `target_dict` creation completely. Creating a hash map for 220k+ cars requires immense memory.
- **Targeted Diff Calculation**: Rewrite the Python diff extractor (`Extracting VDN differences...`). Instead of looping through all VINs and doing lookups, apply a row-level function to `result_df`. If a row is identified as `MATCH`, we instantly return empty strings. Only if the row is a `MISMATCH` do we parse the strings into a set and find the delta.
- **Aggressive Garbage Collection**: explicitly delete `df_source` and `df_target` from memory using `del` and invoke `gc.collect()` immediately after the DuckDB join finishes. 

## User Review Required

> [!WARNING]  
> All of the output data will remain exactly the same. The only difference is that the processing will be much more memory efficient, and slightly faster because it skips calculating differences for VINs that perfectly match.

Please approve this approach so I can apply the memory optimizations!
