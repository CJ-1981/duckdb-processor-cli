import json
import pandas as pd

def parse_vdn(val):
    if pd.isna(val) or str(val).strip() in ('nan', ''): return []
    val_str = str(val).strip()
    if val_str.startswith('[') and val_str.endswith(']'):
        try:
            parsed = json.loads(val_str.replace("'", '"'))
            if isinstance(parsed, list):
                result = sorted(str(v).strip() for v in parsed if str(v).strip())
                return result if result else []
        except Exception:
            pass
    chunks = [val_str[i:i+4] for i in range(0, len(val_str), 4)]
    result = sorted(c for c in chunks if c.strip())
    return result if result else []

inputs = ['CA02', 'ca02', '["CA02", "CA02"]', '["CA02"]', '["ca02"]']
for i in inputs:
    p = parse_vdn(i)
    d = json.dumps(p)
    print(f'Input: {i:15} | Parsed: {p} | Dump: {d}')
