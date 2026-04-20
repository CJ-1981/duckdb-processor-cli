import duckdb
import json
import pandas as pd

df = pd.DataFrame({
    'vin': ['1', '2', '3', '4'],
    's1_vdns_json': ['["A1B2", "C3D4"]', '[]', None, '["A1B2"]'],
    's2_vdns_json': ['["A1B2", "X9Y8"]', '["Z1Z2"]', '["Z1Z2"]', None]
})

con = duckdb.connect()
con.register('df', df)

try:
    query = """
        SELECT 
            vin, 
            CAST(from_json(COALESCE(s1_vdns_json, '[]'), '["VARCHAR"]') AS VARCHAR[]) as s1_list,
            CAST(from_json(COALESCE(s2_vdns_json, '[]'), '["VARCHAR"]') AS VARCHAR[]) as s2_list
        FROM df
    """
    res = con.execute(f"""
        WITH parsed AS ({query})
        SELECT 
            vin,
            list_filter(s1_list, x -> NOT list_contains(s2_list, x)) as s1_rem_list,
            list_filter(s2_list, x -> NOT list_contains(s1_list, x)) as s2_add_list
        FROM parsed
    """).df()
    print("Filtered lists:")
    print(res)
    
    # Try the manual join Python logic
    def _join_list(l):
        if hasattr(l, '__iter__') and not isinstance(l, (str, bytes)):
            return ", ".join(sorted(str(v) for v in l if v is not None))
        return ""
    res['Only in S1'] = res['s1_rem_list'].apply(_join_list)
    res['Only in S2'] = res['s2_add_list'].apply(_join_list)
    print("\nJoined strings:")
    print(res[['vin', 'Only in S1', 'Only in S2']])

except Exception as e:
    print(f"Error parsing JSON: {e}")
