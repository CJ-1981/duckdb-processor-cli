import duckdb
import pandas as pd

df = pd.DataFrame({'s': ['["A", "B"]']})
con = duckdb.connect()
con.register('df', df)

res = con.execute('SELECT CAST(from_json(s, \'["VARCHAR"]\') AS VARCHAR[]) as lst FROM df').df()
lst = res['lst'][0]
print(type(lst))
print(repr(lst))

def _join_list(l):
    if hasattr(l, '__iter__') and not isinstance(l, (str, bytes)):
        return ", ".join(sorted(str(v) for v in l if v is not None))
    return ""

print("Joined:")
print(_join_list(lst))
