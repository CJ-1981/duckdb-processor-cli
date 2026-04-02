import duckdb
con = duckdb.connect()
try:
    con.execute('SELECT 1!')
except Exception as e:
    print("Factorial Error:", e)

try:
    con.execute('SELECT `amount` FROM (SELECT 1 AS amount)')
except Exception as e:
    print("Backtick Error:", e)
