import duckdb
import pandas as pd

# Initialize DuckDB
con = duckdb.connect()

# Create sample data with Korean column name
df = pd.DataFrame({'교인성명': ['홍길동', '김철수', '홍길동', '이영희'], 'amount': [100, 200, 300, 400]})
con.execute("CREATE TABLE data AS SELECT * FROM df")

print("--- Testing SQL Queries ---")

# 1. Standard double quotes
try:
    sql1 = 'SELECT COUNT(DISTINCT "교인성명") FROM data'
    res1 = con.execute(sql1).df()
    print(f"PASS: {sql1}")
    print(res1)
except Exception as e:
    print(f"FAIL: {sql1} -> {e}")

# 2. Backticks (as in the error report)
try:
    sql2 = 'SELECT COUNT(DISTINCT `교인성명`) FROM data'
    res2 = con.execute(sql2).df()
    print(f"PASS: {sql2}")
    print(res2)
except Exception as e:
    print(f"FAIL: {sql2} -> {e}")

# 3. Just the column select with backticks
try:
    sql3 = 'SELECT `교인성명` FROM data LIMIT 1'
    res3 = con.execute(sql3).df()
    print(f"PASS: {sql3}")
    print(res3)
except Exception as e:
    print(f"FAIL: {sql3} -> {e}")

# 4. ASCII column select with backticks
try:
    sql4 = 'SELECT `amount` FROM data LIMIT 1'
    res4 = con.execute(sql4).df()
    print(f"PASS: {sql4}")
    print(res4)
except Exception as e:
    print(f"FAIL: {sql4} -> {e}")
