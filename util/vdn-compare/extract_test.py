import pandas as pd
df_s1 = pd.read_csv("input/DB.csv", sep=',', encoding='utf-8-sig')
df_s2 = pd.read_csv("input/PIE.csv", sep=';', encoding='utf-8-sig')

bad_vins = ["L6TEY2E2XRZ000201", "LYV2LEKEXRS000352", "YV12ZEL99RS000256"]

print("S1 rows:")
print(df_s1[df_s1['vin'].isin(bad_vins)][['vin', 'DB_targetVdns']].to_string())
print("\nS2 rows:")
print(df_s2[df_s2['VIN'].isin(bad_vins)][['VIN', 'VDN_LIST']].to_string())
