import duckdb
import pandas as pd

# Conectamos
con = duckdb.connect('data/pixqui_sentinel.duckdb')

print("--- 🏥 ÚLTIMOS 5 PACIENTES (EGRESOS) ---")
# Ordenamos por AÑO descendente para ver lo más reciente
df_egre = con.sql("SELECT ID, ANIO, EDAD, SEXO, ENTIDAD FROM EGRESOS_BASE ORDER BY ANIO DESC LIMIT 5").df()
print(df_egre)

print("\n--- 🦠 ÚLTIMAS 5 AFECCIONES ---")
df_afec = con.sql("SELECT * FROM AFECCIONES ORDER BY ANIO DESC LIMIT 5").df()
print(df_afec)

con.close()