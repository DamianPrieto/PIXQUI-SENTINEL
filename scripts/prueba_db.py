import duckdb
import pandas as pd

# Formato bonito para números
pd.set_option('display.float_format', '{:,.2f}'.format)
pd.set_option('display.max_columns', None)

DB_PATH = 'data/pixqui_sentinel.duckdb'
con = duckdb.connect(DB_PATH)

print("🕵️‍♂️ INICIANDO AUDITORÍA FORENSE DE DATOS...")
print("=" * 60)

# ---------------------------------------------------------
# 1. INTEGRIDAD REFERENCIAL (EL "ID" + "ANIO")
# ---------------------------------------------------------
print("\n🔗 1. VERIFICANDO COHERENCIA DE IDs (Huérfanos)...")
print("   Nota: Un 'Huérfano' es una cirugía o enfermedad que apunta a un ID que no existe en Egresos.")

# Revisamos AFECCIONES vs EGRESOS
q_afec = """
SELECT 
    COUNT(*) as TOTAL_HUERFANOS,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM AFECCIONES), 4) as PORCENTAJE_ERROR
FROM AFECCIONES a
LEFT JOIN EGRESOS_BASE e 
    ON a.ID = e.ID AND a.ANIO = e.ANIO -- ¡IMPORTANTE! La llave es compuesta
WHERE e.ID IS NULL;
"""
df_afec = con.sql(q_afec).df()
print(f"   🦠 Afecciones Huérfanas: {df_afec['TOTAL_HUERFANOS'][0]:,.0f} ({df_afec['PORCENTAJE_ERROR'][0]}%)")

# Revisamos PROCEDIMIENTOS vs EGRESOS
q_proc = """
SELECT 
    COUNT(*) as TOTAL_HUERFANOS,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM PROCEDIMIENTOS), 4) as PORCENTAJE_ERROR
FROM PROCEDIMIENTOS p
LEFT JOIN EGRESOS_BASE e 
    ON p.ID = e.ID AND p.ANIO = e.ANIO
WHERE e.ID IS NULL;
"""
df_proc = con.sql(q_proc).df()
print(f"   💉 Procedimientos Huérfanos: {df_proc['TOTAL_HUERFANOS'][0]:,.0f} ({df_proc['PORCENTAJE_ERROR'][0]}%)")

if df_afec['TOTAL_HUERFANOS'][0] > 0 or df_proc['TOTAL_HUERFANOS'][0] > 0:
    print("   ⚠️ ALERTA: Tienes registros sin paciente. Podrías borrarlos con un DELETE.")
else:
    print("   ✅ INTEGRIDAD PERFECTA: Todos los datos extra pertenecen a un paciente real.")


# ---------------------------------------------------------
# 2. LÍMITES BIOLÓGICOS (MAXIMOS Y PROMEDIOS)
# ---------------------------------------------------------
print("\n⚖️ 2. LÍMITES BIOLÓGICOS (Detectando monstruos)...")

# Vamos a ver los máximos para detectar si se coló un 999 o un error de dedo
q_bio = """
SELECT 
    ANIO,
    MIN(PESO) as MIN_PESO, MAX(PESO) as MAX_PESO, AVG(PESO) as AVG_PESO,
    MIN(TALLA) as MIN_TALLA, MAX(TALLA) as MAX_TALLA,
    MIN(IMC) as MIN_IMC, MAX(IMC) as MAX_IMC,
    MAX(EDAD) as MAX_EDAD
FROM EGRESOS_BASE
GROUP BY ANIO
ORDER BY ANIO DESC
LIMIT 5; -- Solo mostramos los últimos 5 años para no saturar
"""
print(con.sql(q_bio).df())


# ---------------------------------------------------------
# 3. CALIDAD DE CAMPOS (NULOS ESPERADOS)
# ---------------------------------------------------------
print("\nUnknown 3. VALIDACIÓN DE CAMPOS CLAVE (Conteo de Nulos)...")
# Verificamos cuántos pacientes no tienen peso, talla o sexo definido
q_nulls = """
SELECT 
    COUNT(*) as TOTAL_ROWS,
    COUNT(CASE WHEN PESO IS NULL THEN 1 END) as NULOS_PESO,
    COUNT(CASE WHEN TALLA IS NULL THEN 1 END) as NULOS_TALLA,
    COUNT(CASE WHEN SEXO IS NULL OR SEXO = 0 OR SEXO = 9 THEN 1 END) as SEXO_INDEFINIDO
FROM EGRESOS_BASE;
"""
df_nulls = con.sql(q_nulls).df()
print(df_nulls)

con.close()
