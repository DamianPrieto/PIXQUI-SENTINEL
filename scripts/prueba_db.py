import duckdb

DB_PATH = 'data/pixqui_sentinel.duckdb'
con = duckdb.connect(DB_PATH)

print(" INICIANDO LIMPIEZA FINAL DE LA BASE DE DATOS...")

# 1. BORRAR AFECCIONES HUÉRFANAS
print("   Eliminando Afecciones sin paciente...")
q1 = """
DELETE FROM AFECCIONES 
WHERE NOT EXISTS (
    SELECT 1 FROM EGRESOS_BASE e 
    WHERE AFECCIONES.ID = e.ID AND AFECCIONES.ANIO = e.ANIO
);
"""
con.sql(q1)
print("      Listo.")

# 2. BORRAR PROCEDIMIENTOS HUÉRFANOS
print("   🗑️ Eliminando Procedimientos sin paciente...")
q2 = """
DELETE FROM PROCEDIMIENTOS 
WHERE NOT EXISTS (
    SELECT 1 FROM EGRESOS_BASE e 
    WHERE PROCEDIMIENTOS.ID = e.ID AND PROCEDIMIENTOS.ANIO = e.ANIO
);
"""
con.sql(q2)
print("      Listo.")

# 3. (OPCIONAL) BORRAR EL RECORD DE 658KG (Probable error)
# Si quieres ser muy estricto, borramos pesos mayores a 600kg
print("   ⚖️ Ajustando outliers extremos de peso (>600kg)...")
con.sql("UPDATE EGRESOS_BASE SET PESO = NULL WHERE PESO > 600")
print("      Listo.")

print("\n✨ BASE DE DATOS OPTIMIZADA Y LISTA PARA PRODUCCIÓN ✨")
con.close()