import duckdb
import os

# 1. Definimos la ruta correcta a la base de datos
db_path = 'data/pixqui_sentinel.duckdb'

# Verificamos si el archivo existe antes de conectar
if not os.path.exists(db_path):
    print(f"¡CUIDADO! No encuentro el archivo en: {db_path}")
    print("¿Seguro que ya corriste la función inicializar_db()?")
else:
    print(f"Archivo encontrado en: {db_path}")

    # 2. Conectamos a la base de datos correcta
    con = duckdb.connect(db_path)

    print("\n--- 1. TABLAS ACTUALES ---")
    con.sql("SHOW TABLES").show()

    print("\n--- 2. VARIABLES DE LA TABLA 'EGRESOS' ---")
    try:
        # OJO: Sin comillas o con comillas dobles
        con.sql('DESCRIBE "EGRESOS"').show()
    except Exception as e:
        print(f"Error buscando la tabla: {e}")
        print("Es probable que el archivo exista pero esté vacío. ¡Corre inicializar_db() primero!")

    con.close()