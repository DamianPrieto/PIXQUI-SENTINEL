import duckdb
import pandas as pd
import numpy as np
from src.utils import extract_arch, estand_geo, limpiar_outliers, fechas_inteligentes

def ejecutar_ingesta():
    # 1. Conexión a tu bóveda DuckDB
    con = duckdb.connect("data/pixqui_sentinel.duckdb")
    
    # 2. Rango de investigación (2010 - 2025)
    años = range(2010, 2026)
    
    print("🛡️ Iniciando Ingesta Masiva en PIXQUI-SENTINEL...")
    print("-" * 50)

    for año in años:
        try:
            print(f"Procesando año {año}...")
            
            # --- CARGA Y LIMPIEZA ---
            # Extraemos con la optimización de memoria que ya validaste
            df = extract_arch(año)
            
            # Aplicamos tus funciones de limpieza biológica
            df['PESO'] = limpiar_outliers(df['PESO'], 0.5, 250)
            df['TALLA'] = limpiar_outliers(df['TALLA'], 10, 250)
            
            # Estandarización geográfica (zfill para tus mapas de focos rojos)
            df['ENTIDAD'] = estand_geo(df['ENTIDAD'], 2)
            df['MUNIC'] = estand_geo(df['MUNIC'], 3)
            df['LOC'] = estand_geo(df['LOC'], 4)
            df['CLAVE_GEO'] = df['ENTIDAD'] + df['MUNIC'] + df['LOC']
            
            # Normalización inteligente de fechas
            df['INGRE'] = fechas_inteligentes(df['INGRE'])
            df['EGRESO'] = fechas_inteligentes(df['EGRESO'])

            # --- TRANSFORMACIÓN R&D ---
            # Cálculo de IMC al vuelo (Talla en cm a m)
            df['IMC'] = df['PESO'] / ((df['TALLA'] / 100) ** 2)
            
            # --- INYECCIÓN ---
            # DuckDB lee el DataFrame 'df' directamente. ¡Es ultra rápido!
            con.execute("INSERT INTO egresos_ecv SELECT * FROM df")
            
            print(f"Año {año} completado. {len(df):,} registros inyectados.")
            
        except FileNotFoundError:
            print(f"Salto: Carpeta del año {año} no encontrada.")
        except Exception as e:
            print(f"Error crítico en {año}: {e}")
            continue

    con.close()
    print("-" * 50)
    print("¡MISIÓN CUMPLIDA! Los 100M+ de datos están en la bóveda.")

if __name__ == "__main__":
    ejecutar_ingesta()