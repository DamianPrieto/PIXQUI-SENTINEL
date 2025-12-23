from src.utils import extract_arch
import pandas as pd
import numpy as np

# Prueba con un año reciente (que seguro tiene IDs alfanuméricos)
ANIO_PRUEBA = 2023

print(f"🕵️‍♂️ INVESTIGANDO EL AÑO {ANIO_PRUEBA}...")

# 1. Llamamos a tu función extract_arch
try:
    df_egre, _, _ = extract_arch(ANIO_PRUEBA)
    print(df_egre)
    
    if not df_egre.empty:
        print("\n✅ DATOS CARGADOS EN PANDAS")
        
        # 2. VERIFICACIÓN VISUAL
        if 'ID' in df_egre.columns:
            print(f"   Columna ID encontrada. Tipo de dato: {df_egre['ID'].dtype}")
            print("\n--- MUESTRA DE 5 IDs (Deben verse letras y números) ---")
            print(df_egre['ID'].head(10).to_list())
            
            # Verificamos nulos
            nulos = df_egre['ID'].isnull().sum()
            total = len(df_egre)
            print(f"\n--- ESTADÍSTICAS ---")
            print(f"   Total filas: {total}")
            print(f"   Total Nulos: {nulos}")
            
            if nulos == total:
                print("❌ PELIGRO: TODOS SON NULOS. Revisa la función extract_arch.")
            else:
                print("✅ ÉXITO: Tenemos IDs válidos.")
        else:
            print("❌ ERROR CRÍTICO: La columna ID no existe en el DataFrame.")
            print("   Columnas detectadas:", df_egre.columns.tolist())
    else:
        print("⚠️ El DataFrame está vacío.")

except Exception as e:
    print(f"💥 Error al ejecutar: {e}")