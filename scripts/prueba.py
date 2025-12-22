# prueba.py
from src.utils import extract_arch, estand_geo, limpiar_outliers, fechas_inteligentes
import pandas as pd
import numpy as np

def ejecutar_prueba(anio):
    print(" Iniciando Fase de Pruebas: PIXQUI-SENTINEL")
    print("-" * 45)
    
    # Elegimos un año para la prueba (por ejemplo, 2024 por ser formato moderno)
    anio_test = anio
    
    try:
        print(f" Procesando datos del año {anio_test}...")
        df_resultado = extract_arch(anio_test)
        
        # --- BLOQUE DE DIAGNÓSTICO ---
        print("\nPRUEBA EXITOSA")
        print(f"Total de registros (filas): {df_resultado.shape[0]:,}")
        print(f"Total de variables (columnas): {df_resultado.shape[1]}")
        
        #print("\nPrimeras 5 filas del Master Merge:")
        #print(df_resultado.head())
        
        # Verificamos si logramos capturar las columnas críticas para tu investigación
        # como Peso y Talla para el IMC
        columnas_criticas = ['ID', 'PESO', 'TALLA', 'AFEC', 'MOTEGRE']
        presentes = [c for c in columnas_criticas if c in df_resultado.columns]
        
        print(f"\n Variables críticas detectadas: {presentes}")

        
        # Prueba de estandarización 

        print("\n Probando función de estandarización geográfica...")

        print("Generando llaves geográficas...")
        df_resultado['CVE_ENT'] = estand_geo(df_resultado['ENTIDAD'], 2)
        df_resultado['CVE_MUN'] = estand_geo(df_resultado['MUNIC'], 3)
        df_resultado['CVE_LOC'] = estand_geo(df_resultado['LOC'], 4)

        # FUSIONAMOS PARA CREAR UNA LLAVE DE IDENTIFICACION
        df_resultado['CVE_GEO'] = df_resultado['CVE_ENT'] + df_resultado['CVE_MUN']

        print("Probando función de limpieza de outliers...")
        print("Limpiando datos biométricos...")
        df_resultado['PESO'] = limpiar_outliers(df_resultado['PESO'], 1, 250)
        df_resultado['TALLA'] = limpiar_outliers(df_resultado['TALLA'], 20, 230)

        print("Probando función de manejo inteligente de fechas...")
        print("   Procesando INGRE...")    
        df_resultado['INGRE'] = fechas_inteligentes(df_resultado['INGRE'])
        print("   Procesando EGRESO...")
        df_resultado['EGRESO'] = fechas_inteligentes(df_resultado['EGRESO'])

        print("Imprimiendo muestra de datos procesados:")

        print(df_resultado[['ID', 'PESO', 'TALLA', 'INGRE', 'EGRESO', 'CVE_GEO']].head())

        print("\n Fase de Pruebas completada exitosamente.")


    except FileNotFoundError as e:
        print(f"\n Error de archivos: {e}")
    except Exception as e:
        print(f"\n Fallo inesperado: {e}")
 


if __name__ == "__main__":
    años = np.arange(2010, 2026)
    for año in años:
        ejecutar_prueba(año)



