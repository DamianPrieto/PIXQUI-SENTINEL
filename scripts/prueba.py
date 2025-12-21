# prueba.py
from src.utils import extract_arch
import pandas as pd

def ejecutar_prueba():
    print("🛡️ Iniciando Fase de Pruebas: PIXQUI-SENTINEL")
    print("-" * 45)
    
    # Elegimos un año para la prueba (por ejemplo, 2024 por ser formato moderno)
    anio_test = 2024
    
    try:
        print(f"⏳ Procesando datos del año {anio_test}...")
        df_resultado = extract_arch(anio_test)
        
        # --- BLOQUE DE DIAGNÓSTICO ---
        print("\n✅ PRUEBA EXITOSA")
        print(f"📊 Total de registros (filas): {df_resultado.shape[0]:,}")
        print(f"📋 Total de variables (columnas): {df_resultado.shape[1]}")
        
        print("\n🔍 Primeras 5 filas del Master Merge:")
        print(df_resultado.head())
        
        # Verificamos si logramos capturar las columnas críticas para tu investigación
        # como Peso y Talla para el IMC
        columnas_criticas = ['ID', 'PESO', 'TALLA', 'AFEC', 'MOTEGRE']
        presentes = [c for c in columnas_criticas if c in df_resultado.columns]
        print(f"\n🎯 Variables críticas detectadas: {presentes}")

    except FileNotFoundError as e:
        print(f"\n❌ Error de archivos: {e}")
    except Exception as e:
        print(f"\n💥 Fallo inesperado: {e}")

if __name__ == "__main__":
    ejecutar_prueba()