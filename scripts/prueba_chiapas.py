import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from src.database.connection import db 

# Supondremos que agregaste el método arriba a tu clase EpidemiologyService
from src.database.repositories import EpidemiologyService
repo = EpidemiologyService()

def visualizar_chiapas():
    # 1. Obtener los Datos (El Heavy Lifting)
    # Chiapas = '07', ECV = 'I', 2010 a 2023
    df = repo.obtener_evolucion_estado('07', 'I', 2010, 2025)
    
    if df.empty:
        print("❌ No hay datos para Chiapas.")
        return

    print("✅ Datos obtenidos. Procesando visualización...")

    # 2. Pivotar los datos (Transformación para Heatmap)
    # Queremos: Filas = Municipios, Columnas = Años, Valores = Tasa
    df_pivot = df.pivot_table(index='CVE_GEO', columns='ANIO', values='tasa')

    # LIMPIEZA VISUAL:
    # Chiapas tiene muchos municipios. Vamos a graficar solo el Top 30 con mayor tasa promedio
    # para que la gráfica sea legible.
    df_pivot['promedio'] = df_pivot.mean(axis=1)
    df_top = df_pivot.sort_values('promedio', ascending=False).head(30)
    df_top = df_top.drop(columns=['promedio']) # Quitamos la columna auxiliar

    # 3. Graficar
    plt.figure(figsize=(12, 10))
    
    # Creamos el Heatmap
    # cmap='YlOrRd' significa (Yellow-Orange-Red). Rojo intenso = Peligro.
    sns.heatmap(df_top, cmap='Reds', linewidths=.5)

    plt.title('Evolución de Enfermedades Cardiovasculares (CIE-10 Bloque I)\nTop 30 Municipios de Chiapas con mayor incidencia', fontsize=16)
    plt.xlabel('Año')
    plt.ylabel('Clave Municipio')
    
    print("📸 Guardando gráfica en 'chiapas_ecv.png'...")
    plt.savefig('chiapas_ecv.png', dpi=300, bbox_inches='tight')
    plt.show()

if __name__ == "__main__":
    visualizar_chiapas()