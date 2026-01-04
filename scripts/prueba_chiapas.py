import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from src.database.connection import db 
from src.database.repositories import EpidemiologyService

repo = EpidemiologyService()

# --- 1. FUNCIÓN DE LIMPIEZA DE TEXTO ---
def arreglar_nombres(texto):
    """Arregla caracteres raros tipo 'ComitÃ¡n' -> 'Comitán'"""
    if not isinstance(texto, str):
        return str(texto)
    try:
        return texto.encode('latin1').decode('utf-8')
    except:
        return texto

def visualizar_chiapas():
    # ---------------------------------------------------------
    # 1. OBTENCIÓN DE DATOS
    # ---------------------------------------------------------
    print("Obteniendo datos epidemiológicos...")
    df = repo.obtener_evolucion_estado('07', 'I', 2010, 2025)
    
    if df.empty:
        print("No hay datos para Chiapas.")
        return

    # --- MAPEO DE NOMBRES ---
    print("Mapeando nombres de municipios...")
    query_nombres = "SELECT DISTINCT CVE_GEO, NOM_MUN FROM DIM_POBLACION WHERE CVE_GEO LIKE '07%'"
    try:
        raw_nombres = db.ejecutar_query(query_nombres).fetchall()
        mapa_nombres = {fila[0]: fila[1] for fila in raw_nombres}
        df['Municipio_Nombre'] = df['CVE_GEO'].map(mapa_nombres).fillna(df['CVE_GEO'])
        df['Municipio_Nombre'] = df['Municipio_Nombre'].apply(arreglar_nombres)
    except Exception as e:
        print(f"Error en nombres: {e}")
        df['Municipio_Nombre'] = df['CVE_GEO']

    # ---------------------------------------------------------
    # 2. GRÁFICA 1: HEATMAP (MAPA DE CALOR)
    # ---------------------------------------------------------
    print(" Generando Heatmap...")
    df_pivot = df.pivot_table(index='Municipio_Nombre', columns='ANIO', values='tasa')
    
    # Top 20
    df_pivot['promedio'] = df_pivot.mean(axis=1)
    df_top = df_pivot.sort_values('promedio', ascending=False).head(20).drop(columns=['promedio']) 

    sns.set_theme(style="white", context="talk")
    f, ax = plt.subplots(figsize=(14, 10))
    sns.heatmap(df_top, cmap='Reds', linewidths=.5, linecolor='#dddddd',
                cbar_kws={'label': 'Tasa por 100k habitantes'}, annot=False, ax=ax)

    ax.set_title('Top 20: Zonas de Alto Riesgo Cardiovascular\nChiapas (2010-2025)', 
                 fontsize=18, fontweight='bold', pad=20)
    ax.set_ylabel('')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('chiapas_ecv_heatmap.png', dpi=300)
    plt.show()

    # ---------------------------------------------------------
    # 3. GRÁFICA 2: JUSTIFICACIÓN (PROMEDIO PONDERADO CIENTÍFICO)
    # ---------------------------------------------------------
    print("Generando Gráfica de Justificación (Cálculo Riguroso)...")
    
    # A) Tasa Real de Arriaga (Promedio de sus tasas)
    df_arriaga = df[df['Municipio_Nombre'] == 'Arriaga']
    if df_arriaga.empty:
        print("Advertencia: 'Arriaga' no aparece en los datos.")
        return
    tasa_arriaga = df_arriaga['tasa'].mean()

    # B) Tasa Estatal Ponderada (CORREGIDO: usa 'enfermos' en vez de 'casos')
    # Sumamos enfermos y habitantes de TODO el estado por año
    df_estatal_anual = df.groupby('ANIO')[['enfermos', 'habitantes']].sum().reset_index()
    
    # Calculamos la tasa real del estado por año y luego promediamos
    df_estatal_anual['tasa_estatal'] = (df_estatal_anual['enfermos'] / df_estatal_anual['habitantes']) * 100000
    tasa_promedio_estatal = df_estatal_anual['tasa_estatal'].mean()

    # C) El Segundo Lugar (Competidor)
    df_promedios_mun = df.groupby('Municipio_Nombre')['tasa'].mean().reset_index().sort_values('tasa', ascending=False)
    
    if df_promedios_mun.iloc[0]['Municipio_Nombre'] == 'Arriaga':
        competidor = df_promedios_mun.iloc[1]
        etiqueta_competidor = f"2do Lugar\n({competidor['Municipio_Nombre']})"
    else:
        competidor = df_promedios_mun.iloc[0]
        etiqueta_competidor = f"Más Alto\n({competidor['Municipio_Nombre']})"
        
    tasa_competidor = competidor['tasa']

    # --- PLOTEO ---
    etiquetas = ['Promedio Estatal\n(Ponderado)', etiqueta_competidor, 'ARRIAGA\n(Zona Crítica)']
    valores = [tasa_promedio_estatal, tasa_competidor, tasa_arriaga]
    colores = ['#95a5a6', '#7f8c8d', '#c0392b']

    plt.figure(figsize=(11, 7))
    bars = plt.bar(etiquetas, valores, color=colores, width=0.6)

    # Línea promedio
    plt.axhline(y=tasa_promedio_estatal, color='#7f8c8d', linestyle='--', alpha=0.6)
    plt.text(-0.45, tasa_promedio_estatal + 5, 'Nivel Estatal', color='#7f8c8d', fontstyle='italic', fontweight='bold')

    # Valores numéricos
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 3,
                 f'{int(height)}', ha='center', va='bottom', fontsize=15, fontweight='bold', color='#2c3e50')

    # Texto de Impacto
    veces_mayor = tasa_arriaga / tasa_promedio_estatal
    pct_mayor = (veces_mayor - 1) * 100
    
    plt.annotate(f'¡{int(pct_mayor)}% mayor al\npromedio estatal!', 
                 xy=(2, tasa_arriaga), xytext=(1.25, tasa_arriaga),
                 arrowprops=dict(facecolor='#c0392b', shrink=0.05, width=2),
                 fontsize=12, fontweight='bold', color='#c0392b',
                 bbox=dict(boxstyle="round,pad=0.4", fc="#fadbd8", ec="#c0392b", lw=2))

    plt.title('Justificación: Arriaga mantiene la mayor tasa histórica de ECV\n(Promedio 2010-2025)', 
              fontsize=16, fontweight='bold', pad=20, color='#2c3e50')
    
    sns.despine(left=True)
    plt.yticks([])
    plt.tight_layout()
    plt.savefig('chiapas_justificacion_arriaga_final.png', dpi=300)
    print("¡LISTO! Gráfica generada: chiapas_justificacion_arriaga_final.png")
    plt.show()

if __name__ == "__main__":
    visualizar_chiapas()