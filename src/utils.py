# Este es un documento que recopila todas las funciones y clases
# definidas en los archivos del proyecto para facilitar su referencia.

#Librerías importadas   

import pandas as pd 
import numpy as np


# Funciones definidas en utils.py

def extract_arch(archivo):
    ruta_base = 'D:\\Users\\Angel\\Documents\\Proyectos\\Proyecto_ECV_Mexico\\Datos'
    if (archivo > 2018):
        txt = str(archivo)
        df_egre = pd.read_table(f'{ruta_base}\\ssa_egresos_{txt}\\Egresos.txt', sep='|')
        df_afec = pd.read_table(f'{ruta_base}\\ssa_egresos_{txt}\\Afecciones.txt', sep='|')
    else:
        txt = str(archivo)
        df_egre = pd.read_csv(f'{ruta_base}\\ssa_egresos_{txt}\\EGRESO_{txt}.csv')
        df_afec = pd.read_csv(f'{ruta_base}\\ssa_egresos_{txt}\\AFECCIONES_{txt}.csv')

    df_afec['AFEC'] = df_afec['AFEC'].astype(str).str.extract(r"(\b[I]\w+)")
    df_afec = df_afec.dropna(subset=["AFEC"])
    df_filtrado_combinado = pd.merge(df_afec, df_egre, on='ID', how='inner')

    return df_filtrado_combinado