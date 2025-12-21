# Este es un documento que recopila todas las funciones y clases
# definidas en los archivos del proyecto para facilitar su referencia.

#Librerías importadas   

from pathlib import Path
import pandas as pd

def extract_arch(año):
    # Ajustamos la ruta si utils.py está en la carpeta 'src'
    ruta_base = Path(__file__).resolve().parent.parent / 'data'
    ruta_carpeta = ruta_base / f'ssa_egresos_{año}'

    #Ajustamos columnas deseadas para optimizar memoria
    col_egre = ['ID', 'CLUES', 'EGRESO', 'INGRE', 'DIAS_ESTA', 'EDAD', 'SEXO', 
                'PESO', 'TALLA', 'ENTIDAD', 'MUNIC', 'LOC', 'SERVICIOINGRE', 
                'SERVICIO02', 'SERVICIO03', 'SERVICIOEGRE', 'PROCED', 'CLUESPROCED',
                'MOTEGRE', 'CLUESREFERIDO', 'DIAG_INI', 'AFECPRIN',
                'VEZ', 'CAUSAEXT']
    
    col_afec = ['ID', 'AFEC']

    col_proc = ['ID', 'PROMED', 'TIPO']

    if not ruta_carpeta.exists():
        raise FileNotFoundError(f"La carpeta para el año {año} no existe.")

    # Lógica de carga según el periodo histórico
    if año > 2018:
        # Formato Moderno: TXT con separador de tubería |
        df_egre = pd.read_table(ruta_carpeta / 'Egresos.txt', sep='|', usecols=col_egre, low_memory=False)
        df_afec = pd.read_table(ruta_carpeta / 'Afecciones.txt', sep='|', usecols=col_afec, low_memory=False)
        df_proc = pd.read_table(ruta_carpeta / 'Procedimientos.txt', sep='|', usecols=col_proc, low_memory=False)
    else:
        # Formato Clásico: CSV con comas
        df_egre = pd.read_csv(ruta_carpeta / f'EGRESO_{año}.csv', usecols=col_egre, low_memory=False)
        df_afec = pd.read_csv(ruta_carpeta / f'AFECCIONES_{año}.csv', usecols=col_afec, low_memory=False)
        df_proc = pd.read_csv(ruta_carpeta / f'PROCEDIMIENTOS_{año}.csv', usecols=col_proc, low_memory=False)

    # El Triple Merge Encadenado
    # Unimos Afecciones con Egresos, y el resultado con Procedimientos
    df_combinado = df_afec.merge(df_egre, on='ID', how='inner').merge(df_proc, on='ID', how='inner')


    return df_combinado
